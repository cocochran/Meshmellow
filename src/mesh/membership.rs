use anyhow::{Result, bail, ensure};
use iroh::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const MAX_MEMBERS: usize = 64;
pub const INVITE_SECONDS: u64 = 600;
pub fn name(value: &str) -> Result<String> {
    let value = value.trim();
    ensure!(
        !value.is_empty() && value.chars().count() <= 48 && !value.chars().any(char::is_control),
        "Use a name of 1–48 characters."
    );
    Ok(value.to_owned())
}

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Genesis {
    pub version: u8,
    pub root: String,
    pub name: String,
    pub device_name: String,
    pub nonce: String,
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Grant {
    pub mesh: String,
    pub issuer: String,
    pub member: String,
    pub name: String,
    pub request: String,
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ledger {
    pub genesis: Genesis,
    pub grants: Vec<Grant>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub member_status: Vec<MemberStatus>,
}

/// Only the mesh creator can remove or reinstate a device. Monotonic signed
/// revisions prevent an offline peer's older ledger from undoing a removal.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberStatus {
    pub mesh: String,
    pub member: String,
    pub revision: u64,
    pub allowed: bool,
    pub retained_grants: Vec<String>,
    pub signature: String,
}
impl MemberStatus {
    fn payload(&self) -> impl Serialize + '_ {
        (
            "meshthing/member-status/1",
            &self.mesh,
            &self.member,
            self.revision,
            self.allowed,
            &self.retained_grants,
        )
    }
}

fn sign(key: &SecretKey, message: impl Serialize) -> Result<String> {
    Ok(hex::encode(
        key.sign(&serde_json::to_vec(&message)?).to_bytes(),
    ))
}

fn verify(signer: &str, signature: &str, message: impl Serialize) -> Result<()> {
    let bytes: [u8; 64] = hex::decode(signature)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("Invalid signature length"))?;
    signer.parse::<EndpointId>()?.verify(
        &serde_json::to_vec(&message)?,
        &iroh::Signature::from_bytes(&bytes),
    )?;
    Ok(())
}

impl Genesis {
    fn payload(&self) -> impl Serialize + '_ {
        (
            "meshthing/genesis/1",
            self.version,
            &self.root,
            &self.name,
            &self.device_name,
            &self.nonce,
        )
    }
    pub fn id(&self) -> String {
        hex::encode(Sha256::digest(serde_json::to_vec(&self.payload()).unwrap()))
    }
}

impl Grant {
    fn payload(&self) -> impl Serialize + '_ {
        (
            "meshthing/grant/1",
            &self.mesh,
            &self.issuer,
            &self.member,
            &self.name,
            &self.request,
        )
    }
}

impl Ledger {
    pub fn create(key: &SecretKey, mesh_name: &str, device_name: &str) -> Result<Self> {
        let mut genesis = Genesis {
            version: 1,
            root: key.public().to_string(),
            name: name(mesh_name)?,
            device_name: name(device_name)?,
            nonce: uuid::Uuid::new_v4().to_string(),
            signature: String::new(),
        };
        genesis.signature = sign(key, genesis.payload())?;
        Ok(Self {
            genesis,
            grants: vec![],
            member_status: vec![],
        })
    }

    pub fn members(&self) -> Result<BTreeMap<String, String>> {
        ensure!(
            self.genesis.version == 1 && self.grants.len() < MAX_MEMBERS,
            "Unsupported or oversized membership."
        );
        name(&self.genesis.name)?;
        name(&self.genesis.device_name)?;
        uuid::Uuid::parse_str(&self.genesis.nonce)?;
        verify(
            &self.genesis.root,
            &self.genesis.signature,
            self.genesis.payload(),
        )?;
        let mesh = self.genesis.id();
        ensure!(
            self.member_status.len() < MAX_MEMBERS,
            "Too many membership decisions"
        );
        let mut decisions = BTreeMap::new();
        for status in &self.member_status {
            ensure!(
                status.mesh == mesh
                    && status.member != self.genesis.root
                    && status.revision > 0
                    && status.retained_grants.len() < MAX_MEMBERS,
                "Invalid membership decision"
            );
            status.member.parse::<EndpointId>()?;
            verify(&self.genesis.root, &status.signature, status.payload())?;
            ensure!(
                decisions.insert(status.member.clone(), status).is_none(),
                "Duplicate membership decision"
            );
        }
        let mut members =
            BTreeMap::from([(self.genesis.root.clone(), self.genesis.device_name.clone())]);
        let mut remaining = self.grants.clone();
        while !remaining.is_empty() {
            let mut progressed = false;
            for grant in std::mem::take(&mut remaining) {
                ensure!(grant.mesh == mesh, "Grant belongs to another mesh.");
                grant.member.parse::<EndpointId>()?;
                name(&grant.name)?;
                uuid::Uuid::parse_str(&grant.request)?;
                if !members.contains_key(&grant.issuer) {
                    remaining.push(grant);
                    continue;
                }
                ensure!(
                    !members.contains_key(&grant.member),
                    "Duplicate membership grant."
                );
                verify(&grant.issuer, &grant.signature, grant.payload())?;
                members.insert(grant.member, grant.name);
                progressed = true;
            }
            ensure!(progressed, "Membership has no valid trust chain.");
        }
        for status in decisions.values().filter(|s| !s.allowed) {
            members.remove(&status.member);
        }
        loop {
            let mut changed = false;
            for grant in &self.grants {
                let retained = decisions
                    .get(&grant.issuer)
                    .is_some_and(|s| !s.allowed && s.retained_grants.contains(&grant.signature));
                if !members.contains_key(&grant.issuer) && !retained {
                    changed |= members.remove(&grant.member).is_some();
                }
            }
            if !changed {
                break;
            }
        }
        Ok(members)
    }

    pub fn set_member_allowed(
        &mut self,
        key: &SecretKey,
        member: &str,
        allowed: bool,
    ) -> Result<()> {
        self.members()?;
        ensure!(
            key.public().to_string() == self.genesis.root,
            "Only the mesh creator can remove or restore a device"
        );
        ensure!(
            member != self.genesis.root && self.grants.iter().any(|g| g.member == member),
            "Choose another mesh member"
        );
        let revision = self
            .member_status
            .iter()
            .find(|s| s.member == member)
            .map_or(Ok(1), |s| {
                s.revision
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("Membership revision exhausted"))
            })?;
        let mut status = MemberStatus {
            mesh: self.genesis.id(),
            member: member.into(),
            revision,
            allowed,
            retained_grants: self
                .grants
                .iter()
                .filter(|g| g.issuer == member)
                .map(|g| g.signature.clone())
                .collect(),
            signature: String::new(),
        };
        status.signature = sign(key, status.payload())?;
        self.member_status.retain(|s| s.member != member);
        self.member_status.push(status);
        self.members()?;
        Ok(())
    }

    pub fn admit(
        &mut self,
        key: &SecretKey,
        member: &str,
        device_name: &str,
        request: &str,
    ) -> Result<()> {
        let members = self.members()?;
        ensure!(
            members.contains_key(&key.public().to_string()),
            "Only a trusted device can admit a peer."
        );
        if members.contains_key(member) {
            return Ok(());
        }
        if self.grants.iter().any(|g| g.member == member) {
            return self.set_member_allowed(key, member, true);
        }
        ensure!(
            members.len() < MAX_MEMBERS,
            "This mesh has reached its 64-device limit."
        );
        let mut grant = Grant {
            mesh: self.genesis.id(),
            issuer: key.public().to_string(),
            member: member.to_owned(),
            name: name(device_name)?,
            request: request.to_owned(),
            signature: String::new(),
        };
        grant.signature = sign(key, grant.payload())?;
        self.grants.push(grant);
        self.members()?;
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        ensure!(self.genesis == other.genesis, "This is a different mesh.");
        other.members()?;
        let mut merged = self.clone();
        for status in &other.member_status {
            if let Some(existing) = merged
                .member_status
                .iter_mut()
                .find(|s| s.member == status.member)
            {
                ensure!(
                    existing.revision != status.revision || existing == status,
                    "Conflicting membership decisions"
                );
                if status.revision > existing.revision {
                    *existing = status.clone();
                }
            } else {
                merged.member_status.push(status.clone());
            }
        }
        for grant in &other.grants {
            if let Some(existing) = merged.grants.iter_mut().find(|g| g.member == grant.member) {
                // Refuse conflicting admissions instead of silently rewriting a trust chain.
                // The invitation issuer serializes normal concurrent decisions.
                if grant != existing {
                    bail!("Conflicting membership grants; keep the existing mesh state.");
                }
            } else {
                ensure!(
                    !merged.member_status.iter().any(|s| s.member == grant.issuer
                        && !s.allowed
                        && !s.retained_grants.contains(&grant.signature)),
                    "A removed device cannot issue new membership grants"
                );
                merged.grants.push(grant.clone());
            }
        }
        merged.members()?;
        *self = merged;
        Ok(())
    }
}

#[cfg(test)]
mod removal_tests {
    use super::*;

    #[test]
    fn signed_removal_survives_stale_sync_and_requires_root_to_restore() -> Result<()> {
        let root = SecretKey::from_bytes(&[11; 32]);
        let peer = SecretKey::from_bytes(&[12; 32]);
        let child = SecretKey::from_bytes(&[13; 32]);
        let mut ledger = Ledger::create(&root, "Mesh", "Coordinator")?;
        ledger.admit(
            &root,
            &peer.public().to_string(),
            "Worker",
            &uuid::Uuid::new_v4().to_string(),
        )?;
        ledger.admit(
            &peer,
            &child.public().to_string(),
            "Existing member",
            &uuid::Uuid::new_v4().to_string(),
        )?;
        let mut stale = ledger.clone();
        assert!(
            ledger
                .set_member_allowed(&peer, &child.public().to_string(), false)
                .is_err()
        );
        ledger.set_member_allowed(&root, &peer.public().to_string(), false)?;
        ledger.merge(&stale)?;
        assert!(!ledger.members()?.contains_key(&peer.public().to_string()));
        assert!(ledger.members()?.contains_key(&child.public().to_string()));
        stale.merge(&ledger)?;
        assert_eq!(stale.members()?, ledger.members()?);
        let mut forged = ledger.clone();
        forged.member_status[0].allowed = true;
        assert!(forged.members().is_err());
        let unknown = SecretKey::from_bytes(&[14; 32]);
        let mut old = Ledger::create(&root, "Other", "Other")?;
        // An old copy of this mesh cannot add a new device through a removed issuer.
        old.genesis = ledger.genesis.clone();
        old.grants = ledger.grants.clone();
        old.admit(
            &peer,
            &unknown.public().to_string(),
            "New",
            &uuid::Uuid::new_v4().to_string(),
        )?;
        assert!(ledger.merge(&old).is_err());
        assert!(
            !ledger
                .members()?
                .contains_key(&unknown.public().to_string())
        );
        ledger.admit(
            &root,
            &peer.public().to_string(),
            "Worker",
            &uuid::Uuid::new_v4().to_string(),
        )?;
        assert!(ledger.members()?.contains_key(&peer.public().to_string()));
        ledger.merge(&stale)?;
        assert!(ledger.members()?.contains_key(&peer.public().to_string()));
        Ok(())
    }
}
