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
        Ok(members)
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
        for grant in &other.grants {
            if let Some(existing) = merged.grants.iter_mut().find(|g| g.member == grant.member) {
                // Refuse conflicting admissions instead of silently rewriting a trust chain.
                // The invitation issuer serializes normal concurrent decisions.
                if grant != existing {
                    bail!("Conflicting membership grants; keep the existing mesh state.");
                }
            } else {
                merged.grants.push(grant.clone());
            }
        }
        merged.members()?;
        *self = merged;
        Ok(())
    }
}
