//! Signed, self-contained invitations. An invitation never grants membership.
use super::{
    Peer,
    membership::{INVITE_SECONDS, now},
};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use iroh::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
const MAX_MESSAGE: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Invitation {
    pub issuer: String,
    pub invite: String,
    pub expires: u64,
    pub relay: Option<String>,
    pub signature: String,
}
impl Invitation {
    fn payload(&self) -> Vec<u8> {
        serde_json::to_vec(&(
            "layercake/invitation/1",
            &self.issuer,
            &self.invite,
            self.expires,
            &self.relay,
        ))
        .unwrap()
    }
    pub fn signed(key: &SecretKey, invite: String, expires: u64, relay: Option<String>) -> Self {
        let mut result = Self {
            issuer: key.public().to_string(),
            invite,
            expires,
            relay,
            signature: String::new(),
        };
        result.signature = hex::encode(key.sign(&result.payload()).to_bytes());
        result
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.expires > now() && self.expires <= now() + INVITE_SECONDS,
            "Invitation expired or has an invalid lifetime."
        );
        uuid::Uuid::parse_str(&self.invite)?;
        let signature: [u8; 64] = hex::decode(&self.signature)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid invitation signature."))?;
        self.issuer
            .parse::<EndpointId>()?
            .verify(&self.payload(), &iroh::Signature::from_bytes(&signature))?;
        if let Some(relay) = &self.relay {
            let _: iroh::RelayUrl = relay.parse()?;
        }
        Ok(())
    }
    /// Self-contained signed bootstrap; no directory lookup is needed.
    pub fn ticket(&self) -> String {
        format!(
            "layercake://join/{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).unwrap())
        )
    }

    pub fn from_ticket(ticket: &str) -> Result<Self> {
        ensure!(ticket.len() <= MAX_MESSAGE, "Invitation is too large.");
        let encoded = ticket
            .trim()
            .strip_prefix("layercake://join/")
            .context("Invalid Layercake invitation.")?;
        let locator: Self = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?;
        locator.validate()?;
        Ok(locator)
    }

    pub fn peer(&self) -> Peer {
        Peer {
            id: self.issuer.clone(),
            addresses: vec![],
            invite: self.invite.clone(),
            relay: self.relay.clone(),
        }
    }
}
