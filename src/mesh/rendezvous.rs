//! Invitation lookup only. A registry response never grants membership.
use super::{
    Peer,
    membership::{INVITE_SECONDS, emoji_code, normalize_code, now},
};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, Semaphore};

pub const ALPN: &[u8] = b"layercake/invitations/1";
const MAX_MESSAGE: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Locator {
    pub issuer: String,
    pub invite: String,
    pub expires: u64,
    pub relay: Option<String>,
    pub signature: String,
}
impl Locator {
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
    pub fn validate(&self, code: &str) -> Result<()> {
        ensure!(
            self.expires > now() && self.expires <= now() + INVITE_SECONDS,
            "Invitation expired or has an invalid lifetime."
        );
        uuid::Uuid::parse_str(&self.invite)?;
        ensure!(
            emoji_code(&self.issuer, &self.invite) == normalize_code(code)?,
            "Invitation does not match these emojis."
        );
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
        locator.validate(&emoji_code(&locator.issuer, &locator.invite))?;
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

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Request {
    Publish { locator: Locator },
    Resolve { code: String },
}
#[derive(Serialize, Deserialize)]
struct Reply {
    locator: Option<Locator>,
    error: Option<String>,
}

#[derive(Default)]
struct Registry {
    entries: BTreeMap<String, Locator>,
    requests: BTreeMap<String, (u64, u32)>,
}
impl Registry {
    fn handle(&mut self, remote: &str, request: Request) -> Result<Option<Locator>> {
        let time = now();
        self.entries.retain(|_, entry| entry.expires > time);
        self.requests
            .retain(|_, (start, _)| time.saturating_sub(*start) < 60);
        ensure!(
            self.requests.contains_key(remote) || self.requests.len() < 2048,
            "Invitation lookup is busy. Try again shortly."
        );
        let (_, count) = self.requests.entry(remote.to_owned()).or_insert((time, 0));
        ensure!(
            *count < 60,
            "Too many invitation requests. Try again in a minute."
        );
        *count += 1;
        match request {
            Request::Publish { locator } => {
                ensure!(
                    locator.issuer == remote,
                    "Only the invitation issuer can publish it."
                );
                let code = emoji_code(&locator.issuer, &locator.invite);
                locator.validate(&code)?;
                if let Some(existing) = self.entries.get(&code) {
                    ensure!(
                        existing.issuer == remote,
                        "Invitation collision. Create a new code."
                    );
                }
                ensure!(
                    self.entries.len() < 4096 || self.entries.values().any(|e| e.issuer == remote),
                    "Invitation lookup is full. Try again later."
                );
                self.entries.retain(|_, entry| entry.issuer != remote);
                self.entries.insert(code, locator);
                Ok(None)
            }
            Request::Resolve { code } => Ok(Some(
                self.entries
                    .get(&normalize_code(&code)?)
                    .context(
                        "Invitation not found or expired. Check the emojis or ask for a new code.",
                    )?
                    .clone(),
            )),
        }
    }
}

pub fn configured_address() -> Result<Option<EndpointAddr>> {
    let id = std::env::var("LAYERCAKE_RENDEZVOUS_ID")
        .ok()
        .or_else(|| option_env!("LAYERCAKE_RENDEZVOUS_ID").map(str::to_owned));
    id.filter(|id| !id.is_empty())
        .map(|id| Ok(EndpointAddr::new(id.parse()?)))
        .transpose()
}

async fn request(endpoint: &Endpoint, registry: EndpointAddr, request: Request) -> Result<Reply> {
    tokio::time::timeout(Duration::from_secs(25), async {
        let connection = endpoint.connect(registry, ALPN).await?;
        let result = async {
            let (mut send, mut recv) = connection.open_bi().await?;
            send.write_all(&serde_json::to_vec(&request)?).await?;
            send.finish()?;
            let reply: Reply = serde_json::from_slice(&recv.read_to_end(MAX_MESSAGE).await?)?;
            if let Some(error) = &reply.error {
                anyhow::bail!("{error}");
            }
            Ok(reply)
        }
        .await;
        connection.close(0u32.into(), b"complete");
        result
    })
    .await
    .context("Invitation lookup timed out. Check your Internet connection and try again.")?
}
pub async fn publish(endpoint: &Endpoint, registry: EndpointAddr, locator: Locator) -> Result<()> {
    request(endpoint, registry, Request::Publish { locator }).await?;
    Ok(())
}
pub async fn resolve(endpoint: &Endpoint, registry: EndpointAddr, code: &str) -> Result<Peer> {
    let locator = request(
        endpoint,
        registry,
        Request::Resolve {
            code: normalize_code(code)?,
        },
    )
    .await?
    .locator
    .context("Missing invitation.")?;
    locator.validate(code)?;
    Ok(locator.peer())
}

/// Serves bounded, expiring public locators; never stores device names, keys,
/// membership ledgers, prompts or model data. Endpoint ownership stays with caller.
pub async fn serve(endpoint: Endpoint) {
    let registry = Arc::new(Mutex::new(Registry::default()));
    let permits = Arc::new(Semaphore::new(32));
    while let Some(incoming) = endpoint.accept().await {
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            incoming.refuse();
            continue;
        };
        let registry = registry.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _ = tokio::time::timeout(Duration::from_secs(15), async {
                let connection = incoming.await?;
                let (mut send, mut recv) = connection.accept_bi().await?;
                let body = recv.read_to_end(MAX_MESSAGE).await?;
                let result = match serde_json::from_slice(&body) {
                    Ok(request) => registry
                        .lock()
                        .await
                        .handle(&connection.remote_id().to_string(), request),
                    Err(error) => Err(error.into()),
                };
                let reply = match result {
                    Ok(locator) => Reply {
                        locator,
                        error: None,
                    },
                    Err(error) => Reply {
                        locator: None,
                        error: Some(error.to_string()),
                    },
                };
                send.write_all(&serde_json::to_vec(&reply)?).await?;
                send.finish()?;
                let _ = send.stopped().await;
                connection.close(0u32.into(), b"complete");
                Ok::<_, anyhow::Error>(())
            })
            .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_rejects_impersonation_tampering_expiry_and_wrong_codes() {
        let key = SecretKey::generate();
        let other = SecretKey::generate();
        let locator = Locator::signed(&key, uuid::Uuid::new_v4().to_string(), now() + 300, None);
        let code = emoji_code(&locator.issuer, &locator.invite);
        let mut registry = Registry::default();
        assert!(
            registry
                .handle(
                    &other.public().to_string(),
                    Request::Publish {
                        locator: locator.clone()
                    }
                )
                .is_err()
        );
        registry
            .handle(
                &locator.issuer,
                Request::Publish {
                    locator: locator.clone(),
                },
            )
            .unwrap();
        let resolved = registry
            .handle(
                &other.public().to_string(),
                Request::Resolve { code: code.clone() },
            )
            .unwrap()
            .unwrap();
        resolved.validate(&code).unwrap();
        let mut tampered = locator.clone();
        tampered.relay = Some("https://example.com".into());
        assert!(tampered.validate(&code).is_err());
        assert!(
            locator
                .validate(&emoji_code(&other.public().to_string(), &locator.invite))
                .is_err()
        );
        let expired = Locator::signed(&key, locator.invite.clone(), now() - 1, None);
        assert!(
            registry
                .handle(&locator.issuer, Request::Publish { locator: expired })
                .is_err()
        );
        let replacement =
            Locator::signed(&key, uuid::Uuid::new_v4().to_string(), now() + 300, None);
        registry
            .handle(
                &locator.issuer,
                Request::Publish {
                    locator: replacement,
                },
            )
            .unwrap();
        assert!(
            registry
                .handle(&other.public().to_string(), Request::Resolve { code })
                .is_err()
        );
    }
}
