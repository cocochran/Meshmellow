pub mod cli;
pub mod invitation;
pub mod membership;
#[cfg(test)]
mod tests;

use anyhow::{Context, Result, bail, ensure};
use iroh::{Endpoint, EndpointAddr, SecretKey};
use membership::{INVITE_SECONDS, Ledger, name, now};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, Semaphore};

const ALPN: &[u8] = b"meshthing/enrollment/1";
const MAX_WIRE: usize = 128 * 1024;

/// A bound local HTTP API, announced only to authenticated mesh members.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiEndpoint {
    pub base_url: String,
    pub scope: String,
    pub inference_ready: bool,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Peer {
    pub id: String,
    #[serde(default)]
    pub addresses: Vec<SocketAddr>,
    #[serde(default)]
    pub invite: String,
    #[serde(default)]
    pub relay: Option<String>,
}
impl Peer {
    fn addr(&self) -> Result<EndpointAddr> {
        ensure!(self.addresses.len() <= 16, "Too many peer addresses.");
        let mut addr = EndpointAddr::new(self.id.parse()?);
        for address in &self.addresses {
            addr = addr.with_ip_addr(*address);
        }
        if let Some(relay) = &self.relay {
            addr = addr.with_relay_url(relay.parse()?);
        }
        Ok(addr)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Invite {
    id: String,
    expires: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Pending {
    pub id: String,
    pub applicant: String,
    pub name: String,
    pub issuer: String,
    pub expires: u64,
    pub status: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Joining {
    peer: Peer,
    request: String,
    ledger: Ledger,
    expires: u64,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Record {
    ledger: Option<Ledger>,
    invite: Option<Invite>,
    #[serde(default)]
    requests: Vec<Pending>,
    joining: Option<Joining>,
    #[serde(default)]
    notice: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Wire {
    Join {
        invite: String,
        name: String,
    },
    Poll {
        request: String,
    },
    Cancel {
        request: String,
    },
    Sync {
        ledger: Ledger,
    },
    Decide {
        ledger: Ledger,
        request: String,
        approve: bool,
    },
}

#[derive(Serialize, Deserialize)]
struct Reply {
    #[serde(default)]
    api: Option<ApiEndpoint>,
    error: Option<String>,
    ledger: Option<Ledger>,
    #[serde(default)]
    pending: Vec<Pending>,
    status: Option<String>,
}
impl Reply {
    fn empty() -> Self {
        Self {
            api: None,
            error: None,
            ledger: None,
            pending: vec![],
            status: None,
        }
    }
}

#[derive(Clone)]
pub struct Node {
    endpoint: Endpoint,
    key: SecretKey,
    path: PathBuf,
    record: Arc<Mutex<Record>>,
    peers: Arc<Mutex<BTreeMap<String, Peer>>>,
    remote_pending: Arc<Mutex<Vec<Pending>>>,
    local_api: Arc<Mutex<Option<ApiEndpoint>>>,
    peer_apis: Arc<Mutex<BTreeMap<String, ApiEndpoint>>>,
    internet: bool,
    artifact_seed: Arc<Mutex<Option<Arc<crate::artifact::Seed>>>>,
    membership_epoch: tokio::sync::watch::Sender<u64>,
    control_peers: crate::network::SharedPeerTable,
    control_tasks: Arc<Mutex<BTreeMap<String, tokio::task::JoinHandle<()>>>>,
}

impl Node {
    pub async fn open(directory: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(directory.as_ref(), true).await
    }

    #[cfg(test)]
    pub(crate) async fn open_local(directory: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(directory.as_ref(), false).await
    }

    async fn open_with(dir: &Path, internet: bool) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let key_path = dir.join("identity.key");
        let key =
            match std::fs::read(&key_path) {
                Ok(bytes) => SecretKey::from_bytes(&bytes.try_into().map_err(|_| {
                    anyhow::anyhow!("Invalid saved identity; refusing to replace it.")
                })?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let key = SecretKey::generate();
                    write_private(&key_path, &key.to_bytes(), true)?;
                    key
                }
                Err(error) => return Err(error.into()),
            };
        let path = dir.join("membership.json");
        let record: Record = match std::fs::read(&path) {
            Ok(bytes) => {
                ensure!(bytes.len() <= MAX_WIRE, "Saved membership is too large.");
                serde_json::from_slice(&bytes)?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Record::default(),
            Err(error) => return Err(error.into()),
        };
        if let Some(ledger) = &record.ledger {
            ensure!(
                ledger.members()?.contains_key(&key.public().to_string()),
                "Saved identity is not a member of this mesh."
            );
        }
        let endpoint = if internet {
            let builder = crate::network::endpoint_builder_with_key(
                key.clone(),
                vec![
                    ALPN.to_vec(),
                    crate::protocol::CONTROL_ALPN.to_vec(),
                    crate::artifact::transport::ALPN.to_vec(),
                ],
            );
            // Internet integration tests deliberately have no IP transport, so
            // successful pairing cannot accidentally use loopback or the LAN.
            #[cfg(test)]
            let builder = builder.clear_ip_transports();
            builder.bind().await?
        } else {
            Endpoint::builder(iroh::endpoint::presets::Minimal)
                .secret_key(key.clone())
                .alpns(vec![
                    ALPN.to_vec(),
                    crate::protocol::CONTROL_ALPN.to_vec(),
                    crate::artifact::transport::ALPN.to_vec(),
                ])
                .bind()
                .await?
        };
        let node = Self {
            endpoint,
            key,
            path,
            record: Arc::new(Mutex::new(record)),
            peers: Default::default(),
            remote_pending: Default::default(),
            internet,
            artifact_seed: Default::default(),
            local_api: Default::default(),
            peer_apis: Default::default(),
            membership_epoch: tokio::sync::watch::channel(0).0,
            control_peers: Default::default(),
            control_tasks: Default::default(),
        };
        node.serve();
        node.maintain_mesh();
        Ok(node)
    }

    /// Attach verified exports to this node; membership still gates every transfer.
    pub async fn set_artifact_seed(&self, seed: Option<Arc<crate::artifact::Seed>>) {
        *self.artifact_seed.lock().await = seed;
    }

    pub(crate) async fn artifact_connection(
        &self,
        peer: &Peer,
    ) -> Result<(
        iroh::endpoint::Connection,
        tokio::sync::watch::Receiver<u64>,
    )> {
        let epoch = self.membership_epoch.subscribe();
        self.control_hello(&peer.id).await?;
        let connection = self
            .endpoint
            .connect(peer.addr()?, crate::artifact::transport::ALPN)
            .await?;
        self.control_hello(&peer.id).await?;
        Ok((connection, epoch))
    }

    pub async fn set_api_endpoint(&self, api: Option<ApiEndpoint>) {
        *self.local_api.lock().await = api;
    }

    /// Shared authenticated control connections for the embedded read-only API.
    pub fn control_peer_table(&self) -> crate::network::SharedPeerTable {
        self.control_peers.clone()
    }

    pub fn id(&self) -> String {
        self.endpoint.id().to_string()
    }
    pub fn address(&self) -> Peer {
        Peer {
            id: self.id(),
            addresses: self.endpoint.addr().ip_addrs().copied().collect(),
            invite: String::new(),
            relay: self
                .endpoint
                .addr()
                .relay_urls()
                .next()
                .map(ToString::to_string),
        }
    }
    pub async fn close(&self) {
        self.endpoint.close().await;
        for (_, task) in std::mem::take(&mut *self.control_tasks.lock().await) {
            task.abort();
        }
    }

    fn persist(&self, record: &Record) -> Result<()> {
        let bytes = serde_json::to_vec(record)?;
        ensure!(bytes.len() <= MAX_WIRE, "Membership storage limit reached.");
        let temporary = self.path.with_extension("tmp");
        write_private(&temporary, &bytes, false)?;
        std::fs::rename(&temporary, &self.path)?;
        // Unix supports syncing the parent directory after rename. Windows cannot
        // open directories with File::open; the file itself was synced above.
        #[cfg(unix)]
        std::fs::File::open(self.path.parent().unwrap())?.sync_all()?;
        Ok(())
    }

    async fn change<T>(&self, f: impl FnOnce(&mut Record) -> Result<T>) -> Result<T> {
        let mut record = self.record.lock().await;
        let mut next = record.clone();
        let result = f(&mut next)?;
        if serde_json::to_vec(&next)? != serde_json::to_vec(&*record)? {
            self.persist(&next)?;
            *record = next;
        }
        Ok(result)
    }

    pub async fn create(&self, mesh_name: &str, device_name: &str) -> Result<()> {
        self.change(|r| {
            ensure!(
                r.ledger.is_none() && r.joining.is_none(),
                "This device already has a mesh or a pending join."
            );
            r.ledger = Some(Ledger::create(&self.key, mesh_name, device_name)?);
            r.notice.clear();
            Ok(())
        })
        .await
    }

    /// Leave locally, retaining this device's identity. Existing peers retain
    /// their signed trust history; rejoining still requests explicit approval.
    pub async fn leave(&self) -> Result<()> {
        self.peer_apis.lock().await.clear();
        self.change(|r| {
            ensure!(r.ledger.is_some(), "This device has no mesh to leave.");
            *r = Record::default();
            Ok(())
        })
        .await?;
        self.membership_epoch
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        let tasks = std::mem::take(&mut *self.control_tasks.lock().await);
        for task in tasks.values() {
            task.abort();
        }
        for (_, task) in tasks {
            let _ = task.await;
        }
        *self
            .control_peers
            .lock()
            .map_err(|_| anyhow::anyhow!("Peer state unavailable."))? = Default::default();
        self.peers.lock().await.clear();
        self.remote_pending.lock().await.clear();
        Ok(())
    }

    pub async fn invite(&self) -> Result<String> {
        if self.internet {
            tokio::time::timeout(Duration::from_secs(15), self.endpoint.online())
                .await
                .context("Cannot reach Iroh. Check your Internet connection.")?;
        }
        self.change(|r| {
            ensure!(r.ledger.is_some(), "Create or join a mesh first.");
            ensure!(
                !r.requests
                    .iter()
                    .any(|p| p.status == "pending" && p.expires > now()),
                "Approve or decline the current request first."
            );
            r.requests
                .retain(|p| p.status == "approved" || p.expires > now());
            ensure!(
                r.requests.len() < 64,
                "Too many recent invitations. Try again later."
            );
            let invite = Invite {
                id: uuid::Uuid::new_v4().to_string(),
                expires: now() + INVITE_SECONDS,
            };
            let ticket = self.signed_invitation(&invite).ticket();
            r.invite = Some(invite);
            Ok(ticket)
        })
        .await
    }

    fn signed_invitation(&self, invite: &Invite) -> invitation::Invitation {
        invitation::Invitation::signed(
            &self.key,
            invite.id.clone(),
            invite.expires,
            self.endpoint
                .addr()
                .relay_urls()
                .next()
                .map(ToString::to_string),
        )
    }

    pub async fn discover(&self, peers: Vec<Peer>) -> Result<()> {
        ensure!(peers.len() <= 128, "Too many discovered devices.");
        let mut next = BTreeMap::new();
        for peer in peers {
            peer.addr()?;
            if peer.id != self.id() {
                next.insert(peer.id.clone(), peer);
            }
        }
        *self.peers.lock().await = next;
        Ok(())
    }

    pub async fn join_invitation(&self, ticket: &str, device_name: &str) -> Result<()> {
        let mut peer = invitation::Invitation::from_ticket(ticket)?.peer();
        // Optional address hints do not change the signed issuer identity.
        // Iroh authenticates that identity before accepting the connection.
        if let Some(known) = self.peers.lock().await.get(&peer.id) {
            peer.addresses = known.addresses.clone();
        }
        self.join(peer, device_name).await
    }

    pub async fn join(&self, peer: Peer, device_name: &str) -> Result<()> {
        {
            let r = self.record.lock().await;
            ensure!(
                r.ledger.is_none() && r.joining.is_none(),
                "This device already has a mesh or a pending join."
            );
        }
        let reply = self
            .rpc(
                &peer,
                Wire::Join {
                    invite: peer.invite.clone(),
                    name: name(device_name)?,
                },
            )
            .await?;
        let ledger = reply.ledger.context("Missing mesh identity.")?;
        ensure!(
            ledger.members()?.contains_key(&peer.id),
            "Invitation issuer is not trusted in this mesh."
        );
        let request = reply.pending.first().context("Missing join request.")?;
        ensure!(
            request.applicant == self.id()
                && request.issuer == peer.id
                && request.status == "pending"
                && request.expires > now(),
            "Invalid join response."
        );
        self.change(|r| {
            ensure!(
                r.ledger.is_none() && r.joining.is_none(),
                "Device membership changed during join."
            );
            r.joining = Some(Joining {
                peer,
                request: request.id.clone(),
                ledger,
                expires: request.expires,
            });
            r.notice.clear();
            Ok(())
        })
        .await
    }

    pub async fn decide(&self, issuer: &str, request: &str, approve: bool) -> Result<()> {
        let ledger = self
            .record
            .lock()
            .await
            .ledger
            .clone()
            .context("This device is not trusted yet.")?;
        if issuer == self.id() {
            self.dispatch(
                &self.id(),
                Wire::Decide {
                    ledger,
                    request: request.to_owned(),
                    approve,
                },
            )
            .await?;
        } else {
            ensure!(
                ledger.members()?.contains_key(issuer),
                "Unknown invitation issuer."
            );
            let peer = self
                .peers
                .lock()
                .await
                .get(issuer)
                .cloned()
                .unwrap_or(Peer {
                    id: issuer.to_owned(),
                    addresses: vec![],
                    invite: String::new(),
                    relay: None,
                });
            let response = self
                .rpc(
                    &peer,
                    Wire::Decide {
                        ledger,
                        request: request.to_owned(),
                        approve,
                    },
                )
                .await?;
            if let Some(next) = response.ledger {
                self.merge(&next).await?;
            }
        }
        self.remote_pending
            .lock()
            .await
            .retain(|p| p.id != request || p.issuer != issuer);
        Ok(())
    }

    async fn merge(&self, next: &Ledger) -> Result<()> {
        self.change(|r| r.ledger.as_mut().context("No mesh.")?.merge(next))
            .await
    }

    pub async fn refresh(&self) -> Result<()> {
        let r = self.record.lock().await.clone();
        if let Some(joining) = r.joining {
            {
                let peer = self
                    .peers
                    .lock()
                    .await
                    .get(&joining.peer.id)
                    .cloned()
                    .unwrap_or(joining.peer.clone());
                let reply = self
                    .rpc(
                        &peer,
                        Wire::Poll {
                            request: joining.request.clone(),
                        },
                    )
                    .await?;
                match reply.status.as_deref() {
                    Some("approved") => {
                        let mut ledger = joining.ledger;
                        ledger.merge(&reply.ledger.context("Missing membership.")?)?;
                        ensure!(
                            ledger.members()?.contains_key(&self.id()),
                            "Approval does not include this device."
                        );
                        self.change(|r| {
                            r.ledger = Some(ledger);
                            r.joining = None;
                            r.notice.clear();
                            Ok(())
                        })
                        .await?;
                    }
                    Some("declined" | "expired") => {
                        self.change(|r| {
                            r.joining = None;
                            r.notice =
                                "The request was declined or expired. Ask for a new invitation."
                                    .into();
                            Ok(())
                        })
                        .await?;
                    }
                    Some("pending") => {}
                    _ => bail!("Invalid join status."),
                }
            }
            return Ok(());
        }
        let Some(ledger) = r.ledger else {
            return Ok(());
        };
        let members = ledger.members()?;
        let peers = self.peers.lock().await.clone();
        let mut jobs = tokio::task::JoinSet::new();
        for id in members.keys().filter(|id| **id != self.id()) {
            let node = self.clone();
            let peer = peers.get(id).cloned().unwrap_or(Peer {
                id: id.clone(),
                addresses: vec![],
                invite: String::new(),
                relay: None,
            });
            let ledger = ledger.clone();
            jobs.spawn(async move {
                // An offline member must not stall every UI refresh or command.
                let reply = tokio::time::timeout(
                    Duration::from_secs(3),
                    node.rpc(&peer, Wire::Sync { ledger }),
                )
                .await
                .context("Member sync timed out.")??;
                Ok::<_, anyhow::Error>((peer.id, reply))
            });
        }
        let mut pending = vec![];
        let mut apis = BTreeMap::new();
        while let Some(result) = jobs.join_next().await {
            if let Ok(Ok((issuer, response))) = result {
                if let Some(api) = response.api {
                    apis.insert(issuer.clone(), api);
                }
                if let Some(next) = response.ledger {
                    self.merge(&next).await?;
                }
                pending.extend(
                    response.pending.into_iter().filter(|p| {
                        p.issuer == issuer && p.status == "pending" && p.expires > now()
                    }),
                );
            }
        }
        let current = self.record.lock().await;
        if current.ledger.as_ref().map(|l| l.genesis.id()) == Some(ledger.genesis.id()) {
            *self.remote_pending.lock().await = pending;
            *self.peer_apis.lock().await = apis;
        }
        Ok(())
    }

    pub async fn snapshot(&self) -> Result<Value> {
        let r = self.record.lock().await.clone();
        let mut pending: Vec<_> = r
            .requests
            .iter()
            .filter(|p| p.status == "pending" && p.expires > now())
            .cloned()
            .collect();
        pending.extend(
            self.remote_pending
                .lock()
                .await
                .clone()
                .into_iter()
                .filter(|p| p.expires > now()),
        );
        let invite = r.invite.filter(|i| i.expires > now());
        let connected: Vec<String> = self
            .control_peers
            .lock()
            .map_err(|_| anyhow::anyhow!("Peer state unavailable."))?
            .peers()
            .map(|p| p.node_id.to_string())
            .collect();
        let local_api = self.local_api.lock().await.clone();
        let apis = self.peer_apis.lock().await;
        let members = if let Some(ledger) = &r.ledger {
            ledger
                .members()?
                .into_iter()
                .map(|(id, name)| json!({"id": id, "name": name, "self": id == self.id(), "online": id == self.id() || connected.contains(&id), "api": if id == self.id() { local_api.as_ref() } else { apis.get(&id) }}))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        Ok(json!({
            "api": local_api,
            "id": self.id(), "port": self.endpoint.bound_sockets().iter().find(|s| s.is_ipv4()).map(|s| s.port()).unwrap_or(0),
            "mesh": r.ledger.as_ref().map(|l| json!({"id": l.genesis.id(), "name": l.genesis.name})),
            "members": members, "pending": pending,
            "invite": invite.map(|i| {
                json!({"id": i.id, "expires": i.expires, "ticket": self.signed_invitation(&i).ticket()})
            }),
            "connected_peers": connected,
            "joining": r.joining.map(|j| json!({"name": j.ledger.genesis.name, "expires": j.expires})),
            "notice": r.notice
        }))
    }

    pub async fn command(&self, command: Value) -> Result<Value> {
        let text = |key: &str| {
            command
                .get(key)
                .and_then(Value::as_str)
                .context(format!("Missing {key}."))
        };
        match text("op")? {
            "snapshot" => {}
            "leave" => self.leave().await?,
            "create" => self.create(text("name")?, text("device_name")?).await?,
            "invite" => {
                self.invite().await?;
            }
            "discover" => {
                self.discover(serde_json::from_value(command["peers"].clone())?)
                    .await?
            }
            "join" => {
                self.join_invitation(text("invitation")?, text("device_name")?)
                    .await?
            }
            "refresh" => self.refresh().await?,
            "decide" => {
                self.decide(
                    text("issuer")?,
                    text("request")?,
                    command["approve"].as_bool().context("Missing decision.")?,
                )
                .await?
            }
            "cancel_join" => {
                let joining = self
                    .record
                    .lock()
                    .await
                    .joining
                    .clone()
                    .context("No join request.")?;
                {
                    let peer = self
                        .peers
                        .lock()
                        .await
                        .get(&joining.peer.id)
                        .cloned()
                        .unwrap_or(joining.peer);
                    self.rpc(
                        &peer,
                        Wire::Cancel {
                            request: joining.request,
                        },
                    )
                    .await?;
                }
                self.change(|r| {
                    r.joining = None;
                    r.notice.clear();
                    Ok(())
                })
                .await?;
            }
            _ => bail!("Unknown enrollment command."),
        }
        self.snapshot().await
    }

    async fn handle(&self, remote: &str, message: Wire) -> Result<Reply> {
        self.change(|r| {
            let mut reply = Reply::empty();
            match message {
                Wire::Join {
                    invite,
                    name: device_name,
                } => {
                    let ledger = r.ledger.as_ref().context("This device has no mesh.")?;
                    // A device may have left locally while other members retain
                    // its signed trust history. It can request admission again;
                    // the invitation and explicit decision are still required.
                    let active = r.invite.as_ref().context("Invitation unavailable.")?;
                    ensure!(
                        active.id == invite && active.expires > now(),
                        "This invitation expired or was replaced."
                    );
                    let device_name = name(&device_name)?;
                    r.requests
                        .retain(|p| p.status == "approved" || p.expires > now());
                    if let Some(existing) = r.requests.iter().find(|p| p.id == invite) {
                        ensure!(
                            existing.applicant == remote && existing.status == "pending",
                            "This invitation is already in use. Ask for a new invitation."
                        );
                        reply.pending.push(existing.clone());
                    } else {
                        ensure!(r.requests.len() < 64, "Too many requests.");
                        let pending = Pending {
                            id: invite,
                            applicant: remote.to_owned(),
                            name: device_name,
                            issuer: self.id(),
                            expires: active.expires,
                            status: "pending".into(),
                        };
                        r.requests.push(pending.clone());
                        reply.pending.push(pending);
                    }
                    reply.ledger = Some(ledger.clone());
                }
                Wire::Poll { request } => {
                    if let Some(pending) = r
                        .requests
                        .iter()
                        .find(|p| p.id == request && p.applicant == remote)
                    {
                        reply.status =
                            Some(if pending.status == "pending" && pending.expires <= now() {
                                "expired".into()
                            } else {
                                pending.status.clone()
                            });
                        if pending.status == "approved" {
                            reply.ledger = r.ledger.clone();
                        }
                    } else {
                        bail!("Join request is unavailable.");
                    }
                }
                Wire::Cancel { request } => {
                    let pending = r
                        .requests
                        .iter_mut()
                        .find(|p| p.id == request && p.applicant == remote)
                        .context("Request unavailable.")?;
                    ensure!(
                        pending.status != "approved",
                        "Already approved. Refresh to finish joining."
                    );
                    pending.status = "declined".into();
                    if r.invite.as_ref().is_some_and(|i| i.id == request) {
                        r.invite = None;
                    }
                }
                Wire::Sync { ledger } | Wire::Decide { ledger, .. } => {
                    // Handled below, where the decision is retained separately.
                    let _ = ledger;
                    unreachable!();
                }
            }
            Ok(reply)
        })
        .await
    }

    async fn handle_trusted(
        &self,
        remote: &str,
        ledger: Ledger,
        decision: Option<(String, bool)>,
    ) -> Result<Reply> {
        self.change(|r| {
            let current = r.ledger.as_mut().context("This device has no mesh.")?;
            ensure!(
                ledger.members()?.contains_key(remote),
                "The sender is not trusted."
            );
            current.merge(&ledger)?;
            ensure!(
                current.members()?.contains_key(remote),
                "Only a trusted device can approve."
            );
            if let Some((request, approve)) = decision {
                let pending = r
                    .requests
                    .iter_mut()
                    .find(|p| p.id == request)
                    .context("Request is no longer available.")?;
                let desired = if approve { "approved" } else { "declined" };
                if pending.status != "pending" {
                    ensure!(
                        pending.status == desired,
                        "Another trusted device already decided this request."
                    );
                } else {
                    ensure!(
                        pending.expires > now(),
                        "This request expired. Ask for a new invitation."
                    );
                    if approve {
                        current.admit(&self.key, &pending.applicant, &pending.name, &pending.id)?;
                    }
                    pending.status = desired.into();
                    if r.invite.as_ref().is_some_and(|i| i.id == request) {
                        r.invite = None;
                    }
                }
            }
            Ok(Reply {
                api: None,
                error: None,
                ledger: Some(current.clone()),
                pending: r
                    .requests
                    .iter()
                    .filter(|p| p.status == "pending" && p.expires > now())
                    .cloned()
                    .collect(),
                status: None,
            })
        })
        .await
    }

    async fn dispatch(&self, remote: &str, message: Wire) -> Result<Reply> {
        match message {
            Wire::Sync { ledger } => {
                let mut reply = self.handle_trusted(remote, ledger, None).await?;
                reply.api = self.local_api.lock().await.clone();
                Ok(reply)
            }
            Wire::Decide {
                ledger,
                request,
                approve,
            } => {
                self.handle_trusted(remote, ledger, Some((request, approve)))
                    .await
            }
            other => self.handle(remote, other).await,
        }
    }

    /// Snapshot the authoritative mesh identity and admit only signed members.
    async fn control_hello(&self, remote: &str) -> Result<crate::protocol::Hello> {
        let record = self.record.lock().await;
        let ledger = record
            .ledger
            .as_ref()
            .context("Create or join a mesh first.")?;
        ensure!(
            ledger.members()?.contains_key(remote),
            "This device is not a trusted member."
        );
        Ok(crate::network::local_hello(
            &self.endpoint,
            &ledger.genesis.id(),
            0,
        ))
    }

    fn control_options() -> crate::network::ControlOptions {
        crate::network::ControlOptions {
            heartbeat_interval: Some(crate::leader::DEFAULT_HEARTBEAT_INTERVAL),
            heartbeat_timeout: Some(crate::leader::DEFAULT_HEARTBEAT_TIMEOUT),
        }
    }

    /// Maintain one dialer per pair, independent of UI refresh and lookup service.
    fn maintain_mesh(&self) {
        let node = self.clone();
        tokio::spawn(async move {
            while !node.endpoint.is_closed() {
                let ledger = node.record.lock().await.ledger.clone();
                if let Some(ledger) = ledger
                    && let Ok(members) = ledger.members()
                {
                    let mut tasks = node.control_tasks.lock().await;
                    // A leave may have completed while this loop held an older
                    // ledger snapshot. Never restart links for that snapshot.
                    if node
                        .record
                        .lock()
                        .await
                        .ledger
                        .as_ref()
                        .map(|l| l.genesis.id())
                        != Some(ledger.genesis.id())
                    {
                        continue;
                    }
                    tasks.retain(|id, task| {
                        if !members.contains_key(id) {
                            task.abort();
                            return false;
                        }
                        !task.is_finished()
                    });
                    for id in members.keys().filter(|id| **id > node.id()) {
                        if tasks.contains_key(id) {
                            continue;
                        }
                        let remote = id.clone();
                        let client = node.clone();
                        tasks.insert(
                            id.clone(),
                            tokio::spawn(async move {
                                while !client.endpoint.is_closed() {
                                    let Ok(hello) = client.control_hello(&remote).await else {
                                        break;
                                    };
                                    let peer =
                                        client.peers.lock().await.get(&remote).cloned().unwrap_or(
                                            Peer {
                                                id: remote.clone(),
                                                addresses: vec![],
                                                invite: String::new(),
                                                relay: None,
                                            },
                                        );
                                    let Ok(address) = peer.addr() else {
                                        break;
                                    };
                                    let dial = crate::network::dial_peer(
                                        &client.endpoint,
                                        address,
                                        &hello,
                                        &client.control_peers,
                                    );
                                    if let Ok(Ok(session)) =
                                        tokio::time::timeout(Duration::from_secs(15), dial).await
                                    {
                                        crate::network::maintain_peer_session(
                                            &session,
                                            &client.id(),
                                            &client.control_peers,
                                            Self::control_options(),
                                        )
                                        .await;
                                    }
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                }
                            }),
                        );
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    fn serve(&self) {
        let node = self.clone();
        tokio::spawn(async move {
            // At most 63 member control links plus bounded enrollment/handshake work.
            let permits = Arc::new(Semaphore::new(96));
            let artifact_permits = Arc::new(Semaphore::new(2));
            while let Some(incoming) = node.endpoint.accept().await {
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    incoming.refuse();
                    continue;
                };
                let node = node.clone();
                let artifact_permits = artifact_permits.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let Ok(Ok(connection)) =
                        tokio::time::timeout(Duration::from_secs(10), incoming).await
                    else {
                        return;
                    };
                    let remote = connection.remote_id().to_string();
                    if connection.alpn() == crate::artifact::transport::ALPN {
                        let mut epoch = node.membership_epoch.subscribe();
                        if node.control_hello(&remote).await.is_err() {
                            connection.close(1u32.into(), b"membership required");
                            return;
                        }
                        let Ok(_transfer_permit) = artifact_permits.try_acquire_owned() else {
                            connection.close(1u32.into(), b"seed busy");
                            return;
                        };
                        let Some(seed) = node.artifact_seed.lock().await.clone() else {
                            connection.close(1u32.into(), b"no seed catalog");
                            return;
                        };
                        tokio::select! {
                            _ = epoch.changed() => {},
                            _ = crate::artifact::transport::serve(&seed, connection) => {},
                        }
                        return;
                    }
                    if connection.alpn() == crate::protocol::CONTROL_ALPN {
                        let mut epoch = node.membership_epoch.subscribe();
                        let Ok(hello) = node.control_hello(&remote).await else {
                            connection.close(1u32.into(), b"membership required");
                            return;
                        };
                        if let Ok(session) = crate::network::accept_peer_connection(
                            connection,
                            &hello,
                            &node.control_peers,
                        )
                        .await
                        {
                            let id = node.id();
                            tokio::select! {
                                _ = epoch.changed() => {},
                                _ = crate::network::maintain_peer_session(
                                    &session, &id, &node.control_peers, Self::control_options(),
                                ) => {},
                            }
                        }
                        return;
                    }
                    let _ = tokio::time::timeout(Duration::from_secs(30), async move {
                        let (mut send, mut recv) = connection.accept_bi().await?;
                        let data = recv.read_to_end(MAX_WIRE).await?;
                        let reply = match serde_json::from_slice::<Wire>(&data) {
                            Ok(message) => node.dispatch(&remote, message).await,
                            Err(error) => Err(error.into()),
                        }
                        .unwrap_or_else(|e| Reply {
                            error: Some(e.to_string()),
                            ..Reply::empty()
                        });
                        send.write_all(&serde_json::to_vec(&reply)?).await?;
                        send.finish()?;
                        let _ = send.stopped().await;
                        connection.close(0u32.into(), b"complete");
                        Ok::<_, anyhow::Error>(())
                    })
                    .await;
                });
            }
        });
    }

    async fn rpc(&self, peer: &Peer, wire: Wire) -> Result<Reply> {
        tokio::time::timeout(Duration::from_secs(25), async {
            let connection = self.endpoint.connect(peer.addr()?, ALPN).await?;
            let result = async {
                let (mut send, mut recv) = connection.open_bi().await?;
                send.write_all(&serde_json::to_vec(&wire)?).await?;
                send.finish()?;
                let reply: Reply = serde_json::from_slice(&recv.read_to_end(MAX_WIRE).await?)?;
                if let Some(error) = &reply.error {
                    bail!("{error}");
                }
                Ok(reply)
            }
            .await;
            connection.close(0u32.into(), b"complete");
            result
        })
        .await
        .context("Device unavailable. Check your Internet connection and keep both apps open.")?
    }
}

fn write_private(path: &Path, bytes: &[u8], create_new: bool) -> Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true);
    if create_new {
        options.create_new(true);
    } else {
        options.create(true).truncate(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
