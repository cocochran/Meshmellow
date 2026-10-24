//! Iroh endpoint setup, peer connections, and the hello handshake.
//!
//! A control connection starts with a hello exchange on one bidirectional stream: the dialer
//! opens the stream and sends its [`Hello`], the acceptor validates it, records the peer, and
//! replies with its own. Both sides reject peers whose hello does not match the authenticated
//! Iroh identity, protocol version, or cluster.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow, bail, ensure};
use iroh::endpoint::presets::{self, Preset};
use iroh::endpoint::{Connection, Incoming, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::node::PeerTable;
use crate::protocol::{self, CONTROL_ALPN, Hello, Message, PROTOCOL_VERSION};

/// Peer table shared between the accept loop and the code that dials out.
pub type SharedPeerTable = Arc<Mutex<PeerTable>>;

/// Longest a peer may take to complete the hello exchange before the connection is dropped.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

/// QUIC application close code for a connection dropped because its hello exchange failed.
const HELLO_REJECTED_CODE: u32 = 1;

/// QUIC close reason sent with [`HELLO_REJECTED_CODE`].
const HELLO_REJECTED_REASON: &[u8] = b"hello rejected";

/// Length of an Ed25519 secret key in bytes.
const SECRET_KEY_LEN: usize = 32;

/// Unix permissions for a new key file: read/write for the owner only.
#[cfg(unix)]
const OWNER_ONLY_MODE: u32 = 0o600;

/// Loads the secret key at `key_path` (creating and saving it if absent) and binds an endpoint accepting `alpns`.
///
/// Uses the n0 defaults: public relays plus DNS address lookup, so peers can dial by node ID alone.
pub async fn bind_endpoint(key_path: &Path, alpns: Vec<Vec<u8>>) -> anyhow::Result<Endpoint> {
    bind_with_preset(key_path, alpns, presets::N0).await
}

/// Like [`bind_endpoint`] but with no relays and no address lookup, for tests and isolated LANs.
///
/// Peers of a local endpoint must be dialed with a full [`EndpointAddr`] that carries direct
/// addresses, such as the one returned by [`Endpoint::addr`].
pub async fn bind_local_endpoint(key_path: &Path, alpns: Vec<Vec<u8>>) -> anyhow::Result<Endpoint> {
    bind_with_preset(key_path, alpns, presets::Minimal).await
}

/// Parses a node ID string as printed by `listen` (hex, or base32).
pub fn parse_node_id(text: &str) -> anyhow::Result<EndpointId> {
    text.trim()
        .parse()
        .with_context(|| format!("invalid node ID {text:?}"))
}

/// Builds the hello this node sends on every new control connection.
pub fn local_hello(endpoint: &Endpoint, cluster_id: &str, last_log_index: u64) -> Hello {
    Hello {
        node_id: endpoint.id().to_string(),
        protocol_version: PROTOCOL_VERSION,
        cluster_id: cluster_id.to_owned(),
        last_log_index,
    }
}

/// Dials `addr` on [`CONTROL_ALPN`], exchanges hellos, and records the peer in `peers`.
///
/// Returns the open connection and the peer's hello.
pub async fn dial_peer(
    endpoint: &Endpoint,
    addr: impl Into<EndpointAddr>,
    local: &Hello,
    peers: &SharedPeerTable,
) -> anyhow::Result<(Connection, Hello)> {
    let connection = endpoint
        .connect(addr, CONTROL_ALPN)
        .await
        .context("failed to connect to peer")?;
    let hello = finish_handshake(&connection, dialer_handshake(&connection, local)).await?;
    record_peer(peers, &hello)?;
    Ok((connection, hello))
}

/// Completes an incoming control connection: exchanges hellos and records the peer in `peers`.
///
/// Returns the open connection and the peer's hello.
pub async fn accept_peer(
    incoming: Incoming,
    local: &Hello,
    peers: &SharedPeerTable,
) -> anyhow::Result<(Connection, Hello)> {
    let connection = incoming.await.context("incoming connection failed")?;
    let hello =
        finish_handshake(&connection, acceptor_handshake(&connection, local, peers)).await?;
    Ok((connection, hello))
}

/// Accepts control connections on `endpoint` until it is closed, keeping each one open until the peer leaves.
pub async fn serve_control(
    endpoint: Endpoint,
    local: Hello,
    peers: SharedPeerTable,
) -> anyhow::Result<()> {
    let local = Arc::new(local);
    let mut sessions = JoinSet::new();
    while let Some(incoming) = endpoint.accept().await {
        let local = Arc::clone(&local);
        let peers = Arc::clone(&peers);
        sessions.spawn(async move { run_control_session(incoming, &local, &peers).await });
        // Reap finished sessions so a long-running listener does not accumulate them.
        while sessions.try_join_next().is_some() {}
    }
    sessions.shutdown().await;
    Ok(())
}

/// Handles one incoming control connection from hello to disconnect, logging the outcome.
async fn run_control_session(incoming: Incoming, local: &Hello, peers: &SharedPeerTable) {
    match accept_peer(incoming, local, peers).await {
        Ok((connection, hello)) => {
            let reason = connection.closed().await;
            info!(peer = %hello.node_id, %reason, "peer disconnected");
        }
        Err(error) => warn!("rejected incoming connection: {error:#}"),
    }
}

/// Runs one side of the hello exchange under [`HELLO_TIMEOUT`], closing the connection with
/// [`HELLO_REJECTED_CODE`] on failure so the peer learns why it was dropped.
async fn finish_handshake(
    connection: &Connection,
    handshake: impl Future<Output = anyhow::Result<Hello>>,
) -> anyhow::Result<Hello> {
    let remote = connection.remote_id();
    let outcome = match tokio::time::timeout(HELLO_TIMEOUT, handshake).await {
        Ok(outcome) => outcome,
        Err(_) => Err(anyhow!("peer {remote} did not complete the hello in time")),
    };
    if outcome.is_err() {
        connection.close(HELLO_REJECTED_CODE.into(), HELLO_REJECTED_REASON);
    }
    outcome
}

/// Dialer side of the hello exchange: send ours, then read and validate theirs.
async fn dialer_handshake(connection: &Connection, local: &Hello) -> anyhow::Result<Hello> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .context("failed to open hello stream")?;
    protocol::write_frame(&mut send, &Message::Hello(local.clone())).await?;
    let hello = read_hello(&mut recv).await?;
    validate_hello(&hello, connection.remote_id(), local)?;
    finish_stream(&mut send)?;
    Ok(hello)
}

/// Acceptor side of the hello exchange: read and validate theirs, record the peer, then reply.
///
/// The peer is recorded before replying so that a dialer which has received our hello can rely on
/// being present in our table.
async fn acceptor_handshake(
    connection: &Connection,
    local: &Hello,
    peers: &SharedPeerTable,
) -> anyhow::Result<Hello> {
    let (mut send, mut recv) = connection
        .accept_bi()
        .await
        .context("peer never opened the hello stream")?;
    let hello = read_hello(&mut recv).await?;
    validate_hello(&hello, connection.remote_id(), local)?;
    record_peer(peers, &hello)?;
    protocol::write_frame(&mut send, &Message::Hello(local.clone())).await?;
    finish_stream(&mut send)?;
    Ok(hello)
}

async fn read_hello(recv: &mut RecvStream) -> anyhow::Result<Hello> {
    match protocol::read_frame(recv).await? {
        Some(Message::Hello(hello)) => Ok(hello),
        Some(other) => bail!("expected Hello, got {other:?}"),
        None => bail!("peer closed the stream before sending Hello"),
    }
}

/// Rejects a hello whose node ID differs from the authenticated `remote`, or whose version or cluster differ from ours.
fn validate_hello(hello: &Hello, remote: EndpointId, local: &Hello) -> anyhow::Result<()> {
    let claimed = parse_node_id(&hello.node_id)?;
    ensure!(
        claimed == remote,
        "peer {remote} claimed node ID {}",
        hello.node_id
    );
    ensure!(
        hello.protocol_version == local.protocol_version,
        "peer {remote} speaks protocol version {}, we speak {}",
        hello.protocol_version,
        local.protocol_version
    );
    ensure!(
        hello.cluster_id == local.cluster_id,
        "peer {remote} belongs to cluster {:?}, we belong to {:?}",
        hello.cluster_id,
        local.cluster_id
    );
    Ok(())
}

fn record_peer(peers: &SharedPeerTable, hello: &Hello) -> anyhow::Result<()> {
    let mut table = peers
        .lock()
        .map_err(|_| anyhow!("peer table lock poisoned"))?;
    let info = table.record_connection(hello);
    info!(
        peer = %info.node_id,
        protocol_version = info.protocol_version,
        connected_at_unix_ms = info.connected_at_unix_ms,
        "peer connected"
    );
    Ok(())
}

fn finish_stream(send: &mut SendStream) -> anyhow::Result<()> {
    send.finish().context("failed to finish hello stream")
}

async fn bind_with_preset(
    key_path: &Path,
    alpns: Vec<Vec<u8>>,
    preset: impl Preset,
) -> anyhow::Result<Endpoint> {
    let secret_key = load_or_create_secret_key(key_path)?;
    let endpoint = Endpoint::builder(preset)
        .secret_key(secret_key)
        .alpns(alpns)
        .bind()
        .await
        .context("failed to bind Iroh endpoint")?;
    Ok(endpoint)
}

/// Reads the hex-encoded secret key at `path`, or generates one and writes it there.
fn load_or_create_secret_key(path: &Path) -> anyhow::Result<SecretKey> {
    match std::fs::read_to_string(path) {
        Ok(text) => text
            .trim()
            .parse()
            .with_context(|| format!("invalid secret key in {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let secret_key = SecretKey::generate();
            save_secret_key(path, &secret_key)
                .with_context(|| format!("failed to save secret key to {}", path.display()))?;
            info!(path = %path.display(), "generated new node identity");
            Ok(secret_key)
        }
        Err(error) => {
            Err(error).with_context(|| format!("failed to read secret key {}", path.display()))
        }
    }
}

/// Writes a new key file, refusing to overwrite an existing one; owner-only on Unix.
fn save_secret_key(path: &Path, secret_key: &SecretKey) -> std::io::Result<()> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(OWNER_ONLY_MODE);
    }
    let mut file = options.open(path)?;
    file.write_all(encode_hex(&secret_key.to_bytes()).as_bytes())
}

fn encode_hex(bytes: &[u8; SECRET_KEY_LEN]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_key_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("meshthing-{}-{name}.key", std::process::id()))
    }

    #[test]
    fn secret_key_persists_across_loads() {
        let path = temp_key_path("persist");
        // A leftover file from an aborted run may or may not exist; either way we start fresh.
        let _ = std::fs::remove_file(&path);

        let created = load_or_create_secret_key(&path).expect("create key");
        let loaded = load_or_create_secret_key(&path).expect("load key");
        std::fs::remove_file(&path).expect("remove key");

        assert_eq!(created.public(), loaded.public());
    }

    #[test]
    fn corrupt_key_file_is_an_error() {
        let path = temp_key_path("corrupt");
        std::fs::write(&path, "not a key").expect("write key");

        let result = load_or_create_secret_key(&path);
        std::fs::remove_file(&path).expect("remove key");

        assert!(result.is_err());
    }

    #[test]
    fn node_id_round_trips_through_display() {
        let id = SecretKey::generate().public();

        assert_eq!(parse_node_id(&format!(" {id}\n")).expect("parse"), id);
        assert!(parse_node_id("nonsense").is_err());
    }

    #[test]
    fn hello_from_wrong_identity_or_cluster_is_rejected() {
        let remote = SecretKey::generate().public();
        let local = Hello {
            node_id: SecretKey::generate().public().to_string(),
            protocol_version: PROTOCOL_VERSION,
            cluster_id: "alpha".to_owned(),
            last_log_index: 0,
        };
        let honest = Hello {
            node_id: remote.to_string(),
            ..local.clone()
        };
        let impostor = Hello {
            node_id: SecretKey::generate().public().to_string(),
            ..honest.clone()
        };
        let other_cluster = Hello {
            cluster_id: "beta".to_owned(),
            ..honest.clone()
        };
        let other_version = Hello {
            protocol_version: PROTOCOL_VERSION + 1,
            ..honest.clone()
        };

        assert!(validate_hello(&honest, remote, &local).is_ok());
        assert!(validate_hello(&impostor, remote, &local).is_err());
        assert!(validate_hello(&other_cluster, remote, &local).is_err());
        assert!(validate_hello(&other_version, remote, &local).is_err());
    }
}
