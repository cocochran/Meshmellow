use super::*;
use anyhow::{Context, bail};
use futures_util::TryStreamExt;
use std::{path::Path, pin::Pin, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

pub const ALPN: &[u8] = b"meshmellow/artifact/1";
const MAX_MESSAGE: usize = 8192;
#[derive(Serialize, Deserialize)]
struct Request {
    hash: String,
    offset: u64,
}
#[derive(Serialize, Deserialize)]
struct Response {
    bytes: u64,
    offset: u64,
}
pub(crate) struct Opened {
    pub reader: Pin<Box<dyn AsyncRead + Send>>,
    pub offset: u64,
    pub membership: Option<tokio::sync::watch::Receiver<u64>>,
    _connection: Option<iroh::endpoint::Connection>,
}
pub(crate) async fn membership_changed(epoch: &mut Option<tokio::sync::watch::Receiver<u64>>) {
    if let Some(epoch) = epoch {
        let _ = epoch.changed().await;
    } else {
        std::future::pending::<()>().await;
    }
}

pub(crate) async fn open(
    manifest: &Manifest,
    offset: u64,
    local: Option<&Path>,
    node: Option<&crate::mesh::Node>,
    client: &reqwest::Client,
    cancel: &CancellationToken,
) -> Result<Opened> {
    tokio::select! {
        _ = cancel.cancelled() => bail!("artifact operation cancelled"),
        result = tokio::time::timeout(Duration::from_secs(60), open_inner(manifest, offset, local, node, client)) => result.context("artifact source connection timed out")?,
    }
}
async fn open_inner(
    manifest: &Manifest,
    offset: u64,
    local: Option<&Path>,
    node: Option<&crate::mesh::Node>,
    client: &reqwest::Client,
) -> Result<Opened> {
    if let Some(path) = local {
        let mut file = tokio::fs::File::open(path).await?;
        ensure!(
            file.metadata().await?.is_file() && file.metadata().await?.len() == manifest.bytes,
            "local artifact size mismatch"
        );
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        return Ok(Opened {
            reader: Box::pin(file),
            offset,
            membership: None,
            _connection: None,
        });
    }
    match &manifest.location {
        Location::Https { url } => {
            let response = client
                .get(url)
                .header("Accept-Encoding", "identity")
                .header("Range", format!("bytes={offset}-"))
                .send()
                .await?;
            open_http(response, offset, manifest.bytes)
        }
        Location::Iroh { peer } => {
            let node = node.context("an enrolled mesh node is required for Iroh artifacts")?;
            let (connection, epoch) = node.artifact_connection(peer).await?;
            let (mut send, mut recv) = connection.open_bi().await?;
            write_message(
                &mut send,
                &Request {
                    hash: manifest.artifact_sha256.clone(),
                    offset,
                },
            )
            .await?;
            send.finish()?;
            let response: Response = read_message(&mut recv).await?;
            ensure!(
                response.bytes == manifest.bytes && response.offset == offset,
                "seed returned different artifact bounds"
            );
            Ok(Opened {
                reader: Box::pin(recv),
                offset,
                membership: Some(epoch),
                _connection: Some(connection),
            })
        }
    }
}
fn open_http(response: reqwest::Response, offset: u64, bytes: u64) -> Result<Opened> {
    ensure!(
        !response.headers().contains_key("content-encoding")
            || response.headers()["content-encoding"] == "identity",
        "encoded artifact response is not resumable"
    );
    let actual_offset = if response.status() == reqwest::StatusCode::PARTIAL_CONTENT {
        let expected = format!("bytes {offset}-{}/{bytes}", bytes - 1);
        ensure!(
            response
                .headers()
                .get("content-range")
                .and_then(|v| v.to_str().ok())
                == Some(expected.as_str()),
            "incorrect Content-Range"
        );
        offset
    } else {
        ensure!(
            response.status() == reqwest::StatusCode::OK,
            "artifact HTTP status {}",
            response.status()
        );
        ensure!(
            !response.headers().contains_key("content-range"),
            "unexpected Content-Range on 200 response"
        );
        0
    };
    if let Some(length) = response.content_length() {
        ensure!(
            length == bytes - actual_offset,
            "incorrect artifact Content-Length"
        );
    }
    let stream = response.bytes_stream().map_err(std::io::Error::other);
    Ok(Opened {
        reader: Box::pin(tokio_util::io::StreamReader::new(stream)),
        offset: actual_offset,
        membership: None,
        _connection: None,
    })
}

/// Called only after mesh membership authentication on the node's existing accept loop.
pub(crate) async fn serve(seed: &Seed, connection: iroh::endpoint::Connection) -> Result<()> {
    let (mut send, mut recv) =
        tokio::time::timeout(Duration::from_secs(15), connection.accept_bi()).await??;
    let request: Request =
        tokio::time::timeout(Duration::from_secs(15), read_message(&mut recv)).await??;
    validate_hash(&request.hash)?;
    let (path, bytes) = seed.artifact(&request.hash)?;
    ensure!(request.offset < bytes, "invalid artifact resume offset");
    let mut file = tokio::fs::File::open(path).await?;
    ensure!(
        file.metadata().await?.len() == bytes,
        "seed artifact changed size"
    );
    file.seek(std::io::SeekFrom::Start(request.offset)).await?;
    write_message(
        &mut send,
        &Response {
            bytes,
            offset: request.offset,
        },
    )
    .await?;
    let mut remaining = bytes - request.offset;
    let mut buffer = vec![0u8; 1024 * 1024];
    while remaining > 0 {
        let limit = remaining.min(buffer.len() as u64) as usize;
        let n = file.read(&mut buffer[..limit]).await?;
        ensure!(n > 0, "seed artifact truncated during transfer");
        tokio::time::timeout(Duration::from_secs(60), send.write_all(&buffer[..n])).await??;
        remaining -= n as u64;
    }
    send.finish()?;
    // Keep the connection alive until the receiver acknowledges all stream data.
    tokio::time::timeout(Duration::from_secs(60), send.stopped()).await??;
    Ok(())
}
async fn write_message<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    message: &T,
) -> Result<()> {
    let bytes = serde_json::to_vec(message)?;
    ensure!(bytes.len() <= MAX_MESSAGE, "artifact message too large");
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}
async fn read_message<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    reader: &mut R,
) -> Result<T> {
    let size = reader.read_u32().await? as usize;
    ensure!(size <= MAX_MESSAGE, "artifact message too large");
    let mut bytes = vec![0u8; size];
    reader.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn response(status: &str, headers: &str, body: &'static [u8]) -> reqwest::Response {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let head = format!("HTTP/1.1 {status}\r\nConnection: close\r\n{headers}\r\n");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
        });
        reqwest::get(format!("http://{address}/artifact"))
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn http_resume_validates_exact_range_and_ignored_range_restarts() {
        let partial = response(
            "206 Partial Content",
            "Content-Length: 3\r\nContent-Range: bytes 2-4/5\r\n",
            b"cde",
        )
        .await;
        let mut opened = open_http(partial, 2, 5).unwrap();
        assert_eq!(opened.offset, 2);
        let mut bytes = Vec::new();
        opened.reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"cde");
        let full = response("200 OK", "Content-Length: 5\r\n", b"abcde").await;
        let mut opened = open_http(full, 2, 5).unwrap();
        assert_eq!(opened.offset, 0);
        bytes.clear();
        opened.reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"abcde");
        for (status, headers, body) in [
            (
                "206 Partial Content",
                "Content-Length: 3\r\nContent-Range: bytes 1-3/5\r\n",
                b"bcd".as_slice(),
            ),
            (
                "206 Partial Content",
                "Content-Length: 3\r\n",
                b"cde".as_slice(),
            ),
            ("200 OK", "Content-Length: 3\r\n", b"abc".as_slice()),
            (
                "200 OK",
                "Content-Length: 5\r\nContent-Encoding: gzip\r\n",
                b"abcde".as_slice(),
            ),
            (
                "416 Range Not Satisfiable",
                "Content-Length: 0\r\n",
                b"".as_slice(),
            ),
        ] {
            assert!(open_http(response(status, headers, body).await, 2, 5).is_err());
        }
    }
    #[tokio::test]
    async fn interrupted_http_body_is_an_error_not_a_completed_artifact() {
        let response = response(
            "206 Partial Content",
            "Content-Length: 5\r\nContent-Range: bytes 0-4/5\r\n",
            b"ab",
        )
        .await;
        let mut opened = open_http(response, 0, 5).unwrap();
        assert!(opened.reader.read_to_end(&mut Vec::new()).await.is_err());
    }
}
