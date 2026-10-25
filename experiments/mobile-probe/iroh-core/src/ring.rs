//! Diagnostic activation ring. Intermediate tensors only travel to the next peer.
use anyhow::{Context, bail, ensure};
use iroh::{
    Endpoint, EndpointId,
    endpoint::{Connection, RecvStream, SendStream, VarInt},
};
use serde_json::{Value, json};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore, oneshot},
    task::JoinSet,
};

pub const ALPN: &[u8] = b"meshthing/shard-ring/1";
type Writer = Arc<Mutex<SendStream>>;
type Callback = (Connection, BufReader<RecvStream>);
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Callback>>>>;
struct Readers<T>(Vec<tokio::task::JoinHandle<T>>);
impl<T> Drop for Readers<T> {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

fn read_messages<R: AsyncBufRead + Unpin + Send + 'static>(
    mut reader: R,
    final_result: bool,
    sender: tokio::sync::mpsc::Sender<anyhow::Result<(bool, Value)>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let value = required(&mut reader)
                .await
                .map(|value| (final_result, value));
            let failed = value.is_err();
            if sender.send(value).await.is_err() || failed {
                break;
            }
        }
    })
}

async fn line<R: AsyncBufRead + Unpin>(reader: &mut R) -> anyhow::Result<Option<Value>> {
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            ensure!(bytes.is_empty(), "truncated metadata");
            return Ok(None);
        }
        let count = available
            .iter()
            .position(|b| *b == b'\n')
            .map_or(available.len(), |i| i + 1);
        ensure!(bytes.len() + count <= 65536, "metadata exceeds limit");
        bytes.extend_from_slice(&available[..count]);
        reader.consume(count);
        if bytes.last() == Some(&b'\n') {
            return Ok(Some(serde_json::from_slice(&bytes)?));
        }
    }
}

async fn required<R: AsyncBufRead + Unpin>(reader: &mut R) -> anyhow::Result<Value> {
    tokio::time::timeout(Duration::from_secs(240), line(reader))
        .await??
        .context("peer closed")
}

async fn write_json<W: AsyncWrite + Unpin>(writer: &mut W, value: &Value) -> anyhow::Result<usize> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    ensure!(bytes.len() <= 65536, "metadata exceeds limit");
    writer.write_all(&bytes).await?;
    Ok(bytes.len())
}

async fn progress(writer: &Writer, value: &Value) -> anyhow::Result<()> {
    write_json(&mut *writer.lock().await, value).await?;
    Ok(())
}

fn number(value: &Value, name: &str) -> anyhow::Result<u64> {
    value[name]
        .as_u64()
        .with_context(|| format!("missing {name}"))
}

fn validate_info(info: &Value) -> anyhow::Result<()> {
    ensure!(number(info, "protocol")? == 1, "wrong native protocol");
    ensure!(
        number(info, "begin")? < number(info, "end")?
            && number(info, "end")? <= number(info, "layers")?,
        "invalid range"
    );
    ensure!(
        (1..=32768).contains(&number(info, "embd")?),
        "invalid hidden dimension"
    );
    let hash = info["source_sha256"]
        .as_str()
        .context("missing model identity")?;
    ensure!(
        hash.len() == 64 && hash.bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid model identity"
    );
    Ok(())
}

fn check_frame(frame: &Value, job: &str, step: u64, pos: u64, embd: u64) -> anyhow::Result<usize> {
    ensure!(
        frame["kind"] == "execute" && frame["job"] == job,
        "wrong job"
    );
    ensure!(
        number(frame, "step")? == step && number(frame, "pos")? == pos,
        "replayed/out-of-order request"
    );
    let n = number(frame, "n")?;
    ensure!((1..=128).contains(&n) && pos + n <= 4096, "invalid batch");
    let bytes = n * embd * 4;
    ensure!(
        bytes <= 16 * 1024 * 1024 && number(frame, "payload_bytes")? == bytes,
        "invalid activation size"
    );
    Ok(bytes as usize)
}

fn native_header(step: u64, pos: u64, n: u64) -> Vec<u8> {
    [0x4d534831, 2, step as u32, pos as u32, n as u32]
        .into_iter()
        .flat_map(u32::to_be_bytes)
        .collect()
}

pub async fn worker(
    endpoint: Endpoint,
    upstream: EndpointId,
    leader: EndpointId,
    next: Option<EndpointId>,
    target: SocketAddr,
) -> anyhow::Result<()> {
    ensure!(target.ip().is_loopback(), "native target must be loopback");
    let gate = Arc::new(Semaphore::new(1));
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { return Ok(()) };
                let endpoint = endpoint.clone(); let gate = gate.clone();
                tasks.spawn(async move {
                    let connection = incoming.await?;
                    if connection.remote_id() != upstream || connection.alpn() != ALPN {
                        connection.close(VarInt::from_u32(3), b"peer not approved");
                        bail!("unapproved ring peer");
                    }
                    while let Ok((send, recv)) = connection.accept_bi().await {
                        let Ok(permit) = gate.clone().try_acquire_owned() else {
                            drop(send); drop(recv); continue;
                        };
                        let result = worker_session(endpoint.clone(), leader, next, target, send, recv).await;
                        drop(permit);
                        if let Err(error) = result { eprintln!("ring worker session: {error:#}"); }
                    }
                    anyhow::Ok(())
                });
            }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    match result { Ok(Err(error)) => eprintln!("ring task: {error:#}"), Err(error) => eprintln!("ring task: {error}"), _ => {} }
            }
        }
    }
}

async fn worker_session(
    endpoint: Endpoint,
    leader: EndpointId,
    next: Option<EndpointId>,
    target: SocketAddr,
    send: SendStream,
    recv: RecvStream,
) -> anyhow::Result<()> {
    let mut upstream = BufReader::new(recv);
    let start = required(&mut upstream).await?;
    ensure!(start["kind"] == "start", "missing ring start");
    let job = start["job"].as_str().context("missing job")?.to_string();
    ensure!(
        job.len() == 32 && job.bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid job id"
    );
    let native = TcpStream::connect(target).await?;
    native.set_nodelay(true)?;
    let (read, mut native_write) = native.into_split();
    let mut native_read = BufReader::new(read);
    native_write.write_all(b"MSH1\n").await?;
    let mut info = required(&mut native_read).await?;
    validate_info(&info)?;
    info["node_id"] = json!(endpoint.id().to_string());
    let writer = Arc::new(Mutex::new(send));
    let mut stages = vec![info.clone()];
    let mut downstream = None;
    let mut result_channel = None;
    let mut relay_tasks = Readers(Vec::new());
    if let Some(peer) = next {
        let connection = endpoint.connect(peer, ALPN).await?;
        let (mut send, recv) = connection.open_bi().await?;
        write_json(&mut send, &json!({"kind":"start","job":job})).await?;
        let mut reader = BufReader::new(recv);
        let ready = required(&mut reader).await?;
        ensure!(
            ready["kind"] == "ready" && ready["job"] == job,
            "wrong downstream readiness"
        );
        let rest = ready["stages"]
            .as_array()
            .context("missing downstream metadata")?;
        let first = rest.first().context("empty downstream chain")?;
        ensure!(info["end"] == first["begin"], "noncontiguous ranges");
        for other in rest {
            validate_info(other)?;
            for key in ["layers", "embd", "vocab", "source_sha256"] {
                ensure!(info[key] == other[key], "model mismatch: {key}");
            }
        }
        stages.extend(rest.iter().cloned());
        let output = writer.clone();
        relay_tasks.0.push(tokio::spawn(async move {
            while let Some(message) = line(&mut reader).await? {
                ensure!(
                    message["kind"] == "progress",
                    "unexpected downstream control message"
                );
                progress(&output, &message).await?;
            }
            anyhow::Ok(())
        }));
        downstream = Some((connection, send));
    } else {
        ensure!(
            info["end"] == info["layers"],
            "terminal does not own final layer"
        );
        let connection = endpoint.connect(leader, ALPN).await?;
        let (mut send, recv) = connection.open_bi().await?;
        write_json(&mut send, &json!({"kind":"result_channel","job":job})).await?;
        let accepted = required(&mut BufReader::new(recv)).await?;
        ensure!(
            accepted["kind"] == "accepted" && accepted["job"] == job,
            "callback not accepted"
        );
        result_channel = Some((connection, send));
    }
    progress(&writer, &json!({"kind":"ready","job":job,"stages":stages})).await?;
    let (mut step, mut pos, mut inbound, mut outbound, mut replies) =
        (0, 0, 0usize, 0usize, 0usize);
    let outcome: anyhow::Result<()> = async {
        while let Some(mut frame) = tokio::time::timeout(Duration::from_secs(240), line(&mut upstream)).await?? {
            let size = check_frame(&frame, &job, step, pos, number(&info,"embd")?)?;
            let n = number(&frame,"n")?;
            let mut payload = vec![0; size];
            tokio::time::timeout(Duration::from_secs(120), upstream.read_exact(&mut payload)).await??;
            inbound += size;
            progress(&writer, &json!({"kind":"progress","event":"stage_start","begin":info["begin"],
                                     "step":step,"phase":if step==0 {"prefill"} else {"decode"},"input_bytes":size})).await?;
            let started = std::time::Instant::now();
            let mut request = native_header(step,pos,n); request.extend_from_slice(&payload);
            native_write.write_all(&request).await?;
            let mut result = required(&mut native_read).await?;
            ensure!(number(&result,"step")? == step, "wrong native step");
            let floats = number(&result,"n_floats")?;
            let expected = if next.is_some() { n*number(&info,"embd")? } else { 0 };
            ensure!(floats == expected && floats <= 4194304, "wrong native output shape");
            let mut hidden = vec![0; floats as usize*4];
            tokio::time::timeout(Duration::from_secs(120), native_read.read_exact(&mut hidden)).await??;
            result["wall_ms"] = json!(started.elapsed().as_secs_f64()*1000.0);
            result["request_bytes"] = json!(20+size);
            result["response_tensor_bytes"] = json!(hidden.len());
            result["node_id"] = info["node_id"].clone();
            let mut metrics = frame["metrics"].as_array().context("missing metrics")?.clone();
            ensure!(metrics.len() < 16, "chain too long");
            metrics.push(result.clone());
            progress(&writer, &json!({"kind":"progress","event":"stage_end","begin":info["begin"],
                                     "step":step,"input_bytes":size,"output_bytes":hidden.len(),"result":result})).await?;
            if let Some((_, send)) = downstream.as_mut() {
                frame["metrics"] = json!(metrics); frame["payload_bytes"] = json!(hidden.len());
                write_json(send, &frame).await?;
                send.write_all(&hidden).await?;
                outbound += hidden.len();
            } else if let Some((_, send)) = result_channel.as_mut() {
                result["kind"] = json!("result"); result["job"] = json!(job);
                result["chain_metrics"] = json!(metrics);
                replies += write_json(send, &result).await?;
            }
            step += 1; pos += n;
        }
        Ok(())
    }.await;
    drop(relay_tasks);
    if let Some((_, send)) = downstream.as_mut() {
        let _ = send.finish();
    }
    if let Some((_, send)) = result_channel.as_mut() {
        let _ = send.finish();
    }
    eprintln!(
        "RING job={job} node={} next={next:?} leader={leader} activation_in={inbound} activation_out={outbound} token_reply_bytes={replies} steps={step}",
        endpoint.id()
    );
    outcome
}

pub async fn gateway(
    endpoint: Endpoint,
    entry: EndpointId,
    terminal: EndpointId,
    port: u16,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).await?;
    let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
    let gate = Arc::new(Semaphore::new(1));
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { return Ok(()) };
                let pending = pending.clone();
                tasks.spawn(async move {
                    let connection = incoming.await?;
                    ensure!(connection.remote_id() == terminal && connection.alpn() == ALPN, "unapproved result peer");
                    while let Ok((mut send,recv)) = connection.accept_bi().await {
                        let mut reader = BufReader::new(recv);
                        let hello = required(&mut reader).await?;
                        ensure!(hello["kind"] == "result_channel", "wrong callback kind");
                        let job = hello["job"].as_str().context("missing callback job")?;
                        let sink = pending.lock().await.remove(job).context("unknown/replayed callback")?;
                        write_json(&mut send,&json!({"kind":"accepted","job":job})).await?;
                        send.finish()?;
                        let _ = sink.send((connection.clone(),reader));
                    }
                    anyhow::Ok(())
                });
            }
            accepted = listener.accept() => {
                let (tcp,_) = accepted?;
                let Ok(permit) = gate.clone().try_acquire_owned() else { continue };
                let endpoint=endpoint.clone(); let pending=pending.clone();
                tasks.spawn(async move {
                    let result=gateway_session(endpoint,entry,tcp,pending).await;
                    drop(permit);
                    if let Err(error)=result { eprintln!("ring gateway: {error:#}"); }
                    anyhow::Ok(())
                });
            }
            Some(result)=tasks.join_next(), if !tasks.is_empty() => {
                match result { Ok(Err(error))=>eprintln!("ring task: {error:#}"),Err(error)=>eprintln!("ring task panic: {error}"),_=>{} }
            }
        }
    }
}

async fn gateway_session(
    endpoint: Endpoint,
    entry: EndpointId,
    mut tcp: TcpStream,
    pending: Pending,
) -> anyhow::Result<()> {
    tcp.set_nodelay(true)?;
    let mut preface = [0; 5];
    tcp.read_exact(&mut preface).await?;
    ensure!(&preface == b"MSH1\n", "wrong client preface");
    let job = uuid::Uuid::new_v4().simple().to_string();
    let (sender, receiver) = oneshot::channel();
    pending.lock().await.insert(job.clone(), sender);
    let outcome: anyhow::Result<()> = async {
        let connection=endpoint.connect(entry,ALPN).await?;
        let (mut send,recv)=connection.open_bi().await?;
        write_json(&mut send,&json!({"kind":"start","job":job})).await?;
        let mut control=BufReader::new(recv);
        let ready=required(&mut control).await?;
        ensure!(ready["kind"]=="ready" && ready["job"]==job,"wrong ring readiness");
        let stages=ready["stages"].as_array().context("missing chain")?;
        let first=stages.first().context("empty chain")?;
        let last=stages.last().context("empty chain")?;
        let mut info=first.clone();info["end"]=last["end"].clone();info["ring_protocol"]=json!(1);
        info["chain_stages"]=json!(stages);
        write_json(&mut tcp,&info).await?;
        let (_result_connection,results)=tokio::time::timeout(Duration::from_secs(240),receiver).await??;
        let (messages,mut incoming)=tokio::sync::mpsc::channel(64);
        let _readers=Readers(vec![read_messages(control,false,messages.clone()),read_messages(results,true,messages)]);
        let (mut step,mut pos,mut sent,mut received)=(0,0,0usize,0usize);
        loop {
            let mut header=[0u8;20];
            match tcp.read_exact(&mut header[..1]).await {
                Ok(_)=>{}, Err(error) if error.kind()==std::io::ErrorKind::UnexpectedEof=>break,Err(error)=>return Err(error.into())
            }
            tcp.read_exact(&mut header[1..]).await?;
            let values:Vec<u32>=header.chunks_exact(4).map(|s|u32::from_be_bytes(s.try_into().unwrap())).collect();
            ensure!(values[0]==0x4d534831 && values[1]==2 && values[2] as u64==step && values[3] as u64==pos,"invalid client step");
            let n=values[4] as u64;
            let frame=json!({"kind":"execute","job":job,"step":step,"pos":pos,"n":n,
                             "payload_bytes":n*number(first,"embd")?*4,"metrics":[]});
            let size=check_frame(&frame,&job,step,pos,number(first,"embd")?)?;
            let mut payload=vec![0;size];tcp.read_exact(&mut payload).await?;
            write_json(&mut send,&frame).await?;send.write_all(&payload).await?;sent+=size;
            loop {
                let (final_result,message)=incoming.recv().await.context("ring readers stopped")??;
                if final_result {
                    ensure!(message["kind"]=="result" && message["job"]==job && number(&message,"step")?==step && number(&message,"n_floats")?==0,"invalid final result");
                    received+=write_json(&mut tcp,&message).await?;break;
                } else {
                    ensure!(message["kind"]=="progress","unexpected control response");
                    write_json(&mut tcp,&message).await?;
                }
            }
            step+=1;pos+=n;
        }
        send.finish()?;
        eprintln!("RING gateway job={job} activation_to_entry={sent} activation_received=0 final_reply_bytes={received} steps={step}");
        Ok(())
    }.await;
    pending.lock().await.remove(&job);
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_replay_and_wrong_payload() {
        let frame =
            json!({"kind":"execute","job":"j","step":0,"pos":0,"n":1,"payload_bytes":20480});
        assert_eq!(check_frame(&frame, "j", 0, 0, 5120).unwrap(), 20480);
        assert!(check_frame(&frame, "j", 1, 0, 5120).is_err());
        assert!(check_frame(&frame, "j", 0, 0, 4096).is_err());
        assert!(check_frame(&frame, "other", 0, 0, 5120).is_err());
    }
    #[tokio::test]
    async fn rejects_oversized_metadata() {
        let bytes = vec![b'x'; 65537];
        assert!(line(&mut BufReader::new(bytes.as_slice())).await.is_err());
    }
}
