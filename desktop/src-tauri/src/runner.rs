//! Relay-runner core ported from apps/relay-runner/src/index.ts.
//!
//! Hosts a WebSocket pubsub on a local port (default 3030) and POSTs
//! proof-of-uptime to the Pulsar API every 5 minutes. Same wire
//! protocol as the JS version so the two implementations are
//! interchangeable.

use futures_util::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_tungstenite::tungstenite::Message;

const RATE_PER_SECOND: u32 = 60;
const MAX_PAYLOAD_BYTES: usize = 8 * 1024;
const PROOF_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Default)]
pub struct Stats {
    pub bytes_relayed: u64,
    pub unique_peers: HashSet<String>,
    pub lifetime_bytes: u64,
    pub lifetime_peers: HashSet<String>,
    pub started_at: Option<Instant>,
    pub active_connections: u32,
    pub last_proof_at: Option<Instant>,
    pub last_proof_status: Option<String>,
}

pub static STATS: Lazy<Mutex<Stats>> = Lazy::new(|| Mutex::new(Stats::default()));

type Subscribers = Arc<Mutex<HashMap<String, Vec<UnboundedSender<Message>>>>>;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Inbound {
    Subscribe { pubkey: String },
    Publish { to: String, payload: serde_json::Value },
    Ping,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Outbound<'a> {
    Subscribed { pubkey: &'a str },
    Packet { from: &'a str, payload: &'a serde_json::Value },
    Pong,
    Error { code: &'a str },
}

fn valid_pubkey(s: &str) -> bool {
    let n = s.len();
    if !(2..=64).contains(&n) { return false; }
    s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

async fn handle_socket(stream: TcpStream, subs: Subscribers) {
    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(s) => s,
        Err(_) => return,
    };
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = unbounded_channel::<Message>();

    STATS.lock().active_connections += 1;

    let mut subscribed_to: Option<String> = None;
    let mut sec_window: u64 = 0;
    let mut pub_this_sec: u32 = 0;

    // Outgoing pump
    let outgoing = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write.send(msg).await.is_err() { break; }
        }
    });

    while let Some(Ok(msg)) = read.next().await {
        let bytes = match &msg {
            Message::Text(t) => t.as_bytes(),
            Message::Binary(b) => b.as_slice(),
            Message::Close(_) => break,
            _ => continue,
        };
        if bytes.len() > MAX_PAYLOAD_BYTES { continue; }

        let parsed: Inbound = match serde_json::from_slice(bytes) {
            Ok(p) => p,
            Err(_) => {
                let _ = tx.send(Message::Text(serde_json::to_string(&Outbound::Error { code: "BAD_JSON" }).unwrap().into()));
                continue;
            }
        };

        match parsed {
            Inbound::Ping => {
                let _ = tx.send(Message::Text(serde_json::to_string(&Outbound::Pong).unwrap().into()));
            }
            Inbound::Subscribe { pubkey } => {
                if !valid_pubkey(&pubkey) {
                    let _ = tx.send(Message::Text(serde_json::to_string(&Outbound::Error { code: "BAD_PUBKEY" }).unwrap().into()));
                    continue;
                }
                // Drop previous subscription on this socket if any.
                if let Some(prev) = subscribed_to.take() {
                    let mut s = subs.lock();
                    if let Some(list) = s.get_mut(&prev) {
                        list.retain(|t| !t.same_channel(&tx));
                        if list.is_empty() { s.remove(&prev); }
                    }
                }
                subs.lock().entry(pubkey.clone()).or_default().push(tx.clone());
                {
                    let mut st = STATS.lock();
                    st.unique_peers.insert(pubkey.clone());
                    st.lifetime_peers.insert(pubkey.clone());
                }
                let _ = tx.send(Message::Text(
                    serde_json::to_string(&Outbound::Subscribed { pubkey: &pubkey }).unwrap().into(),
                ));
                subscribed_to = Some(pubkey);
            }
            Inbound::Publish { to, payload } => {
                let from = match &subscribed_to {
                    Some(p) => p.clone(),
                    None => {
                        let _ = tx.send(Message::Text(serde_json::to_string(&Outbound::Error { code: "NOT_SUBSCRIBED" }).unwrap().into()));
                        continue;
                    }
                };
                if !valid_pubkey(&to) {
                    let _ = tx.send(Message::Text(serde_json::to_string(&Outbound::Error { code: "BAD_TO" }).unwrap().into()));
                    continue;
                }
                let now_sec = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
                if now_sec != sec_window { sec_window = now_sec; pub_this_sec = 0; }
                pub_this_sec += 1;
                if pub_this_sec > RATE_PER_SECOND {
                    let _ = tx.send(Message::Text(serde_json::to_string(&Outbound::Error { code: "RATE_LIMIT" }).unwrap().into()));
                    continue;
                }
                let out_str = serde_json::to_string(&Outbound::Packet { from: &from, payload: &payload }).unwrap();
                let out_len = out_str.len() as u64;
                let targets: Vec<UnboundedSender<Message>> = subs.lock().get(&to).cloned().unwrap_or_default();
                let mut delivered = 0u64;
                for t in targets {
                    if t.send(Message::Text(out_str.clone().into())).is_ok() { delivered += 1; }
                }
                let mut st = STATS.lock();
                st.bytes_relayed = st.bytes_relayed.saturating_add(out_len * delivered);
                st.lifetime_bytes = st.lifetime_bytes.saturating_add(out_len * delivered);
            }
        }
    }

    // Cleanup on disconnect
    if let Some(prev) = subscribed_to {
        let mut s = subs.lock();
        if let Some(list) = s.get_mut(&prev) {
            list.retain(|t| !t.same_channel(&tx));
            if list.is_empty() { s.remove(&prev); }
        }
    }
    drop(tx);
    let _ = outgoing.await;
    STATS.lock().active_connections = STATS.lock().active_connections.saturating_sub(1);
}

#[derive(Serialize)]
struct ProofBody {
    #[serde(rename = "bytesRelayed")]
    bytes_relayed: String,
    #[serde(rename = "activeConnections")]
    active_connections: u32,
    #[serde(rename = "uniquePeers")]
    unique_peers: u32,
}

async fn submit_proof(api_url: &str, node_id: &str, token: &str) {
    let (bytes, peers, conns) = {
        let mut st = STATS.lock();
        let b = st.bytes_relayed;
        let p = st.unique_peers.len() as u32;
        let c = st.active_connections;
        st.bytes_relayed = 0;
        st.unique_peers.clear();
        (b, p, c)
    };
    let url = format!("{}/api/v1/nodes/{}/proof", api_url.trim_end_matches('/'), node_id);
    let client = reqwest::Client::new();
    let body = ProofBody {
        bytes_relayed: bytes.to_string(),
        active_connections: conns,
        unique_peers: peers,
    };
    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await;
    let mut st = STATS.lock();
    st.last_proof_at = Some(Instant::now());
    st.last_proof_status = Some(match res {
        Ok(r) => format!("{} ({} B, {} peers)", r.status(), bytes, peers),
        Err(e) => format!("err: {}", e),
    });
}

#[derive(Clone)]
pub struct RunnerConfig {
    pub api_url: String,
    pub node_id: String,
    pub token: String,
    pub port: u16,
}

/// Boot the relay + proof loop. Returns immediately; the work runs in
/// background tokio tasks. Call once per app lifetime — second call is
/// a no-op (the runner is self-cleaning when host process exits).
///
/// NB: spawn through `tauri::async_runtime` (Tauri's global tokio
/// runtime), not `tokio::spawn`. The latter panics when called from a
/// synchronous Tauri command — there's no runtime in the calling
/// thread, just on Tauri's worker pool.
pub fn start(cfg: RunnerConfig) {
    STATS.lock().started_at = Some(Instant::now());

    let subs: Subscribers = Arc::new(Mutex::new(HashMap::new()));
    let listen_addr = format!("0.0.0.0:{}", cfg.port);

    // WS server task
    let subs_clone = subs.clone();
    tauri::async_runtime::spawn(async move {
        let listener = match TcpListener::bind(&listen_addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[runner] bind failed: {}", e);
                STATS.lock().last_proof_status = Some(format!("bind error: {}", e));
                return;
            }
        };
        eprintln!("[runner] listening on {}", listen_addr);
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let s = subs_clone.clone();
                    tauri::async_runtime::spawn(handle_socket(stream, s));
                }
                Err(e) => {
                    eprintln!("[runner] accept error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    // Proof submitter task
    let api = cfg.api_url.clone();
    let nid = cfg.node_id.clone();
    let tok = cfg.token.clone();
    tauri::async_runtime::spawn(async move {
        // First proof after 5s so registration is visible immediately.
        tokio::time::sleep(Duration::from_secs(5)).await;
        submit_proof(&api, &nid, &tok).await;
        let mut interval = tokio::time::interval(PROOF_INTERVAL);
        interval.tick().await; // skip first immediate tick
        loop {
            interval.tick().await;
            submit_proof(&api, &nid, &tok).await;
        }
    });
}

#[derive(Serialize)]
pub struct StatsSnapshot {
    pub uptime_seconds: u64,
    pub active_connections: u32,
    pub unique_peers_session: u32,
    pub bytes_pending: String,
    pub lifetime_bytes: String,
    pub lifetime_peers: u32,
    pub last_proof_status: Option<String>,
}

/// Read-only snapshot for the UI.
pub fn snapshot() -> StatsSnapshot {
    let st = STATS.lock();
    StatsSnapshot {
        uptime_seconds: st.started_at.map(|t| t.elapsed().as_secs()).unwrap_or(0),
        active_connections: st.active_connections,
        unique_peers_session: st.unique_peers.len() as u32,
        bytes_pending: st.bytes_relayed.to_string(),
        lifetime_bytes: st.lifetime_bytes.to_string(),
        lifetime_peers: st.lifetime_peers.len() as u32,
        last_proof_status: st.last_proof_status.clone(),
    }
}
