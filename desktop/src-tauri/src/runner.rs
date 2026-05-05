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
use tokio_tungstenite::connect_async;
use base64::Engine;

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

fn base64_encode(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

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
    /// Optional offline-message storage. When None, Store/Fetch/Challenge
    /// tunnel frames are silently dropped (graceful degradation if SQLite
    /// init failed at boot).
    pub storage: Option<crate::storage::Storage>,
}

/// Handle for a running runner — keeps the JoinHandles so we can stop
/// everything cleanly when the user clicks Stop. Aborting drops the
/// listening socket and tears down the outbound tunnel.
pub struct RunnerHandle {
    listener: tauri::async_runtime::JoinHandle<()>,
    tunnel: tauri::async_runtime::JoinHandle<()>,
    proof: tauri::async_runtime::JoinHandle<()>,
}

impl RunnerHandle {
    pub fn abort(&self) {
        self.listener.abort();
        self.tunnel.abort();
        self.proof.abort();
        // Reset session counters so the UI shows a clean state.
        let mut st = STATS.lock();
        st.started_at = None;
        st.active_connections = 0;
        st.bytes_relayed = 0;
        st.unique_peers.clear();
        st.last_proof_status = Some("stopped".to_string());
    }
}

/// Boot the relay + proof loop. Returns a `RunnerHandle` whose tasks
/// run in the background; call `.abort()` on it to stop them.
///
/// Two parallel tasks per node:
///   1. **Local TCP listener** on `cfg.port` for users who exposed the
///      port themselves (port forwarding / Cloudflare Tunnel route).
///   2. **Outbound tunnel** to `${api_url}/node-tunnel?token=${token}`.
///      No port-forwarding required; the central relay multiplexes
///      browser sessions through this connection. This is the "just
///      paste your token" UX path — every node gets traffic regardless
///      of NAT setup.
///
/// NB: spawn through `tauri::async_runtime` (Tauri's global tokio
/// runtime), not `tokio::spawn`. The latter panics when called from a
/// synchronous Tauri command — there's no runtime in the calling
/// thread, just on Tauri's worker pool.
pub fn start(cfg: RunnerConfig) -> RunnerHandle {
    STATS.lock().started_at = Some(Instant::now());

    let subs: Subscribers = Arc::new(Mutex::new(HashMap::new()));
    let listen_addr = format!("0.0.0.0:{}", cfg.port);

    // WS server task
    let subs_local = subs.clone();
    let listener_handle = tauri::async_runtime::spawn(async move {
        let listener = match TcpListener::bind(&listen_addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[runner] bind failed: {} (tunnel mode still active)", e);
                return;
            }
        };
        eprintln!("[runner] listening on {}", listen_addr);
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let s = subs_local.clone();
                    tauri::async_runtime::spawn(handle_socket(stream, s));
                }
                Err(e) => {
                    eprintln!("[runner] accept error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    // Outbound tunnel task — auto-reconnects on drop.
    let subs_tunnel = subs.clone();
    let api_for_tunnel = cfg.api_url.clone();
    let tok_for_tunnel = cfg.token.clone();
    let storage_for_tunnel = cfg.storage.clone();
    let tunnel_handle = tauri::async_runtime::spawn(async move {
        loop {
            let result = run_tunnel(
                &api_for_tunnel,
                &tok_for_tunnel,
                subs_tunnel.clone(),
                storage_for_tunnel.clone(),
            ).await;
            let status = match result {
                Ok(()) => "tunnel: disconnected".to_string(),
                Err(e) => format!("tunnel error: {}", e),
            };
            eprintln!("[tunnel] {}, reconnecting in 5s", status);
            STATS.lock().last_proof_status = Some(status);
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    // Proof submitter task
    let api = cfg.api_url.clone();
    let nid = cfg.node_id.clone();
    let tok = cfg.token.clone();
    let proof_handle = tauri::async_runtime::spawn(async move {
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

    RunnerHandle { listener: listener_handle, tunnel: tunnel_handle, proof: proof_handle }
}

// ─── Tunnel client (outbound to central relay) ───────────────────────

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum TunnelIn {
    // Browser-session multiplexing (existing).
    Open { sid: u64 },
    Msg { sid: u64, data: serde_json::Value },
    Close { sid: u64 },
    Ping,
    // Miner-storage protocol (Phase 0). Spec: docs/MINER_STORAGE.md.
    Store {
        id: String,
        recipient: String,
        ciphertext: String,   // base64 of E2E ciphertext
        #[serde(rename = "expiresAt")]
        expires_at: i64,      // unix seconds
    },
    Challenge { id: String },
    Fetch {
        recipient: String,
        #[serde(default)]
        since: i64,
    },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum TunnelOut {
    Msg { sid: u64, data: serde_json::Value },
    #[allow(dead_code)]
    Close { sid: u64 },
    Pong,
    Stored { id: String, ok: bool },
    Proof { id: String, hash: Option<String> },
    Fetched { recipient: String, messages: Vec<FetchedMsg> },
}

#[derive(Serialize)]
struct FetchedMsg {
    id: String,
    ciphertext: String,        // base64
    #[serde(rename = "createdAt")]
    created_at: i64,
}

/// Per-virtual-session state running the same pubsub state machine that
/// `handle_socket` runs for direct TCP clients.
struct SessionState {
    subscribed_to: Option<String>,
    sec_window: u64,
    pub_this_sec: u32,
}

/// Open the tunnel ws and demux frames forever. Returns Err on any
/// transport failure; caller schedules a reconnect.
async fn run_tunnel(
    api_url: &str,
    token: &str,
    subs: Subscribers,
    storage: Option<crate::storage::Storage>,
) -> Result<(), String> {
    // Convert https://host -> wss://host/node-tunnel?token=...
    let base = api_url.trim_end_matches('/');
    let ws_base = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{}", rest)
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{}", rest)
    } else {
        format!("wss://{}", base)
    };
    let url = format!("{}/node-tunnel?token={}", ws_base, token);
    eprintln!("[tunnel] connecting to {}", ws_base);

    let (ws_stream, _resp) = connect_async(&url).await.map_err(|e| e.to_string())?;
    let (mut write, mut read) = ws_stream.split();

    eprintln!("[tunnel] connected");

    // Outgoing pump — single tx, all session sends go through it framed.
    let (tx, mut rx) = unbounded_channel::<Message>();
    let outgoing = tauri::async_runtime::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write.send(msg).await.is_err() { break; }
        }
    });

    // sid → (per-session state, mpsc-tx that produces TunnelOut::Msg{sid,...})
    let mut sessions: HashMap<u64, SessionState> = HashMap::new();
    // Per-sid sender registered in `subs`. Carries packet payloads (the
    // existing pubsub Outbound::Packet JSON). The tx wraps the data in
    // TunnelOut::Msg{sid, data} before sending to the tunnel.
    let mut session_tx_for_subs: HashMap<u64, UnboundedSender<Message>> = HashMap::new();

    while let Some(Ok(msg)) = read.next().await {
        let bytes = match &msg {
            Message::Text(t) => t.as_bytes(),
            Message::Binary(b) => b.as_slice(),
            Message::Ping(p) => {
                let _ = tx.send(Message::Pong(p.clone()));
                continue;
            }
            Message::Close(_) => break,
            _ => continue,
        };
        if bytes.len() > MAX_PAYLOAD_BYTES * 2 { continue; }

        let frame: TunnelIn = match serde_json::from_slice(bytes) {
            Ok(f) => f,
            Err(_) => continue,
        };

        match frame {
            TunnelIn::Ping => {
                let _ = tx.send(Message::Text(
                    serde_json::to_string(&TunnelOut::Pong).unwrap().into(),
                ));
            }
            TunnelIn::Open { sid } => {
                sessions.insert(sid, SessionState {
                    subscribed_to: None,
                    sec_window: 0,
                    pub_this_sec: 0,
                });
                // Per-sid forwarder: receives raw JSON strings (Outbound::Packet)
                // from subs map, wraps in TunnelOut::Msg{sid, data} and pushes
                // into the tunnel outgoing pump.
                let (sid_tx, mut sid_rx) = unbounded_channel::<Message>();
                session_tx_for_subs.insert(sid, sid_tx);
                let tunnel_tx = tx.clone();
                tauri::async_runtime::spawn(async move {
                    while let Some(msg) = sid_rx.recv().await {
                        let data_str = match msg {
                            Message::Text(t) => t.to_string(),
                            _ => continue,
                        };
                        let data: serde_json::Value = match serde_json::from_str(&data_str) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let frame_str = serde_json::to_string(&TunnelOut::Msg { sid, data }).unwrap();
                        if tunnel_tx.send(Message::Text(frame_str.into())).is_err() { break; }
                    }
                });
                STATS.lock().active_connections += 1;
            }
            TunnelIn::Msg { sid, data } => {
                let inner_str = match serde_json::to_string(&data) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let parsed: Inbound = match serde_json::from_str(&inner_str) {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                let session = match sessions.get_mut(&sid) {
                    Some(s) => s,
                    None => continue,
                };
                let sid_tx = match session_tx_for_subs.get(&sid) {
                    Some(t) => t.clone(),
                    None => continue,
                };
                handle_pubsub_inbound(&subs, session, sid_tx, parsed);
            }
            TunnelIn::Close { sid } => {
                if let Some(state) = sessions.remove(&sid) {
                    // Remove this sid's subscription from subs.
                    if let (Some(prev), Some(sid_tx)) = (
                        state.subscribed_to,
                        session_tx_for_subs.remove(&sid),
                    ) {
                        let mut s = subs.lock();
                        if let Some(list) = s.get_mut(&prev) {
                            list.retain(|t| !t.same_channel(&sid_tx));
                            if list.is_empty() { s.remove(&prev); }
                        }
                    }
                    let mut st = STATS.lock();
                    st.active_connections = st.active_connections.saturating_sub(1);
                }
            }

            // ── Miner-storage frames (Phase 0) ─────────────────────
            // Silently degrade if storage is None — disk init failed at
            // boot. Never crash the tunnel for storage failures.
            TunnelIn::Store { id, recipient, ciphertext, expires_at } => {
                let ok = match (&storage, base64_decode(&ciphertext)) {
                    (Some(s), Some(bytes)) => {
                        s.store(&id, &recipient, &bytes, expires_at).is_ok()
                    }
                    _ => false,
                };
                let _ = tx.send(Message::Text(
                    serde_json::to_string(&TunnelOut::Stored { id, ok }).unwrap().into(),
                ));
            }
            TunnelIn::Challenge { id } => {
                let hash = storage.as_ref().and_then(|s| s.proof(&id).ok().flatten());
                let _ = tx.send(Message::Text(
                    serde_json::to_string(&TunnelOut::Proof { id, hash }).unwrap().into(),
                ));
            }
            TunnelIn::Fetch { recipient, since } => {
                let messages = storage
                    .as_ref()
                    .and_then(|s| s.fetch(&recipient, since).ok())
                    .unwrap_or_default()
                    .into_iter()
                    .map(|m| FetchedMsg {
                        id: m.id,
                        ciphertext: base64_encode(&m.ciphertext),
                        created_at: m.created_at,
                    })
                    .collect();
                let _ = tx.send(Message::Text(
                    serde_json::to_string(&TunnelOut::Fetched { recipient, messages })
                        .unwrap().into(),
                ));
            }
        }
    }

    // Cleanup all sessions on tunnel drop.
    for (sid, state) in sessions.into_iter() {
        if let (Some(prev), Some(sid_tx)) = (
            state.subscribed_to,
            session_tx_for_subs.remove(&sid),
        ) {
            let mut s = subs.lock();
            if let Some(list) = s.get_mut(&prev) {
                list.retain(|t| !t.same_channel(&sid_tx));
                if list.is_empty() { s.remove(&prev); }
            }
        }
    }
    {
        let mut st = STATS.lock();
        st.active_connections = 0;
    }
    drop(tx);
    let _ = outgoing.await;
    Ok(())
}

/// Run one pubsub-protocol message for a virtual tunnel session.
/// Pulled out of `handle_socket` so direct + tunnel paths share logic.
fn handle_pubsub_inbound(
    subs: &Subscribers,
    session: &mut SessionState,
    tx: UnboundedSender<Message>,
    msg: Inbound,
) {
    match msg {
        Inbound::Ping => {
            let _ = tx.send(Message::Text(
                serde_json::to_string(&Outbound::Pong).unwrap().into(),
            ));
        }
        Inbound::Subscribe { pubkey } => {
            if !valid_pubkey(&pubkey) {
                let _ = tx.send(Message::Text(
                    serde_json::to_string(&Outbound::Error { code: "BAD_PUBKEY" }).unwrap().into(),
                ));
                return;
            }
            if let Some(prev) = session.subscribed_to.take() {
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
            session.subscribed_to = Some(pubkey);
        }
        Inbound::Publish { to, payload } => {
            let from = match &session.subscribed_to {
                Some(p) => p.clone(),
                None => {
                    let _ = tx.send(Message::Text(
                        serde_json::to_string(&Outbound::Error { code: "NOT_SUBSCRIBED" }).unwrap().into(),
                    ));
                    return;
                }
            };
            if !valid_pubkey(&to) {
                let _ = tx.send(Message::Text(
                    serde_json::to_string(&Outbound::Error { code: "BAD_TO" }).unwrap().into(),
                ));
                return;
            }
            let now_sec = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs()).unwrap_or(0);
            if now_sec != session.sec_window {
                session.sec_window = now_sec;
                session.pub_this_sec = 0;
            }
            session.pub_this_sec += 1;
            if session.pub_this_sec > RATE_PER_SECOND {
                let _ = tx.send(Message::Text(
                    serde_json::to_string(&Outbound::Error { code: "RATE_LIMIT" }).unwrap().into(),
                ));
                return;
            }
            let out_str = serde_json::to_string(&Outbound::Packet {
                from: &from, payload: &payload,
            }).unwrap();
            let out_len = out_str.len() as u64;
            let targets: Vec<UnboundedSender<Message>> = subs.lock()
                .get(&to).cloned().unwrap_or_default();
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
