#!/usr/bin/env node
/**
 * @pulsar-chat/relay-runner — single-binary node operator.
 *
 * Hosts a Pulsar signaling relay on a local port AND submits proof-
 * of-uptime to the platform every 5 minutes. Anyone with Node 20+ can
 * `npx @pulsar-chat/relay-runner` after registering a node in Pulsar
 * settings to get a (Node ID, Token) pair.
 *
 * No DB, no external dependencies beyond `ws`. All state lives in
 * memory + an optional config cache (~/.pulsar-runner.json) so
 * restarts pick up the same node identity.
 *
 * For the relay protocol see apps/relay/src/index.ts — this file
 * intentionally duplicates it so the package is self-contained on
 * npm and doesn't pull the whole monorepo in transitive deps.
 */
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';

// ─── CLI / env config ──────────────────────────────────────────────

interface Config {
  nodeId: string;
  token: string;
  apiUrl: string;
  port: number;
  host: string;
}

function readConfig(): Config {
  const args = parseArgs(process.argv.slice(2));
  const cacheFile = args.config || join(homedir(), '.pulsar-runner.json');
  let cached: Partial<Config> = {};
  if (existsSync(cacheFile)) {
    try { cached = JSON.parse(readFileSync(cacheFile, 'utf8')); } catch { /* ignore */ }
  }
  const cfg: Config = {
    nodeId: args['node-id'] || process.env.PULSAR_NODE_ID || cached.nodeId || '',
    token: args.token || process.env.PULSAR_NODE_TOKEN || cached.token || '',
    apiUrl: args['api-url'] || process.env.PULSAR_API_URL || cached.apiUrl || 'https://pulsar-chat.fun',
    port: Number(args.port || process.env.PORT || cached.port || 3030),
    host: args.host || process.env.HOST || cached.host || '0.0.0.0',
  };
  if (!cfg.nodeId || !cfg.token) {
    console.error(`
Pulsar Relay Runner — missing credentials.

Get a Node ID + token by registering a node in Pulsar:
  Settings → Nodes → "Register a node"

Then run one of:
  npx @pulsar-chat/relay-runner --node-id=<id> --token=<token>
  PULSAR_NODE_ID=<id> PULSAR_NODE_TOKEN=<token> npx @pulsar-chat/relay-runner

The values are cached in ~/.pulsar-runner.json after first run.

Optional flags:
  --api-url=<url>     Override platform API (default: https://pulsar-chat.fun)
  --port=<n>          Local relay port (default: 3030)
  --host=<addr>       Bind address (default: 0.0.0.0)
  --config=<path>     Cache file path
`);
    process.exit(1);
  }
  // Persist for the next run.
  try {
    writeFileSync(cacheFile, JSON.stringify(cfg, null, 2));
  } catch { /* read-only filesystem? not fatal */ }
  return cfg;
}

function parseArgs(argv: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const a of argv) {
    const m = /^--([^=]+)(?:=(.*))?$/.exec(a);
    if (m) out[m[1]] = m[2] ?? 'true';
  }
  return out;
}

// ─── Relay (signaling pubsub — same protocol as apps/relay) ────────

const RATE_PER_SECOND = 60;
const MAX_PAYLOAD_BYTES = 8 * 1024;
const PUBKEY_RE = /^[A-Za-z0-9_-]{2,64}$/;

const subscriptions = new Map<string, Set<WebSocket>>();
interface ConnState { pubkey: string | null; pubThisSec: number; lastSec: number }
const conns = new WeakMap<WebSocket, ConnState>();

// Stats accumulated since last proof. Reset after each successful POST.
const stats = {
  bytesRelayed: 0n,
  uniquePeers: new Set<string>(),
  lifetimeBytesRelayed: 0n,
  lifetimePeersServed: new Set<string>(),
  startedAt: Date.now(),
};

function send(ws: WebSocket, msg: any) {
  if (ws.readyState !== WebSocket.OPEN) return;
  try { ws.send(JSON.stringify(msg)); } catch { /* socket closed */ }
}

function unsubscribe(ws: WebSocket) {
  const s = conns.get(ws);
  if (!s?.pubkey) return;
  const set = subscriptions.get(s.pubkey);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) subscriptions.delete(s.pubkey);
}

function startRelay(port: number, host: string) {
  const httpServer = createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        subscribers: subscriptions.size,
        uptimeSec: Math.floor((Date.now() - stats.startedAt) / 1000),
        lifetimeBytesRelayed: stats.lifetimeBytesRelayed.toString(),
        lifetimePeersServed: stats.lifetimePeersServed.size,
      }));
      return;
    }
    res.writeHead(404);
    res.end();
  });

  const wss = new WebSocketServer({ server: httpServer, path: '/ws' });

  wss.on('connection', (ws) => {
    conns.set(ws, { pubkey: null, pubThisSec: 0, lastSec: 0 });
    let alive = true;
    ws.on('pong', () => { alive = true; });
    const ka = setInterval(() => {
      if (!alive) { try { ws.terminate(); } catch {/**/} clearInterval(ka); return; }
      alive = false;
      try { ws.ping(); } catch {/**/}
    }, 30_000);

    ws.on('close', () => { clearInterval(ka); unsubscribe(ws); });
    ws.on('error', () => { /* close cleans up */ });

    ws.on('message', (raw) => {
      const buf = Buffer.isBuffer(raw) ? raw
        : Array.isArray(raw) ? Buffer.concat(raw)
        : Buffer.from(raw as ArrayBuffer);
      if (buf.length > MAX_PAYLOAD_BYTES) {
        send(ws, { kind: 'error', code: 'PAYLOAD_TOO_LARGE' });
        return;
      }
      let msg: any;
      try { msg = JSON.parse(buf.toString('utf8')); }
      catch { send(ws, { kind: 'error', code: 'BAD_JSON' }); return; }

      const state = conns.get(ws)!;
      if (msg.kind === 'ping') { send(ws, { kind: 'pong' }); return; }

      if (msg.kind === 'subscribe') {
        if (typeof msg.pubkey !== 'string' || !PUBKEY_RE.test(msg.pubkey)) {
          send(ws, { kind: 'error', code: 'BAD_PUBKEY' });
          return;
        }
        unsubscribe(ws);
        state.pubkey = msg.pubkey;
        let set = subscriptions.get(msg.pubkey);
        if (!set) { set = new Set(); subscriptions.set(msg.pubkey, set); }
        set.add(ws);
        // Stats: every distinct subscriber counts as one served peer.
        stats.uniquePeers.add(msg.pubkey);
        stats.lifetimePeersServed.add(msg.pubkey);
        send(ws, { kind: 'subscribed', pubkey: msg.pubkey });
        return;
      }

      if (msg.kind === 'publish') {
        if (!state.pubkey) { send(ws, { kind: 'error', code: 'NOT_SUBSCRIBED' }); return; }
        if (typeof msg.to !== 'string' || !PUBKEY_RE.test(msg.to)) {
          send(ws, { kind: 'error', code: 'BAD_TO' });
          return;
        }
        const now = Math.floor(Date.now() / 1000);
        if (state.lastSec !== now) { state.lastSec = now; state.pubThisSec = 0; }
        state.pubThisSec++;
        if (state.pubThisSec > RATE_PER_SECOND) {
          send(ws, { kind: 'error', code: 'RATE_LIMIT' });
          return;
        }
        const targets = subscriptions.get(msg.to);
        if (!targets) return;
        const out = JSON.stringify({ kind: 'packet', from: state.pubkey, payload: msg.payload });
        const outBytes = BigInt(Buffer.byteLength(out, 'utf8'));
        let delivered = 0;
        for (const t of targets) {
          if (t.readyState === WebSocket.OPEN) {
            try { t.send(out); delivered++; } catch { /* ignore */ }
          }
        }
        // Stats: count outgoing bytes once per delivered copy.
        stats.bytesRelayed += outBytes * BigInt(delivered);
        stats.lifetimeBytesRelayed += outBytes * BigInt(delivered);
        return;
      }

      send(ws, { kind: 'error', code: 'BAD_KIND' });
    });
  });

  httpServer.listen(port, host, () => {
    console.log(`[runner] relay listening on ${host}:${port}`);
  });
  return httpServer;
}

// ─── Proof-of-uptime submission ────────────────────────────────────

const PROOF_INTERVAL_MS = 5 * 60 * 1000;

async function submitProof(cfg: Config): Promise<void> {
  const url = `${cfg.apiUrl.replace(/\/$/, '')}/api/v1/nodes/${cfg.nodeId}/proof`;
  const body = {
    bytesRelayed: stats.bytesRelayed.toString(),
    activeConnections: countActiveConnections(),
    uniquePeers: stats.uniquePeers.size,
  };
  // Snapshot then reset — if the POST fails we *do* lose this slice,
  // which is acceptable: the next slice will resume counting cleanly,
  // and lifetime totals are tracked separately for diagnostics.
  stats.bytesRelayed = 0n;
  stats.uniquePeers.clear();

  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${cfg.token}`,
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      console.warn(`[runner] proof rejected (${res.status}):`, text.slice(0, 200));
      return;
    }
    const data = await res.json().catch(() => ({}));
    console.log(`[runner] proof submitted: ${body.bytesRelayed} bytes, ${body.uniquePeers} peers, next in ${(data as any).intervalSeconds || 300}s`);
  } catch (err) {
    console.warn(`[runner] proof failed:`, (err as Error).message);
  }
}

function countActiveConnections(): number {
  let n = 0;
  for (const set of subscriptions.values()) n += set.size;
  return n;
}

// ─── Boot ─────────────────────────────────────────────────────────

const cfg = readConfig();

console.log(`Pulsar Relay Runner v0.1.0`);
console.log(`  Node ID:  ${cfg.nodeId}`);
console.log(`  API URL:  ${cfg.apiUrl}`);
console.log(`  Listen:   ${cfg.host}:${cfg.port}`);
console.log(``);
console.log(`Earnings appear in your Pulsar wallet after 24h continuous uptime`);
console.log(`+ another 24h freeze period. Stats: ${cfg.apiUrl}/?settings=nodes`);
console.log(``);

const server = startRelay(cfg.port, cfg.host);

// Submit immediately to mark the node live, then on a steady cadence.
setTimeout(() => submitProof(cfg), 5_000);
setInterval(() => submitProof(cfg), PROOF_INTERVAL_MS);

const shutdown = (sig: string) => {
  console.log(`\n[runner] ${sig} received, shutting down`);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000); // hard kill if close hangs
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
