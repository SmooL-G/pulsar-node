# @pulsar-chat/relay-runner

Run a [Pulsar](https://pulsar-chat.fun) signaling-relay node on your own machine and earn **PLS** for the uptime + relayed traffic.

Single-binary, no Docker, no config files needed — just Node.js 20+ and a one-time pair of credentials from the Pulsar app.

## Quick start

1. **Get credentials.** Open Pulsar → Settings → Nodes → "Register a node". Save the Node ID and one-time token (it won't be shown again).

2. **Run the runner:**

   ```bash
   npx @pulsar-chat/relay-runner --node-id=<YOUR-NODE-ID> --token=<YOUR-TOKEN>
   ```

   That's it. The runner starts a relay on port 3030 and submits proof-of-uptime to the platform every 5 minutes.

3. **Open port 3030** on your router (UPnP, port-forward, or Cloudflare Tunnel) so other Pulsar users can reach your node and route their P2P handshakes through you. Without an open port the node still earns base uptime PLS, just less.

## Earnings

| Resource | Reward |
|----------|--------|
| Uptime | 50 PLS / hour |
| Relayed traffic | 25 PLS / GB |
| Unique peers served | 5 PLS / peer |
| **Daily cap** | 2 500 PLS / node |

Notes:
- First payout requires **24h continuous uptime** (anti-sybil).
- After earning, PLS sits frozen for **another 24h** before crediting your wallet (anti-fraud window).
- A node going offline > 30 min gets marked STALE and stops earning until it reconnects.

## Eligibility

Registering a node requires **Verification Level 3** (Elite — 25 000 PLS one-time burn) on the Pulsar account that owns the node. This stops bot-farm sybil attacks: a fraudulent operator must commit 25k PLS upfront and earn it back over weeks.

## Usage

### Via CLI flags

```bash
pulsar-relay --node-id=abc-123 --token=xyz-secret
```

### Via env vars

```bash
PULSAR_NODE_ID=abc-123 \
PULSAR_NODE_TOKEN=xyz-secret \
pulsar-relay
```

### Via cached config

After the first successful run, credentials are saved to `~/.pulsar-runner.json`. Subsequent runs need no flags:

```bash
npx @pulsar-chat/relay-runner
```

### All options

| Flag | Env var | Default | What |
|------|---------|---------|------|
| `--node-id=<id>` | `PULSAR_NODE_ID` | (required) | Your node UUID |
| `--token=<t>` | `PULSAR_NODE_TOKEN` | (required) | Per-node bearer secret |
| `--api-url=<url>` | `PULSAR_API_URL` | `https://pulsar-chat.fun` | Override platform endpoint |
| `--port=<n>` | `PORT` | `3030` | Local relay port |
| `--host=<addr>` | `HOST` | `0.0.0.0` | Bind address |
| `--config=<path>` | — | `~/.pulsar-runner.json` | Cache file path |

## Running as a service

### systemd (Linux)

`/etc/systemd/system/pulsar-relay.service`:

```ini
[Unit]
Description=Pulsar Relay Runner
After=network.target

[Service]
Type=simple
User=pulsar
Environment=PULSAR_NODE_ID=abc-123
Environment=PULSAR_NODE_TOKEN=xyz-secret
ExecStart=/usr/bin/npx -y @pulsar-chat/relay-runner
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now pulsar-relay
sudo journalctl -u pulsar-relay -f
```

### pm2

```bash
pm2 start "npx @pulsar-chat/relay-runner" \
  --name pulsar-relay \
  --restart-delay 10000
pm2 save
pm2 startup
```

### Windows Service (via NSSM)

```bat
nssm install pulsar-relay "C:\Program Files\nodejs\npx.cmd" "@pulsar-chat/relay-runner"
nssm set pulsar-relay AppEnvironmentExtra PULSAR_NODE_ID=... PULSAR_NODE_TOKEN=...
nssm start pulsar-relay
```

## How it works

The runner is a single-file Node.js program (~300 LOC):

1. Hosts a stateless WebSocket pub/sub on `/ws` — same protocol as the reference relay (`apps/relay`). Pulsar web/mobile clients use it to exchange WebRTC signaling packets directly with each other instead of going through the central server.
2. Counts bytes relayed + unique pubkeys served in memory.
3. Every 5 min POSTs `{ bytesRelayed, activeConnections, uniquePeers }` to `/api/v1/nodes/{id}/proof` with the bearer token.
4. Resets the per-slice counters after a successful proof.

The platform aggregates proofs in a daily payout job at 11:00 UTC. Lifetime stats persist across restarts (you can see them on Pulsar Settings → Nodes).

## Security notes

- The token is a per-node bearer secret. **Don't share it.** You can rotate it anytime from Settings → Nodes → 🔁 — old token immediately invalidated.
- The relay has no access to message *content* — Pulsar messages are E2E encrypted, the relay only forwards encrypted signaling envelopes addressed by Solana pubkey.
- Local rate limit: 60 publishes/sec per WebSocket. Server-side: 30 proofs/hour per node. Either keeps a misconfigured client from blowing things up.

## License

MIT — see [LICENSE](LICENSE).

## Links

- 🌐 https://pulsar-chat.fun
- 📖 [Mining program docs](https://pulsar-chat.fun/mining)
- 🐛 [Issues](https://github.com/SmooL-G/pulsar/issues)
