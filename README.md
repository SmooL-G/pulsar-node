# pulsar-node

Open-source clients that let anyone run a [Pulsar](https://pulsar-chat.fun) **relay node** and earn **PLS** for the uptime + bandwidth they contribute to the network.

Two ways to run a node, same wire protocol — pick whichever fits:

| | [`runner/`](runner/) | [`desktop/`](desktop/) |
|---|---|---|
| **Stack** | Single Node.js script | Native Rust + Tauri 2 |
| **Install** | `npx @pulsar-chat/relay-runner` | Download `.exe` / `.msi` / `.deb` |
| **Setup** | One terminal command | GUI installer, system tray |
| **Auto-start** | systemd / pm2 / NSSM (manual) | Built-in checkbox |
| **Best for** | Servers, CI, advanced users | Desktop users, "set and forget" |

Both submit the same proof-of-uptime payloads to `https://pulsar-chat.fun/api/v1/nodes/:id/proof` and earn the same PLS rates.

## Quick start

1. Open Pulsar → Settings → Nodes → **Register a node** (Verification Level 3 / Elite required).
2. Save the **Node ID** + **token** that appear (one-time display).
3. Pick a runtime:

   **Desktop (recommended for home use):**
   - Download from [Releases](https://github.com/SmooL-G/pulsar-node/releases)
   - Install, paste credentials, click **Save & Start**

   **CLI (servers):**
   ```bash
   npx @pulsar-chat/relay-runner --node-id=<id> --token=<token>
   ```

4. Open port `3030` so other Pulsar users can route through your node (UPnP / port-forward / Cloudflare Tunnel).

## Earnings

| Resource | Reward |
|----------|--------|
| Uptime | 50 PLS / hour |
| Relayed traffic | 25 PLS / GB |
| Unique peers served | 5 PLS / peer |
| **Daily cap** | 2 500 PLS / node |

- First payout requires **24h continuous uptime** (anti-sybil).
- After earning, PLS is frozen for **another 24h** before crediting your wallet (anti-fraud window).
- Read the full mechanics: https://pulsar-chat.fun/mining

## Repo layout

```
.
├── desktop/      # Tauri 2 native app (Rust + tiny HTML UI)
├── runner/       # @pulsar-chat/relay-runner npm package (TypeScript)
└── .github/
    └── workflows/
        ├── desktop-release.yml   # builds Windows/Linux on tag push
        └── runner-release.yml    # publishes npm on tag push
```

## Building from source

### Desktop
Needs Rust stable, Node 20+, and the [Tauri prerequisites](https://tauri.app/start/prerequisites/) for your OS.

```bash
cd desktop
npm install
npx @tauri-apps/cli icon ./src-tauri/icons/source.png
npm run dev      # hot-reload dev
npm run build    # produces .msi / .exe / .deb in src-tauri/target/release/bundle/
```

### Runner
```bash
cd runner
npm install
npm run build
node dist/index.js --node-id=... --token=...
```

## Contributing

PRs welcome. The protocol is stable — see [`runner/src/index.ts`](runner/src/index.ts) for the wire format. New transports (TURN integration, file caching, whisper transcription) are tracked in the main [`pulsar` roadmap](https://pulsar-chat.fun/roadmap).

## License

MIT — see [LICENSE](LICENSE).
