# Pulsar Desktop

Native Windows / Linux app that bundles a Pulsar **relay node** with a small UI and system tray.

Run it once, leave it in the tray, earn PLS while your machine is online.

## Features

- **Native relay** — Rust port of `@pulsar-chat/relay-runner`, single binary, no Node.js install needed.
- **Tray icon** — close the window, the runner keeps going. Right-click for Open / Stats / Quit.
- **Auto-start on Windows login** — toggle from the Settings panel.
- **Local stats** — uptime, active connections, bytes pending, lifetime traffic + peers.
- **Same protocol** as the web client and the JS runner — interchangeable on the network.

## Install

Download the latest release from [GitHub Releases](https://github.com/SmooL-G/pulsar/releases?q=desktop):

- Windows: `Pulsar.Desktop_x.y.z_x64-setup.exe` (NSIS) or `Pulsar.Desktop_x.y.z_x64_en-US.msi` (MSI)
- Linux: `pulsar-desktop_x.y.z_amd64.deb`

After install:

1. Open the app.
2. Get a Node ID + token from [Pulsar → Settings → Nodes](https://pulsar-chat.fun/?settings=nodes) (Verification Level 3 required).
3. Paste them in **Credentials** → **Save & Start**.
4. Optional: toggle **Start on Windows login**.
5. Open port 3030 on your router so other Pulsar users can route their P2P handshakes through your node (UPnP, port-forward, or Cloudflare Tunnel).

## Build locally

Requirements: Rust stable, Node 20+, and the [Tauri prerequisites](https://tauri.app/start/prerequisites/) for your OS.

```bash
cd desktop
npm install
npx @tauri-apps/cli icon ./src-tauri/icons/source.png
npm run dev      # dev server with hot reload
npm run build    # produces .msi / .exe / .deb in src-tauri/target/release/bundle/
```

CI builds on tag push. To cut a release:

```bash
git tag desktop-v0.1.1
git push origin desktop-v0.1.1
```

That triggers `.github/workflows/desktop-release.yml`, which builds Windows + Linux bundles and uploads them to a GitHub Release.

## Architecture

```
┌────────────────────────────────────────────────────┐
│  Tauri app (single Rust binary)                    │
│                                                    │
│   ┌──────────────┐         ┌──────────────────┐   │
│   │ WebView (UI) │ ◄─IPC─► │ runner.rs        │   │
│   │ ui/index.html│         │  - WS pubsub :3030│   │
│   └──────────────┘         │  - 5-min proof    │   │
│                            └──────────────────┘   │
│   ┌──────────────────────────────────────────┐    │
│   │  Tray icon + autostart + config persist  │    │
│   └──────────────────────────────────────────┘    │
└────────────────────────────────────────────────────┘
              │                       │
              │                       │
        wss://pulsar-chat.fun     local port 3030
        /api/v1/nodes/:id/proof   (other Pulsar clients connect here)
```

The Rust runner is at `src-tauri/src/runner.rs` — port of the JS runner, ~250 lines. Same wire protocol, same proof format. Tested-by-existence: the JS runner already talks to the same `/api/v1/nodes/:id/proof` endpoint, so this version is a drop-in replacement.

## License

MIT
