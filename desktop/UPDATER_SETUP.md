# Desktop auto-updater setup

The desktop app has a "Check for updates" button that uses `tauri-plugin-updater`.
Tauri requires every update to be signed with an ed25519 keypair so a malicious
GitHub release can't push a backdoored binary.

## Status

- ✅ Public key committed to `desktop/src-tauri/tauri.conf.json`.
- ⚠️ Private key needs to be saved as repo secret `TAURI_SIGNING_PRIVATE_KEY`
  before tagging the next release, or signing will fail.

## Generating a new keypair (only if you ever need to rotate)

The keypair was generated with `scripts/gen_updater_key.py` (uses pynacl, no
extra tooling needed beyond Python). Tauri uses standard minisign format, so
this script is byte-compatible with `tauri signer generate --no-password`.

```sh
pip install pynacl
python scripts/gen_updater_key.py ~/.tauri-pulsar
```

Outputs:
- `~/.tauri-pulsar/pulsar-desktop.key.pub` — paste base64 line into
  `tauri.conf.json` `plugins.updater.pubkey`.
- `~/.tauri-pulsar/pulsar-desktop.key` — full file content goes into the
  `TAURI_SIGNING_PRIVATE_KEY` repo secret.

Alternative: trigger the `Bootstrap Updater Key` GitHub Actions workflow.

## After setup

Every `desktop-v*` tag produces signed `.sig` files alongside each bundle,
plus a `latest.json` manifest at the release. The desktop app polls
`https://github.com/SmooL-G/pulsar-node/releases/latest/download/latest.json` —
that URL always 302s to the newest release's `latest.json`, so users on older
versions will discover updates automatically.
