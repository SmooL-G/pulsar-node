# Desktop auto-updater setup (one-time)

The desktop app has a "Check for updates" button that uses `tauri-plugin-updater`.
Tauri requires every update to be signed with an ed25519 keypair so a malicious
GitHub release can't push a backdoored binary. The keypair must exist before the
first signed release is built.

## Steps

1. **Generate the keypair** — run the workflow `Bootstrap Updater Key` from the
   GitHub Actions tab (`workflow_dispatch`). It'll:
   - Print the **public key** at the end of the log.
   - Upload a 1-day artifact `updater-private-key` containing `updater.key` and
     `updater.key.pub`.

2. **Paste the public key** into `desktop/src-tauri/tauri.conf.json`:
   ```json
   "plugins": {
     "updater": {
       "pubkey": "<paste here, the long base64 line from updater.key.pub>",
       ...
     }
   }
   ```

3. **Add the private key as a repo secret**:
   - Settings → Secrets and variables → Actions → New repository secret
   - Name: `TAURI_SIGNING_PRIVATE_KEY`
   - Value: the full content of `updater.key` (multi-line, base64 + headers)

4. **Delete the artifact** from the workflow run (you have the secret saved now).

5. **Optional: delete the bootstrap workflow file** to prevent accidental
   re-runs that might clobber the key.

## After setup

Every `desktop-v*` tag now produces signed `.sig` files alongside each bundle,
plus a `latest.json` manifest at the release. The desktop app polls
`https://github.com/SmooL-G/pulsar-node/releases/latest/download/latest.json` —
that URL always 302s to the newest release's `latest.json`, so users on older
versions will discover updates automatically.
