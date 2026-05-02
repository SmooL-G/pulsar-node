# Pulsar Android (TWA)

Trusted Web Activity wrapper for the Pulsar PWA. Loads `https://pulsar-chat.fun` inside an Android shell so users get an installable app from the website (no Play Store required).

The web is the source of truth — every Pulsar update lands automatically on phones that have the APK installed. The APK only needs rebuilding when:
- App icon changes
- Manifest theme colors change
- We bump SDK targets
- Bubblewrap version changes

## How it ships

CI workflow `.github/workflows/android-release.yml` builds a signed APK + AAB on every `android-v*` tag and uploads to GitHub Releases (public repo → anyone can download without login).

The website's `/download` page links to the latest release asset.

## First-time keystore setup

The signing key MUST be stable across versions — Android refuses to install over an existing app signed by a different key. Two options:

### Option A: Let CI generate one (test path)

Push a tag without setting any secrets. The workflow will:
1. Generate a fresh keystore
2. Print the SHA-256 fingerprint to logs
3. Upload the keystore (base64) as a workflow artifact

Then:
1. Download the `keystore` artifact from the workflow run page
2. Open it — that's the base64 of `android.keystore`
3. Add to repo secrets:
   - `ANDROID_KEYSTORE_B64` — the entire base64 contents
   - `ANDROID_KEYSTORE_PASSWORD` — the password printed in the logs
4. Update `apps/web/public/.well-known/assetlinks.json` in the **main pulsar repo** with the SHA-256 fingerprint (also printed in logs)
5. Push a new tag — subsequent builds use the stored keystore = stable signature

### Option B: Generate locally (production path)

Needs JDK installed (`apt install default-jdk` on Linux).

```bash
keytool -genkey -keystore android.keystore \
  -alias android -keyalg RSA -keysize 2048 -validity 25000 \
  -storepass <PICK-A-PASSWORD> -keypass <SAME-PASSWORD> \
  -dname "CN=Pulsar, O=Pulsar, L=Internet, C=US"

# Print SHA-256 fingerprint (needed for assetlinks.json)
keytool -list -v -keystore android.keystore -alias android -storepass <PASSWORD> | grep SHA256

# Encode for GH secret
base64 -w 0 android.keystore > keystore.b64
cat keystore.b64
```

Add the base64 + password as repo secrets, add fingerprint to assetlinks.json, push tag.

## Digital Asset Links

For the APK to load Pulsar fullscreen (without the URL bar at the top), the website must serve `/.well-known/assetlinks.json` containing the APK's signing fingerprint:

```json
[{
  "relation": ["delegate_permission/common.handle_all_urls"],
  "target": {
    "namespace": "android_app",
    "package_name": "fun.pulsarchat.app",
    "sha256_cert_fingerprints": ["YOUR:SHA256:FINGERPRINT:HERE"]
  }
}]
```

This file lives in **main pulsar repo** at `apps/web/public/.well-known/assetlinks.json`. Without it the APK still works, but shows a small URL bar — not great UX.

## Build locally

Needs Node 20+ and JDK.

```bash
cd android
npx --yes @bubblewrap/cli@latest update
npx --yes @bubblewrap/cli@latest build
```

Output: `app-release-signed.apk` and `app-release-bundle.aab` in this directory.

## Tag & release

```bash
git tag android-v0.1.0
git push origin android-v0.1.0
```
