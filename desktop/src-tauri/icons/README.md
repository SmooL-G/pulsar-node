# Icons

`source.png` is the canonical 1024×1024 icon. CI generates all platform-specific variants from it before each Tauri build:

```sh
npx @tauri-apps/cli icon ./src-tauri/icons/source.png
```

That populates `icon.png`, `icon.ico`, `icon.icns`, `32x32.png`, `128x128.png`, `128x128@2x.png`, plus the Microsoft Store sizes. Generated files are gitignored — only `source.png` (and this README) are committed.

To replace the icon: drop a new 1024×1024 PNG at `source.png`, commit, and the next CI build picks it up.
