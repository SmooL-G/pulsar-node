#!/usr/bin/env python3
"""
Generate a Tauri-compatible minisign signing keypair (unencrypted).

Replicates `tauri signer generate --no-password` output byte-for-byte.
Format reference: https://github.com/jedisct1/rust-minisign/blob/master/src/secretkey.rs

Outputs:
  pulsar-desktop.key      — secret key (paste into TAURI_SIGNING_PRIVATE_KEY secret)
  pulsar-desktop.key.pub  — public key (paste into tauri.conf.json plugins.updater.pubkey)
"""
import base64
import hashlib
import os
import sys

from nacl.signing import SigningKey

SIG_ALG = b"Ed"          # ed25519
KDF_NONE = b"\x00\x00"   # unencrypted
CHK_ALG = b"B2"          # blake2b


def gen_keypair():
    keynum = os.urandom(8)
    seed = os.urandom(32)
    sk_obj = SigningKey(seed)
    pk = sk_obj.verify_key.encode()              # 32 bytes
    sk_full = seed + pk                          # 64 bytes (libsodium format)

    chk = hashlib.blake2b(digest_size=32)
    chk.update(SIG_ALG)
    chk.update(keynum)
    chk.update(sk_full)
    checksum = chk.digest()                      # 32 bytes

    # Secret key blob: 2+2+2+32+8+8+8+64+32 = 158 bytes
    sk_blob = (
        SIG_ALG
        + KDF_NONE
        + CHK_ALG
        + b"\x00" * 32          # kdf_salt (zeros for unencrypted)
        + b"\x00" * 8           # kdf_opslimit_le (zeros)
        + b"\x00" * 8           # kdf_memlimit_le (zeros)
        + keynum
        + sk_full
        + checksum
    )
    assert len(sk_blob) == 158, len(sk_blob)

    # Public key blob: 2+8+32 = 42 bytes
    pk_blob = SIG_ALG + keynum + pk
    assert len(pk_blob) == 42, len(pk_blob)

    keynum_hex = format(int.from_bytes(keynum, "little"), "X")

    sk_text = (
        "untrusted comment: minisign encrypted secret key\n"
        + base64.b64encode(sk_blob).decode()
        + "\n"
    )
    pk_text = (
        f"untrusted comment: minisign public key {keynum_hex}\n"
        + base64.b64encode(pk_blob).decode()
        + "\n"
    )
    return sk_text, pk_text, base64.b64encode(pk_blob).decode()


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    os.makedirs(out_dir, exist_ok=True)
    sk_text, pk_text, pk_b64 = gen_keypair()

    sk_path = os.path.join(out_dir, "pulsar-desktop.key")
    pk_path = os.path.join(out_dir, "pulsar-desktop.key.pub")

    with open(sk_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(sk_text)
    with open(pk_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(pk_text)

    print(f"wrote {sk_path}")
    print(f"wrote {pk_path}")
    print()
    print("public key (base64 — paste into tauri.conf.json):")
    print(pk_b64)


if __name__ == "__main__":
    main()
