#!/usr/bin/env python3
"""
Convert an unencrypted minisign secret key (kdf_alg = "\\0\\0") into an
encrypted-with-empty-password one (kdf_alg = "Sc"), preserving the same
ed25519 keypair so the public key on disk stays valid.

Why: tauri-plugin-updater always reads private keys via the encrypted
path (with optional password). An unencrypted key fails with
"Key is not encrypted" / "Wrong password for that key".

Reads the existing key from C:\\Users\\<you>\\.tauri-pulsar\\pulsar-desktop.key
and writes it back over the same file with the encrypted format.
The .pub file is unchanged.
"""
import base64
import hashlib
import os
import sys

# minisign defaults (from rust-minisign/src/constants.rs)
SIG_ALG = b"Ed"
KDF_ALG = b"Sc"
CHK_ALG = b"B2"
OPSLIMIT = 1_048_576       # 1 << 20
MEMLIMIT = 33_554_432      # 32 MiB

# scrypt params derived from raw_scrypt_params(MEMLIMIT, OPSLIMIT, N_LOG2_MAX=20)
# walking the Rust algorithm by hand: N=2^15=32768, r=8, p=1, dklen=104.
SCRYPT_N = 32_768
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_DKLEN = 104  # KEYNUM(8) + SK(64) + CHK(32)


def parse_unencrypted(path):
    text = open(path, encoding="utf-8").read()
    b64 = text.strip().split("\n")[1]
    blob = base64.b64decode(b64)
    assert len(blob) == 158, f"unexpected blob length: {len(blob)}"
    sig_alg = blob[0:2]
    kdf_alg = blob[2:4]
    chk_alg = blob[4:6]
    assert sig_alg == SIG_ALG, sig_alg
    assert chk_alg == CHK_ALG, chk_alg
    if kdf_alg != b"\x00\x00":
        raise SystemExit(
            "Key is not in unencrypted format (kdf_alg != \\0\\0). "
            "It might already be encrypted; nothing to do."
        )
    keynum = blob[54:62]
    sk = blob[62:126]
    return keynum, sk


def main():
    home = os.path.expanduser("~")
    sk_path = os.path.join(home, ".tauri-pulsar", "pulsar-desktop.key")
    if len(sys.argv) > 1:
        sk_path = sys.argv[1]

    keynum, sk = parse_unencrypted(sk_path)

    # Compute checksum over plain (sig_alg || keynum || sk) — this is what
    # the *encrypted* file will checksum against after decryption.
    chk = hashlib.blake2b(digest_size=32)
    chk.update(SIG_ALG)
    chk.update(keynum)
    chk.update(sk)
    chk_plain = chk.digest()

    # Random salt
    kdf_salt = os.urandom(32)

    # Derive 104-byte stream with scrypt(password=b"", salt, N, r, p)
    # hashlib.scrypt: maxmem must be >= 128*N*r (default is 32 MB).
    stream = hashlib.scrypt(
        b"",
        salt=kdf_salt,
        n=SCRYPT_N,
        r=SCRYPT_R,
        p=SCRYPT_P,
        dklen=SCRYPT_DKLEN,
        maxmem=128 * SCRYPT_N * SCRYPT_R * 2,
    )

    # XOR (keynum || sk || chk_plain) with the stream
    plain = keynum + sk + chk_plain
    encrypted = bytes(p ^ s for p, s in zip(plain, stream))
    enc_keynum = encrypted[:8]
    enc_sk = encrypted[8:8 + 64]
    enc_chk = encrypted[8 + 64:]

    # Build new 158-byte blob
    blob = (
        SIG_ALG
        + KDF_ALG
        + CHK_ALG
        + kdf_salt
        + OPSLIMIT.to_bytes(8, "little")
        + MEMLIMIT.to_bytes(8, "little")
        + enc_keynum
        + enc_sk
        + enc_chk
    )
    assert len(blob) == 158

    text = (
        "untrusted comment: minisign encrypted secret key\n"
        + base64.b64encode(blob).decode()
        + "\n"
    )

    with open(sk_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)

    print(f"re-encrypted in place: {sk_path}")
    print()
    print("paste this single base64 line into the GH secret TAURI_SIGNING_PRIVATE_KEY:")
    print()
    print(base64.b64encode(blob).decode())


if __name__ == "__main__":
    main()
