//! Miner-stored offline messages (Phase 0).
//!
//! Single SQLite file at `<app_data_dir>/offline_messages.db`. Stores
//! ciphertext blobs the central server hands us via the tunnel; serves
//! them back when their recipients query.
//!
//! All E2E content is opaque here — we never decrypt, we only route by
//! `recipient_pubkey`. Spec: pulsar/docs/MINER_STORAGE.md.

use parking_lot::Mutex;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct Storage {
    conn: Arc<Mutex<Connection>>,
}

impl Storage {
    /// Open or create the SQLite file. Initialises schema on first run.
    pub fn open(app_data_dir: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(app_data_dir).map_err(|e| e.to_string())?;
        let path: PathBuf = app_data_dir.join("offline_messages.db");
        let conn = Connection::open(&path).map_err(|e| e.to_string())?;
        // WAL mode = better concurrency, safer crash recovery.
        let _ = conn.execute_batch("PRAGMA journal_mode = WAL;");
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS offline_messages (
                id              TEXT PRIMARY KEY,
                recipient_pubkey TEXT NOT NULL,
                ciphertext      BLOB NOT NULL,
                created_at      INTEGER NOT NULL,  -- unix seconds, when WE received it
                expires_at      INTEGER NOT NULL   -- unix seconds, after which we drop it
            );
            CREATE INDEX IF NOT EXISTS idx_recipient_created
                ON offline_messages(recipient_pubkey, created_at);
            CREATE INDEX IF NOT EXISTS idx_expires
                ON offline_messages(expires_at);
            "#,
        )
        .map_err(|e| e.to_string())?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    /// Save (or replace) a ciphertext for `recipient`. Idempotent on `id`.
    /// Returns Ok(()) on insert/replace, Err on disk failure.
    pub fn store(
        &self,
        id: &str,
        recipient: &str,
        ciphertext: &[u8],
        expires_at_unix: i64,
    ) -> Result<(), String> {
        let now = current_unix();
        let conn = self.conn.lock();
        conn.execute(
            "INSERT OR REPLACE INTO offline_messages
                (id, recipient_pubkey, ciphertext, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, recipient, ciphertext, now, expires_at_unix],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Stored ciphertext + creation timestamp for one msg, by id.
    /// `None` if missing or already expired.
    pub fn get(&self, id: &str) -> Result<Option<(Vec<u8>, i64)>, String> {
        let conn = self.conn.lock();
        let row = conn
            .query_row(
                "SELECT ciphertext, created_at FROM offline_messages
                 WHERE id = ?1 AND expires_at > ?2",
                params![id, current_unix()],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?)),
            )
            .ok();
        Ok(row)
    }

    /// SHA-256 of the stored ciphertext for `id`. Used for challenge-
    /// response: server sends a random msgId, node hashes its stored
    /// blob, server compares against expected.
    pub fn proof(&self, id: &str) -> Result<Option<String>, String> {
        let blob = match self.get(id)? {
            Some((bytes, _)) => bytes,
            None => return Ok(None),
        };
        let mut h = Sha256::new();
        h.update(&blob);
        Ok(Some(format!("{:x}", h.finalize())))
    }

    /// All non-expired messages for `recipient` newer than `since_unix`.
    /// `since_unix = 0` returns everything.
    pub fn fetch(
        &self,
        recipient: &str,
        since_unix: i64,
    ) -> Result<Vec<StoredMessage>, String> {
        let now = current_unix();
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT id, ciphertext, created_at FROM offline_messages
                 WHERE recipient_pubkey = ?1
                   AND created_at > ?2
                   AND expires_at > ?3
                 ORDER BY created_at ASC",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(params![recipient, since_unix, now], |r| {
                Ok(StoredMessage {
                    id: r.get(0)?,
                    ciphertext: r.get(1)?,
                    created_at: r.get(2)?,
                })
            })
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| e.to_string())?);
        }
        Ok(out)
    }

    /// Drops everything past `expires_at`. Returns deleted count for the
    /// proof loop's stat reporting.
    pub fn prune_expired(&self) -> Result<usize, String> {
        let conn = self.conn.lock();
        let n = conn
            .execute(
                "DELETE FROM offline_messages WHERE expires_at <= ?1",
                params![current_unix()],
            )
            .map_err(|e| e.to_string())?;
        Ok(n)
    }

    /// (count, total bytes) of currently-stored non-expired messages —
    /// for periodic stat reporting up the tunnel.
    pub fn stats(&self) -> Result<(u64, u64), String> {
        let conn = self.conn.lock();
        let now = current_unix();
        conn.query_row(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(ciphertext)), 0)
             FROM offline_messages
             WHERE expires_at > ?1",
            params![now],
            |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
        )
        .map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub id: String,
    pub ciphertext: Vec<u8>,
    pub created_at: i64,
}

fn current_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
