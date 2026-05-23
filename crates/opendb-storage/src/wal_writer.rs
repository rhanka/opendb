//! Phase E.1 (2026-05-23) — single-writer task that coalesces concurrent
//! WAL append requests into one fsync per group of pending records.
//!
//! Before this layer, each `RootRange::apply_committed` call held the
//! WAL's per-path `append_lock` for the duration of its own `write_all` +
//! `sync_data`. With N concurrent clients all hitting the lock, the
//! observed cost was `N × (open + write + fsync)` — a per-record fsync
//! bottleneck of ~1.1 ms per record on the bench-concurrent c=4 baseline
//! (78 % of `wal.append_with_len`).
//!
//! Here we keep `Wal` as the underlying file primitive but funnel every
//! caller through an `mpsc::channel` consumed by exactly one writer task.
//! The writer task takes the next request, then immediately drains any
//! other requests that have piled up while it was processing the previous
//! batch (`try_recv` loop). All drained records become one
//! `append_many_with_len` call: one open, one write_all, one sync_data.
//! On success the writer broadcasts the final WAL byte length to every
//! requester's oneshot reply. On failure it broadcasts the error.
//!
//! Cost when uncontended: an extra `send` + `await reply` pair (~5 µs
//! tokio task hop) vs. a direct `Wal::append_with_len` call. fsync
//! latency dominates, so the overhead is in the noise.
//!
//! Gain when contended (c=N): the first writer takes the fsync hit; the
//! N-1 following requesters pay only the channel hop. The fsync count
//! drops from `N` per round to `1` per round.
//!
//! Semantic detail: all records in a coalesced batch share atomicity —
//! they durably commit together or not at all. That matches PG's
//! group-commit semantics. Cross-client atomicity (records A and B from
//! different clients ending up in the same fsync) is not a correctness
//! issue because each record was already independently validated by
//! `root_range.validate_semantic_append` before reaching the WAL.

use crate::commit_stream::CommitRecord;
use crate::wal::Wal;
use opendb_common::{OpenDbError, OpenDbResult};
use tokio::sync::{mpsc, oneshot};

/// Default queue depth. The writer can drain much more in a single round
/// via `try_recv`; this is just the back-pressure point at which senders
/// must wait. 1024 leaves plenty of headroom on the hot path.
const DEFAULT_QUEUE_DEPTH: usize = 1024;

#[derive(Clone, Debug)]
pub struct WalWriter {
    sender: mpsc::Sender<WalAppendRequest>,
}

struct WalAppendRequest {
    records: Vec<CommitRecord>,
    reply: oneshot::Sender<OpenDbResult<u64>>,
}

impl WalWriter {
    /// Spawn the single writer task and return a sender handle. The task
    /// runs until the last `WalWriter` clone is dropped (closing the
    /// channel).
    pub fn spawn(wal: Wal) -> Self {
        let (sender, receiver) = mpsc::channel(DEFAULT_QUEUE_DEPTH);
        tokio::spawn(writer_loop(wal, receiver));
        Self { sender }
    }

    /// Append one record. Returns the new WAL byte length after the
    /// record is durably committed.
    pub async fn append_with_len(&self, record: CommitRecord) -> OpenDbResult<u64> {
        self.append_many_with_len(vec![record]).await
    }

    /// Append a pre-batched set of records. All records in the slice
    /// will be packed into the same coalescing round and fsync.
    pub async fn append_many_with_len(&self, records: Vec<CommitRecord>) -> OpenDbResult<u64> {
        if records.is_empty() {
            // Empty submit: ask the writer for the current durable length
            // via an empty request. The writer recognizes this as a probe
            // and just returns the WAL's current byte length without an
            // fsync.
            let (reply_tx, reply_rx) = oneshot::channel();
            self.sender
                .send(WalAppendRequest {
                    records: Vec::new(),
                    reply: reply_tx,
                })
                .await
                .map_err(|_| OpenDbError::Storage("wal writer task is closed".to_owned()))?;
            return reply_rx
                .await
                .map_err(|_| OpenDbError::Storage("wal writer task dropped reply".to_owned()))?;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(WalAppendRequest {
                records,
                reply: reply_tx,
            })
            .await
            .map_err(|_| OpenDbError::Storage("wal writer task is closed".to_owned()))?;
        reply_rx
            .await
            .map_err(|_| OpenDbError::Storage("wal writer task dropped reply".to_owned()))?
    }
}

async fn writer_loop(wal: Wal, mut rx: mpsc::Receiver<WalAppendRequest>) {
    while let Some(first) = rx.recv().await {
        // Drain anything else that piled up while we were idle. Each
        // request brings its own records + reply channel.
        let mut batch: Vec<WalAppendRequest> = vec![first];
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }

        // Split out the no-op probes (empty record vectors) before we do
        // any I/O — they get answered with the current durable length
        // and do not trigger an fsync.
        let mut work: Vec<WalAppendRequest> = Vec::with_capacity(batch.len());
        let mut probes: Vec<oneshot::Sender<OpenDbResult<u64>>> = Vec::new();
        for req in batch {
            if req.records.is_empty() {
                probes.push(req.reply);
            } else {
                work.push(req);
            }
        }

        if work.is_empty() {
            // Only probes: answer each with the current durable length
            // (a cheap fs::metadata syscall). One call serves all probes
            // in this round.
            let current = wal.byte_len().await;
            for reply in probes {
                let _ = reply.send(current.clone());
            }
            continue;
        }

        // Concatenate all work records into one batch. Track the
        // per-request payload length so we can compute a per-request
        // final byte length if a caller ever cares (today every caller
        // only needs the final post-fsync length, so we just broadcast
        // it).
        let mut combined: Vec<CommitRecord> = Vec::new();
        let mut replies: Vec<oneshot::Sender<OpenDbResult<u64>>> = Vec::with_capacity(work.len());
        for req in work {
            combined.extend(req.records);
            replies.push(req.reply);
        }

        let result = wal.append_many_with_len(&combined).await;

        // Probes from this round get answered with the post-fsync length
        // (consistent with what they would have seen if they raced just
        // after).
        match result {
            Ok(new_len) => {
                for reply in replies {
                    let _ = reply.send(Ok(new_len));
                }
                for reply in probes {
                    let _ = reply.send(Ok(new_len));
                }
            }
            Err(error) => {
                for reply in replies {
                    let _ = reply.send(Err(error.clone()));
                }
                // Probes did not write anything; give them the durable
                // length we can see (or the propagated error if even
                // that read fails).
                let current = wal.byte_len().await;
                for reply in probes {
                    let _ = reply.send(current.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_stream::{ColumnValue, Mutation, Value};
    use opendb_common::{LogicalTimestamp, TransactionId};
    use std::sync::Arc;

    fn insert_record(tx: u64, ts: u64, key: &str, name: &str) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx),
            LogicalTimestamp(ts),
            vec![Mutation::InsertRow {
                table: "users".to_string(),
                key: key.to_owned(),
                values: vec![
                    ColumnValue {
                        column: "id".to_string(),
                        value: Value::Int64(key.parse().unwrap_or(0)),
                    },
                    ColumnValue {
                        column: "name".to_string(),
                        value: Value::Text(name.to_string()),
                    },
                ],
            }],
        )
    }

    #[tokio::test]
    async fn writer_handles_single_record_round_trip() {
        let temp = tempfile::tempdir().expect("temp");
        let wal = Wal::new(temp.path().join("commit.wal"));
        let writer = WalWriter::spawn(wal.clone());
        let r = insert_record(1, 10, "1", "Ada");

        let len = writer
            .append_with_len(r.clone())
            .await
            .expect("single append");
        assert!(len > 0);
        let records = wal.read_all().await.expect("read");
        assert_eq!(records, vec![r]);
    }

    #[tokio::test]
    async fn writer_coalesces_concurrent_requests_into_single_fsync() {
        let temp = tempfile::tempdir().expect("temp");
        let wal = Wal::new(temp.path().join("commit.wal"));
        let writer = Arc::new(WalWriter::spawn(wal.clone()));

        // Fire N requests at once; the writer should batch them.
        let n: u64 = 16;
        let mut handles = Vec::new();
        for i in 0..n {
            let writer = writer.clone();
            let record = insert_record(i + 1, 100 + i, &format!("{i}"), &format!("name{i}"));
            handles.push(tokio::spawn(
                async move { writer.append_with_len(record).await },
            ));
        }
        for h in handles {
            h.await.expect("join handle").expect("append succeeded");
        }
        let records = wal.read_all().await.expect("read all");
        assert_eq!(records.len(), n as usize);
    }

    #[tokio::test]
    async fn writer_empty_probe_returns_current_durable_len() {
        let temp = tempfile::tempdir().expect("temp");
        let wal = Wal::new(temp.path().join("commit.wal"));
        let writer = WalWriter::spawn(wal.clone());

        // Probe on empty WAL.
        let empty_len = writer
            .append_many_with_len(Vec::new())
            .await
            .expect("empty probe");
        assert_eq!(empty_len, 0);

        // Append something, then probe again.
        let r = insert_record(1, 10, "1", "Ada");
        let after = writer.append_with_len(r).await.expect("append");
        let probe_len = writer
            .append_many_with_len(Vec::new())
            .await
            .expect("probe after");
        assert_eq!(probe_len, after);
    }
}
