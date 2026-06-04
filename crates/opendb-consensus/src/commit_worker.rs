//! Phase E.2 (2026-05-24) — commit worker task with cross-client
//! coalescing of validate + WAL fsync + snapshot commit.
//!
//! Before this layer the `RootRange::semantic_append_lock` serialized
//! the entire `validate → wal_writer.append → commit_snapshot` triple,
//! so client B was blocked at the lock for the duration of client A's
//! round (validate ≈1.1 ms + fsync ≈1 ms + commit ≈0.4 ms ≈ 2.5 ms per
//! record at c=4). The `WalWriter` task from E.1 already coalesced
//! fsyncs **within** a `validate + commit` round but could not do
//! anything across rounds because the semantic lock was held across
//! await on its reply.
//!
//! E.2 spawns a single tokio task that owns access to the semantic
//! state. Client requests arrive via an `mpsc::channel` and pile up
//! while the worker is busy. After each batch the worker:
//!
//! 1. Drains every queued request (`try_recv` loop).
//! 2. For each request, clones the working snapshot, applies the
//!    request's records, and on success advances the working snapshot
//!    and adds the records to the batch (on failure the request gets
//!    an `Err` reply and the working snapshot is unchanged).
//! 3. Calls `WalWriter::append_many_with_len` once with the full
//!    batch — a single fsync covers all of it.
//! 4. Commits the final working snapshot back to the
//!    `semantic_append_cache`.
//! 5. Sends `Ok(wal_len)` to every successful request's oneshot.
//!
//! Semantic: each request is still **per-request atomic** — the
//! request fails as a unit if any of its records fails validation.
//! Cross-request batching only commits **the successful prefix of
//! each request**, so a request whose validation passes does not get
//! penalised by a sibling request failing later in the same round.
//!
//! When uncontended (single-request rounds), the per-request cost is
//! a `send` + `await reply` hop (~5 µs) on top of the existing
//! validate + fsync path — same as E.1's `WalWriter` overhead.

use crate::root_range::RootRange;
use opendb_common::{OpenDbError, OpenDbResult};
use opendb_storage::commit_stream::CommitRecord;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

const DEFAULT_QUEUE_DEPTH: usize = 1024;

#[derive(Clone, Debug)]
pub struct CommitWorker {
    sender: mpsc::Sender<CommitRequest>,
}

struct CommitRequest {
    records: Vec<CommitRecord>,
    reply: oneshot::Sender<OpenDbResult<u64>>,
}

impl CommitWorker {
    /// Spawn the commit worker task bound to `root_range`. Keeps an
    /// `Arc<RootRange>` alive in the task; dropping every `CommitWorker`
    /// clone closes the channel and the task exits.
    pub fn spawn(root_range: Arc<RootRange>) -> Self {
        let (sender, receiver) = mpsc::channel(DEFAULT_QUEUE_DEPTH);
        tokio::spawn(worker_loop(root_range, receiver));
        Self { sender }
    }

    /// Submit a batch of records to the worker. Returns the WAL byte
    /// length after the records are durably committed. The batch is
    /// atomic: every record in `records` either commits together or
    /// the call returns `Err`. Multiple `commit()` calls from
    /// concurrent callers coalesce into one fsync per worker round.
    pub async fn commit(&self, records: Vec<CommitRecord>) -> OpenDbResult<u64> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(CommitRequest {
                records,
                reply: reply_tx,
            })
            .await
            .map_err(|_| OpenDbError::Storage("commit worker task is closed".to_owned()))?;
        reply_rx
            .await
            .map_err(|_| OpenDbError::Storage("commit worker task dropped reply".to_owned()))?
    }
}

async fn worker_loop(root_range: Arc<RootRange>, mut rx: mpsc::Receiver<CommitRequest>) {
    while let Some(first) = rx.recv().await {
        let mut batch: Vec<CommitRequest> = vec![first];
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }

        // Apply-record pre-check (cheap version validation) and slice
        // out probes (empty records) before doing any I/O.
        let mut work: Vec<CommitRequest> = Vec::with_capacity(batch.len());
        let mut probes: Vec<oneshot::Sender<OpenDbResult<u64>>> = Vec::new();
        for req in batch {
            if req.records.is_empty() {
                probes.push(req.reply);
                continue;
            }
            if let Some(err) = req
                .records
                .iter()
                .find_map(|r| root_range.validate_apply_record(r).err())
            {
                let _ = req.reply.send(Err(err));
                continue;
            }
            work.push(req);
        }

        if work.is_empty() {
            let current = root_range.wal.byte_len().await;
            for reply in probes {
                let _ = reply.send(current.clone());
            }
            continue;
        }

        // Fetch the working snapshot once for the whole round.
        let mut snapshot = match root_range.semantic_snapshot_for_append().await {
            Ok(s) => s,
            Err(e) => {
                for req in work {
                    let _ = req.reply.send(Err(e.clone()));
                }
                for reply in probes {
                    let _ = reply.send(Err(e.clone()));
                }
                continue;
            }
        };

        // For each request, validate atomically: clone snapshot,
        // apply records, on success advance the working snapshot.
        let mut combined_records: Vec<CommitRecord> = Vec::new();
        let mut successful_replies: Vec<oneshot::Sender<OpenDbResult<u64>>> =
            Vec::with_capacity(work.len());
        for req in work {
            let mut candidate = snapshot.clone();
            let mut failed: Option<OpenDbError> = None;
            for record in &req.records {
                if let Err(e) = root_range.apply_record_to_semantic_snapshot(&mut candidate, record)
                {
                    failed = Some(e);
                    break;
                }
            }
            match failed {
                None => {
                    snapshot = candidate;
                    combined_records.extend(req.records);
                    successful_replies.push(req.reply);
                }
                Some(e) => {
                    let _ = req.reply.send(Err(e));
                }
            }
        }

        if combined_records.is_empty() {
            // Every request in `work` failed validation; probes still
            // get the current durable length.
            let current = root_range.wal.byte_len().await;
            for reply in probes {
                let _ = reply.send(current.clone());
            }
            continue;
        }

        let append_result = root_range
            .wal_writer
            .append_many_with_len(combined_records)
            .await;

        match append_result {
            Ok(wal_len) => {
                root_range
                    .commit_semantic_append_snapshot(snapshot, wal_len)
                    .await;
                for reply in successful_replies {
                    let _ = reply.send(Ok(wal_len));
                }
                for reply in probes {
                    let _ = reply.send(Ok(wal_len));
                }
            }
            Err(e) => {
                for reply in successful_replies {
                    let _ = reply.send(Err(e.clone()));
                }
                let current = root_range.wal.byte_len().await;
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
    use crate::root_range::RootRange;
    use opendb_common::{LogicalTimestamp, TransactionId};
    use opendb_storage::commit_stream::{
        ColumnDefinition, ColumnType, ColumnValue, CommitRecord, Mutation, Value,
    };

    fn user_table_record() -> CommitRecord {
        CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(11),
            vec![Mutation::CreateTable {
                table: "users".to_owned(),
                columns: vec![
                    ColumnDefinition::primary_key("id", ColumnType::Int64),
                    ColumnDefinition::new("name", ColumnType::Text),
                ],
            }],
        )
    }

    fn user_row(tx: u64, ts: u64, id: i64, name: &str) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx),
            LogicalTimestamp(ts),
            vec![Mutation::InsertRow {
                table: "users".to_owned(),
                key: id.to_string(),
                values: vec![
                    ColumnValue {
                        column: "id".to_owned(),
                        value: Value::Int64(id),
                    },
                    ColumnValue {
                        column: "name".to_owned(),
                        value: Value::Text(name.to_owned()),
                    },
                ],
            }],
        )
    }

    #[tokio::test]
    async fn commit_worker_single_request_round_trip() {
        let temp = tempfile::tempdir().expect("temp");
        let root_range = RootRange::new(temp.path());
        root_range.ensure_bootstrapped().await.expect("bootstrap");
        let worker = CommitWorker::spawn(root_range.clone());

        worker
            .commit(vec![user_table_record()])
            .await
            .expect("create table");
        worker
            .commit(vec![user_row(3, 12, 1, "Ada")])
            .await
            .expect("insert");

        let records = root_range.replay().await.expect("replay");
        // Bootstrap + CreateTable + InsertRow.
        assert_eq!(records.len(), 3);
    }

    #[tokio::test]
    async fn commit_worker_coalesces_concurrent_requests() {
        let temp = tempfile::tempdir().expect("temp");
        let root_range = RootRange::new(temp.path());
        root_range.ensure_bootstrapped().await.expect("bootstrap");
        let worker = Arc::new(CommitWorker::spawn(root_range.clone()));

        worker
            .commit(vec![user_table_record()])
            .await
            .expect("create table");

        // Fire N concurrent commits with disjoint PKs.
        let n: u64 = 16;
        let mut handles = Vec::new();
        for i in 0..n {
            let w = worker.clone();
            handles.push(tokio::spawn(async move {
                w.commit(vec![user_row(
                    10 + i,
                    100 + i,
                    (i + 100) as i64,
                    &format!("u{i}"),
                )])
                .await
            }));
        }
        for h in handles {
            h.await.expect("join").expect("commit succeeded");
        }

        let records = root_range.replay().await.expect("replay");
        // Bootstrap + CreateTable + N inserts.
        assert_eq!(records.len() as u64, 2 + n);
    }

    #[tokio::test]
    async fn commit_worker_per_request_atomicity_isolates_failure() {
        let temp = tempfile::tempdir().expect("temp");
        let root_range = RootRange::new(temp.path());
        root_range.ensure_bootstrapped().await.expect("bootstrap");
        let worker = CommitWorker::spawn(root_range.clone());

        worker
            .commit(vec![user_table_record()])
            .await
            .expect("create table");
        worker
            .commit(vec![user_row(3, 12, 1, "Ada")])
            .await
            .expect("first insert");

        // Re-inserting the same PK must fail without disturbing the
        // already-committed row.
        let dup = user_row(4, 13, 1, "Grace");
        let dup_err = worker.commit(vec![dup]).await;
        assert!(dup_err.is_err(), "duplicate PK must fail");

        // A fresh disjoint insert after the failure still succeeds.
        let fresh = user_row(5, 14, 2, "Hopper");
        worker.commit(vec![fresh]).await.expect("fresh insert");

        let records = root_range.replay().await.expect("replay");
        // Bootstrap + CreateTable + first insert + fresh insert.
        assert_eq!(records.len(), 4);
    }
}
