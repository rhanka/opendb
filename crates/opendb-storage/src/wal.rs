use crate::commit_stream::CommitRecord;
use opendb_common::{OpenDbError, OpenDbResult};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, Weak};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

const WAL_MAGIC: &[u8; 4] = b"ODW1";
const WAL_FRAME_VERSION: u16 = 1;
const FRAME_HEADER_LEN: usize = 16;
const FRAME_RESERVED: u16 = 0;

#[derive(Clone, Debug)]
pub struct Wal {
    path: PathBuf,
    append_lock: Arc<Mutex<()>>,
}

impl Wal {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let append_lock = append_lock_for_path(&path);
        Self { path, append_lock }
    }

    pub async fn append(&self, record: &CommitRecord) -> OpenDbResult<()> {
        let _guard = self.append_lock.lock().await;
        let parent = containing_dir(&self.path);

        fs::create_dir_all(&parent)
            .await
            .map_err(|error| storage_error(&self.path, "create parent directory", error))?;

        let existed_before = match fs::try_exists(&self.path).await {
            Ok(exists) => exists,
            Err(error) => return Err(storage_error(&self.path, "check wal existence", error)),
        };

        let append_offset = durable_prefix_len(&self.path).await?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.path)
            .await
            .map_err(|error| storage_error(&self.path, "open wal for append", error))?;
        let frame = encode_frame(record)?;

        file.set_len(append_offset)
            .await
            .map_err(|error| storage_error(&self.path, "truncate torn wal tail", error))?;
        file.seek(std::io::SeekFrom::Start(append_offset))
            .await
            .map_err(|error| storage_error(&self.path, "seek wal for append", error))?;
        file.write_all(&frame)
            .await
            .map_err(|error| storage_error(&self.path, "write wal frame", error))?;
        file.sync_data()
            .await
            .map_err(|error| storage_error(&self.path, "sync wal file data", error))?;

        if !existed_before {
            sync_directory(&self.path, parent).await?;
        }

        Ok(())
    }

    pub async fn read_all(&self) -> OpenDbResult<Vec<CommitRecord>> {
        let bytes = match fs::read(&self.path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(storage_error(&self.path, "read wal file", error)),
        };

        Ok(decode_records(&self.path, &bytes)?.records)
    }
}

static WAL_APPEND_LOCKS: OnceLock<std::sync::Mutex<BTreeMap<PathBuf, Weak<Mutex<()>>>>> =
    OnceLock::new();

fn append_lock_for_path(path: &PathBuf) -> Arc<Mutex<()>> {
    let locks = WAL_APPEND_LOCKS.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()));
    let mut locks = locks.lock().expect("wal append lock registry poisoned");
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(path.clone(), Arc::downgrade(&lock));
    lock
}

#[derive(Debug)]
struct DecodedWal {
    records: Vec<CommitRecord>,
    durable_prefix_len: u64,
}

fn encode_frame(record: &CommitRecord) -> OpenDbResult<Vec<u8>> {
    let payload = serde_json::to_vec(record)
        .map_err(|error| OpenDbError::Storage(format!("encode wal record as json: {error}")))?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        OpenDbError::Storage(format!(
            "encode wal record frame: payload too large: {} bytes",
            payload.len()
        ))
    })?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());

    frame.extend_from_slice(WAL_MAGIC);
    frame.extend_from_slice(&WAL_FRAME_VERSION.to_le_bytes());
    frame.extend_from_slice(&FRAME_RESERVED.to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&0_u32.to_le_bytes());
    frame.extend_from_slice(&payload);
    let checksum = frame_checksum(&frame[4..8], &frame[8..12], &payload);
    frame[12..16].copy_from_slice(&checksum.to_le_bytes());

    Ok(frame)
}

fn decode_records(path: &std::path::Path, bytes: &[u8]) -> OpenDbResult<DecodedWal> {
    let mut records = Vec::new();
    let mut offset = 0;
    let mut record_index = 0;

    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < FRAME_HEADER_LEN {
            return Ok(DecodedWal {
                records,
                durable_prefix_len: offset as u64,
            });
        }

        let header = &bytes[offset..offset + FRAME_HEADER_LEN];
        if &header[0..4] != WAL_MAGIC {
            return Err(OpenDbError::Storage(format!(
                "read wal {} record {record_index}: invalid frame magic",
                path.display()
            )));
        }

        let frame_version = u16::from_le_bytes([header[4], header[5]]);
        if frame_version != WAL_FRAME_VERSION {
            return Err(OpenDbError::Storage(format!(
                "read wal {} record {record_index}: unsupported wal frame version {frame_version}, expected {WAL_FRAME_VERSION}",
                path.display()
            )));
        }

        let reserved = u16::from_le_bytes([header[6], header[7]]);
        if reserved != FRAME_RESERVED {
            return Err(OpenDbError::Storage(format!(
                "read wal {} record {record_index}: invalid reserved header value {reserved}, expected {FRAME_RESERVED}",
                path.display()
            )));
        }

        let payload_len =
            u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let frame_len = FRAME_HEADER_LEN + payload_len;
        let expected_checksum =
            u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let available_payload = if remaining > FRAME_HEADER_LEN {
            &bytes[offset + FRAME_HEADER_LEN..]
        } else {
            &[]
        };
        if remaining < frame_len {
            if serde_json::from_slice::<CommitRecord>(available_payload).is_ok() {
                let actual_checksum =
                    frame_checksum(&header[4..8], &header[8..12], available_payload);
                return Err(OpenDbError::Storage(format!(
                    "read wal {} record {record_index}: checksum mismatch: expected {expected_checksum}, got {actual_checksum}",
                    path.display()
                )));
            }
            return Ok(DecodedWal {
                records,
                durable_prefix_len: offset as u64,
            });
        }

        let payload_start = offset + FRAME_HEADER_LEN;
        let payload_end = payload_start + payload_len;
        let payload = &bytes[payload_start..payload_end];
        let actual_checksum = frame_checksum(&header[4..8], &header[8..12], payload);
        if actual_checksum != expected_checksum {
            return Err(OpenDbError::Storage(format!(
                "read wal {} record {record_index}: checksum mismatch: expected {expected_checksum}, got {actual_checksum}",
                path.display()
            )));
        }

        records.push(decode_frame(path, payload, record_index)?);
        offset += frame_len;
        record_index += 1;
    }

    Ok(DecodedWal {
        records,
        durable_prefix_len: offset as u64,
    })
}

async fn durable_prefix_len(path: &std::path::Path) -> OpenDbResult<u64> {
    let bytes = match fs::read(path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(storage_error(path, "read wal before append", error)),
    };

    Ok(decode_records(path, &bytes)?.durable_prefix_len)
}

fn frame_checksum(version_reserved: &[u8], payload_len: &[u8], payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(version_reserved);
    hasher.update(payload_len);
    hasher.update(payload);
    hasher.finalize()
}

fn decode_frame(
    path: &std::path::Path,
    payload: &[u8],
    record_index: usize,
) -> OpenDbResult<CommitRecord> {
    let raw_record: serde_json::Value = serde_json::from_slice(payload).map_err(|error| {
        OpenDbError::Storage(format!(
            "read wal {} record {record_index}: decode json: {error}",
            path.display()
        ))
    })?;
    let record_version = raw_record
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            OpenDbError::Storage(format!(
                "read wal {} record {record_index}: missing commit record version",
                path.display()
            ))
        })?;

    if record_version != u64::from(CommitRecord::VERSION) {
        return Err(OpenDbError::Storage(format!(
            "read wal {} record {record_index}: unsupported commit record version {}, expected {}",
            path.display(),
            record_version,
            CommitRecord::VERSION
        )));
    }

    let record: CommitRecord = serde_json::from_value(raw_record).map_err(|error| {
        OpenDbError::Storage(format!(
            "read wal {} record {record_index}: decode commit record version {}: {error}",
            path.display(),
            CommitRecord::VERSION
        ))
    })?;

    Ok(record)
}

fn containing_dir(path: &std::path::Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

async fn sync_directory(wal_path: &std::path::Path, dir: PathBuf) -> OpenDbResult<()> {
    let wal_path = wal_path.to_path_buf();
    let task_wal_path = wal_path.clone();
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(&dir)
            .and_then(|file| file.sync_all())
            .map_err(|error| {
                storage_error(
                    &task_wal_path,
                    &format!("sync containing directory {}", dir.display()),
                    error,
                )
            })
    })
    .await
    .map_err(|error| {
        OpenDbError::Storage(format!(
            "sync wal {} containing directory task: {error}",
            wal_path.display()
        ))
    })?
}

fn storage_error(
    path: &std::path::Path,
    operation: &str,
    error: impl std::fmt::Display,
) -> OpenDbError {
    OpenDbError::Storage(format!("{} for {}: {error}", operation, path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive_manifest::{ArchiveBackendKind, ArchiveObjectPointer};
    use crate::commit_stream::{
        ColumnDefinition, ColumnType, ColumnValue, CommitRecord, Mutation, Value,
    };
    use crate::range_catalog::RangeDescriptor;
    use opendb_common::{LogicalTimestamp, RangeId, TransactionId};

    #[tokio::test]
    async fn wal_appends_and_reads_records_in_order() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal = Wal::new(temp_dir.path().join("nested").join("commit.log"));

        assert_eq!(wal.read_all().await.expect("read missing wal"), Vec::new());

        let first = CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(10),
            vec![Mutation::CreateTable {
                table: "users".to_string(),
                columns: users_columns(),
            }],
        );
        let second = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(11),
            vec![Mutation::InsertRow {
                table: "users".to_string(),
                key: "1".to_owned(),
                values: vec![
                    ColumnValue {
                        column: "id".to_string(),
                        value: Value::Int64(1),
                    },
                    ColumnValue {
                        column: "name".to_string(),
                        value: Value::Text("Ada".to_string()),
                    },
                ],
            }],
        );

        wal.append(&first).await.expect("append first record");
        wal.append(&second).await.expect("append second record");

        assert_eq!(
            wal.read_all().await.expect("read appended records"),
            vec![first, second]
        );
    }

    #[tokio::test]
    async fn wal_appends_and_reads_range_descriptor_record() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
        let record = CommitRecord::new(
            TransactionId(3),
            LogicalTimestamp(12),
            vec![Mutation::PutRangeDescriptor {
                descriptor: RangeDescriptor {
                    range_id: RangeId::ROOT,
                    parent_range_id: None,
                    key_start: None,
                    key_end: None,
                    replica_node_ids: vec![0, 1, 2],
                },
            }],
        );

        wal.append(&record)
            .await
            .expect("append range descriptor record");

        assert_eq!(
            wal.read_all().await.expect("read range descriptor record"),
            vec![record]
        );
    }

    #[tokio::test]
    async fn wal_appends_and_reads_archive_object_pointer_record() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
        let record = CommitRecord::new(
            TransactionId(4),
            LogicalTimestamp(13),
            vec![Mutation::PutArchiveObjectPointer {
                pointer: ArchiveObjectPointer {
                    backend: ArchiveBackendKind::AzureBlobCompatible,
                    bucket: "opendb-archives".to_owned(),
                    key: "root-range/00000003.wal".to_owned(),
                    content_sha256:
                        "1111111111111111111111111111111111111111111111111111111111111111"
                            .to_owned(),
                },
            }],
        );

        wal.append(&record)
            .await
            .expect("append archive object pointer record");

        assert_eq!(
            wal.read_all()
                .await
                .expect("read archive object pointer record"),
            vec![record]
        );
    }

    #[test]
    fn wal_frame_has_stable_shape_for_known_record() {
        let record = insert_record(1, 10, "1", "Ada");
        let frame = encode_frame(&record).expect("encode frame");

        assert_eq!(&frame[0..4], WAL_MAGIC);
        assert_eq!(u16::from_le_bytes([frame[4], frame[5]]), WAL_FRAME_VERSION);

        let payload_len = u32::from_le_bytes([frame[8], frame[9], frame[10], frame[11]]) as usize;
        assert_eq!(payload_len, frame.len() - FRAME_HEADER_LEN);

        let checksum = u32::from_le_bytes([frame[12], frame[13], frame[14], frame[15]]);
        let payload = &frame[FRAME_HEADER_LEN..];
        assert_eq!(
            checksum,
            frame_checksum(&frame[4..8], &frame[8..12], payload)
        );
        assert_eq!(
            std::str::from_utf8(payload).expect("utf8 payload"),
            r#"{"version":2,"tx_id":1,"range_id":1,"ts":10,"actor":"system","mutations":[{"InsertRow":{"table":"users","key":"1","values":[{"column":"id","value":{"Int64":1}},{"column":"name","value":{"Text":"Ada"}}]}}]}"#
        );
        assert_eq!(
            decode_frame(std::path::Path::new("golden.wal"), payload, 0).expect("decode payload"),
            record
        );
    }

    #[test]
    fn wal_frame_has_stable_shape_for_archive_pointer_record() {
        let record = CommitRecord::new(
            TransactionId(4),
            LogicalTimestamp(13),
            vec![Mutation::PutArchiveObjectPointer {
                pointer: ArchiveObjectPointer {
                    backend: ArchiveBackendKind::S3Compatible,
                    bucket: "opendb-archives".to_owned(),
                    key: "root-range/00000003.wal".to_owned(),
                    content_sha256:
                        "1111111111111111111111111111111111111111111111111111111111111111"
                            .to_owned(),
                },
            }],
        );
        let frame = encode_frame(&record).expect("encode frame");
        let payload = &frame[FRAME_HEADER_LEN..];

        assert_eq!(
            std::str::from_utf8(payload).expect("utf8 payload"),
            r#"{"version":2,"tx_id":4,"range_id":1,"ts":13,"actor":"system","mutations":[{"PutArchiveObjectPointer":{"pointer":{"backend":"s3_compatible","bucket":"opendb-archives","key":"root-range/00000003.wal","content_sha256":"1111111111111111111111111111111111111111111111111111111111111111"}}}]}"#
        );
        assert_eq!(
            decode_frame(std::path::Path::new("archive-pointer.wal"), payload, 0)
                .expect("decode payload"),
            record
        );
    }

    #[tokio::test]
    async fn wal_ignores_torn_final_frame() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = temp_dir.path().join("root-range.wal");
        let wal = Wal::new(&wal_path);
        let first = insert_record(1, 10, "1", "Ada");
        let second = insert_record(2, 11, "2", "Grace");

        wal.append(&first).await.expect("append first");
        let mut torn = encode_frame(&second).expect("encode second");
        torn.truncate(torn.len() - 5);

        let mut file = OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .await
            .expect("open wal for torn append");
        file.write_all(&torn).await.expect("append torn frame");
        file.sync_data().await.expect("sync torn frame");

        assert_eq!(
            wal.read_all().await.expect("read with torn tail"),
            vec![first]
        );
    }

    #[tokio::test]
    async fn append_truncates_torn_final_frame_before_writing_new_record() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = temp_dir.path().join("root-range.wal");
        let wal = Wal::new(&wal_path);
        let first = insert_record(1, 10, "1", "Ada");
        let torn_record = insert_record(2, 11, "2", "Grace");
        let replacement = insert_record(3, 12, "3", "Katherine");

        wal.append(&first).await.expect("append first");
        let mut torn = encode_frame(&torn_record).expect("encode torn");
        torn.truncate(torn.len() - 5);

        let mut file = OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .await
            .expect("open wal for torn append");
        file.write_all(&torn).await.expect("append torn frame");
        file.sync_data().await.expect("sync torn frame");

        wal.append(&replacement)
            .await
            .expect("append replacement after torn tail");

        assert_eq!(
            wal.read_all()
                .await
                .expect("read after truncating torn tail"),
            vec![first, replacement]
        );
    }

    #[tokio::test]
    async fn wal_rejects_corrupt_complete_final_frame_header() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = temp_dir.path().join("root-range.wal");
        let wal = Wal::new(&wal_path);
        let first = insert_record(1, 10, "1", "Ada");
        let second = insert_record(2, 11, "2", "Grace");

        let mut bytes = encode_frame(&first).expect("encode first");
        let second_offset = bytes.len();
        bytes.extend_from_slice(&encode_frame(&second).expect("encode second"));
        let length_offset = second_offset + 8;
        let corrupt_len = u32::MAX.to_le_bytes();
        bytes[length_offset..length_offset + corrupt_len.len()].copy_from_slice(&corrupt_len);

        fs::write(&wal_path, bytes)
            .await
            .expect("write corrupt complete final frame");

        let error = wal
            .read_all()
            .await
            .expect_err("reject corrupt complete final frame header");
        assert!(error.to_string().contains("record 1"));
        assert!(error.to_string().contains("checksum mismatch"));
        assert!(
            error
                .to_string()
                .contains(wal_path.to_str().expect("utf8 path"))
        );
    }

    #[tokio::test]
    async fn wal_rejects_nonzero_reserved_header() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = temp_dir.path().join("root-range.wal");
        let wal = Wal::new(&wal_path);
        let mut frame = encode_frame(&insert_record(1, 10, "1", "Ada")).expect("encode frame");
        frame[6] = 1;

        fs::write(&wal_path, frame)
            .await
            .expect("write reserved corruption");

        let error = wal.read_all().await.expect_err("reject reserved field");
        assert!(error.to_string().contains("record 0"));
        assert!(error.to_string().contains("invalid reserved header value"));
    }

    #[tokio::test]
    async fn wal_rejects_unknown_commit_record_version() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = temp_dir.path().join("root-range.wal");
        let wal = Wal::new(&wal_path);
        let mut record = insert_record(1, 10, "1", "Ada");
        record.version = CommitRecord::VERSION + 1;

        fs::write(
            &wal_path,
            encode_frame(&record).expect("encode invalid version"),
        )
        .await
        .expect("write invalid version");

        let error = wal.read_all().await.expect_err("reject invalid version");
        assert!(error.to_string().contains("record 0"));
        assert!(
            error
                .to_string()
                .contains("unsupported commit record version")
        );
        assert!(
            error
                .to_string()
                .contains(wal_path.to_str().expect("utf8 path"))
        );
    }

    #[tokio::test]
    async fn wal_rejects_legacy_v1_create_table_shape_with_version_error() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = temp_dir.path().join("root-range.wal");
        let wal = Wal::new(&wal_path);
        let legacy_payload = br#"{"version":1,"tx_id":1,"range_id":1,"ts":10,"actor":"system","mutations":[{"CreateTable":{"table":"users","columns":["id","name"]}}]}"#;

        fs::write(&wal_path, encode_raw_payload_frame(legacy_payload))
            .await
            .expect("write legacy wal frame");

        let error = wal
            .read_all()
            .await
            .expect_err("reject legacy v1 create table shape");
        assert!(error.to_string().contains("record 0"));
        assert!(
            error
                .to_string()
                .contains("unsupported commit record version 1")
        );
        assert!(
            error
                .to_string()
                .contains(wal_path.to_str().expect("utf8 path"))
        );
    }

    #[tokio::test]
    async fn cloned_wal_instances_serialize_appends() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal = Wal::new(temp_dir.path().join("root-range.wal"));
        let left = wal.clone();
        let right = wal.clone();
        let first = insert_record(1, 10, "1", "Ada");
        let second = insert_record(2, 11, "2", "Grace");
        let first_for_task = first.clone();
        let second_for_task = second.clone();

        let first_append = tokio::spawn(async move { left.append(&first_for_task).await });
        let second_append = tokio::spawn(async move { right.append(&second_for_task).await });
        first_append
            .await
            .expect("first task")
            .expect("first append");
        second_append
            .await
            .expect("second task")
            .expect("second append");

        let records = wal.read_all().await.expect("read records");
        assert_eq!(records.len(), 2);
        assert!(records.contains(&first));
        assert!(records.contains(&second));
    }

    #[tokio::test]
    async fn independent_wal_instances_serialize_appends_to_same_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = temp_dir.path().join("root-range.wal");
        let left = Wal::new(&wal_path);
        let right = Wal::new(&wal_path);
        let reader = Wal::new(&wal_path);
        let first = insert_record(1, 10, "1", "Ada");
        let second = insert_record(2, 11, "2", "Grace");
        let first_for_task = first.clone();
        let second_for_task = second.clone();

        let first_append = tokio::spawn(async move { left.append(&first_for_task).await });
        let second_append = tokio::spawn(async move { right.append(&second_for_task).await });
        first_append
            .await
            .expect("first task")
            .expect("first append");
        second_append
            .await
            .expect("second task")
            .expect("second append");

        let records = reader.read_all().await.expect("read records");
        assert_eq!(records.len(), 2);
        assert!(records.contains(&first));
        assert!(records.contains(&second));
    }

    fn insert_record(tx_id: u64, ts: u64, key: &str, name: &str) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(ts),
            vec![Mutation::InsertRow {
                table: "users".to_string(),
                key: key.to_owned(),
                values: vec![
                    ColumnValue {
                        column: "id".to_string(),
                        value: Value::Int64(key.parse().expect("integer key")),
                    },
                    ColumnValue {
                        column: "name".to_string(),
                        value: Value::Text(name.to_string()),
                    },
                ],
            }],
        )
    }

    fn users_columns() -> Vec<ColumnDefinition> {
        vec![
            ColumnDefinition::primary_key("id", ColumnType::Int64),
            ColumnDefinition::new("name", ColumnType::Text),
        ]
    }

    fn encode_raw_payload_frame(payload: &[u8]) -> Vec<u8> {
        let payload_len = u32::try_from(payload.len())
            .expect("legacy payload should fit wal frame")
            .to_le_bytes();
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
        frame.extend_from_slice(WAL_MAGIC);
        frame.extend_from_slice(&WAL_FRAME_VERSION.to_le_bytes());
        frame.extend_from_slice(&FRAME_RESERVED.to_le_bytes());
        frame.extend_from_slice(&payload_len);
        frame.extend_from_slice(&0_u32.to_le_bytes());
        frame.extend_from_slice(payload);
        let checksum = frame_checksum(&frame[4..8], &frame[8..12], payload);
        frame[12..16].copy_from_slice(&checksum.to_le_bytes());
        frame
    }
}
