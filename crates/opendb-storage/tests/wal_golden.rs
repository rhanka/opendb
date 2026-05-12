use opendb_storage::{
    commit_stream::{CommitRecord, Mutation},
    wal::Wal,
};

const FRAME_HEADER_LEN: usize = 16;
const FRAME_VERSION: u16 = 1;
const FRAME_RESERVED: u16 = 0;

fn decode_hex(input: &str) -> Vec<u8> {
    let hex = input
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    assert_eq!(hex.len() % 2, 0, "hex fixture must have even length");
    hex.as_bytes()
        .chunks(2)
        .map(|chunk| {
            let value = std::str::from_utf8(chunk).expect("hex utf8");
            u8::from_str_radix(value, 16).expect("hex byte")
        })
        .collect()
}

fn frame_checksum(version_reserved: &[u8], payload_len: &[u8], payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(version_reserved);
    hasher.update(payload_len);
    hasher.update(payload);
    hasher.finalize()
}

fn assert_frame(bytes: &[u8]) {
    assert_eq!(&bytes[0..4], b"ODW1");
    assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), FRAME_VERSION);
    assert_eq!(u16::from_le_bytes([bytes[6], bytes[7]]), FRAME_RESERVED);
    let payload_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    assert_eq!(FRAME_HEADER_LEN + payload_len, bytes.len());
    let expected_checksum = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    assert_eq!(
        expected_checksum,
        frame_checksum(&bytes[4..8], &bytes[8..12], &bytes[FRAME_HEADER_LEN..])
    );
}

#[tokio::test]
async fn wal_reads_frame_v1_record_v2_bootstrap_fixture() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("commit.wal");
    let bytes = decode_hex(include_str!(
        "fixtures/wal/frame-v1-record-v2-bootstrap.hex"
    ));
    assert_frame(&bytes);

    tokio::fs::write(&path, bytes)
        .await
        .expect("write wal fixture");

    let records = Wal::new(&path).read_all().await.expect("read fixture");

    assert_eq!(records, vec![CommitRecord::root_bootstrap(vec![0, 1, 2])]);
}

#[tokio::test]
async fn wal_reads_frame_v1_record_v2_range_split_fixture() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("commit.wal");
    let bytes = decode_hex(include_str!(
        "fixtures/wal/frame-v1-record-v2-range-split.hex"
    ));
    assert_frame(&bytes);

    tokio::fs::write(&path, bytes)
        .await
        .expect("write wal fixture");

    let records = Wal::new(&path).read_all().await.expect("read fixture");

    assert_eq!(records.len(), 1);
    assert!(matches!(
        records[0].mutations.as_slice(),
        [Mutation::SplitRange { .. }]
    ));
}

#[tokio::test]
async fn wal_reads_frame_v1_record_v2_typed_defaults_fixture() {
    use opendb_storage::commit_stream::{ColumnType, DefaultExpr};

    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("commit.wal");
    let bytes = decode_hex(include_str!(
        "fixtures/wal/frame-v1-record-v2-typed-defaults.hex"
    ));
    assert_frame(&bytes);

    tokio::fs::write(&path, bytes)
        .await
        .expect("write wal fixture");

    let records = Wal::new(&path).read_all().await.expect("read fixture");

    assert_eq!(records.len(), 1);
    let Mutation::CreateTable { table, columns } = &records[0].mutations[0] else {
        panic!("expected CreateTable mutation");
    };
    assert_eq!(table, "typed_events");
    assert_eq!(columns.len(), 4);
    assert_eq!(columns[3].name, "created_at");
    assert!(matches!(columns[3].data_type, ColumnType::Timestamp));
    assert!(!columns[3].nullable);
    assert!(matches!(columns[3].default, Some(DefaultExpr::Now)));
    assert!(matches!(columns[1].data_type, ColumnType::Bool));
    assert!(matches!(columns[2].data_type, ColumnType::Float64));
}
