use opendb_storage::{commit_stream::CommitRecord, wal::Wal};

const FRAME_HEADER_LEN: usize = 16;
const FRAME_VERSION: u16 = 1;
const FRAME_RESERVED: u16 = 0;
const FIXTURE_LEN: usize = 225;
const FIXTURE_PAYLOAD_LEN: usize = 209;

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

#[tokio::test]
async fn wal_reads_frame_v1_record_v2_bootstrap_fixture() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("commit.wal");
    let bytes = decode_hex(include_str!(
        "fixtures/wal/frame-v1-record-v2-bootstrap.hex"
    ));
    assert_eq!(
        bytes.len(),
        FIXTURE_LEN,
        "fixture must be one complete frame"
    );
    assert_eq!(&bytes[0..4], b"ODW1");
    assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), FRAME_VERSION);
    assert_eq!(u16::from_le_bytes([bytes[6], bytes[7]]), FRAME_RESERVED);

    let payload_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    assert_eq!(payload_len, FIXTURE_PAYLOAD_LEN);
    assert_eq!(FRAME_HEADER_LEN + payload_len, bytes.len());

    let expected_checksum = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    let payload = &bytes[FRAME_HEADER_LEN..];
    assert_eq!(
        expected_checksum,
        frame_checksum(&bytes[4..8], &bytes[8..12], payload)
    );

    tokio::fs::write(&path, bytes)
        .await
        .expect("write wal fixture");

    let records = Wal::new(&path).read_all().await.expect("read fixture");

    assert_eq!(records, vec![CommitRecord::root_bootstrap(vec![0, 1, 2])]);
}
