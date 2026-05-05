use clap::Parser;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug, Parser)]
#[command(name = "opendb-node", about = "OpenDB node")]
pub struct NodeConfig {
    #[arg(long, env = "OPENDB_NODE_ID", value_parser = parse_node_id)]
    pub node_id: u64,

    #[arg(long, env = "OPENDB_DATA_DIR", default_value = "/var/lib/opendb")]
    pub data_dir: PathBuf,

    #[arg(long, env = "OPENDB_PGWIRE_ADDR", default_value = "0.0.0.0:5432")]
    pub pgwire_addr: SocketAddr,

    #[arg(long, env = "OPENDB_HEALTH_ADDR", default_value = "0.0.0.0:8080")]
    pub health_addr: SocketAddr,
}

fn parse_node_id(value: &str) -> Result<u64, String> {
    if !value.is_empty() && value.chars().all(|character| character.is_ascii_digit()) {
        return value
            .parse()
            .map_err(|error| format!("node id {value:?} is not a valid u64: {error}"));
    }

    let Some((pod_name, ordinal)) = value.rsplit_once('-') else {
        return Err(format!(
            "node id {value:?} must be an unsigned integer or a pod name ending in a numeric ordinal"
        ));
    };

    if pod_name.is_empty()
        || ordinal.is_empty()
        || !ordinal.chars().all(|character| character.is_ascii_digit())
        || !is_valid_pod_name_prefix(pod_name)
    {
        return Err(format!(
            "node id {value:?} must be an unsigned integer or a pod name ending in a numeric ordinal"
        ));
    }

    ordinal
        .parse()
        .map_err(|error| format!("node id ordinal in {value:?} is not a valid u64: {error}"))
}

fn is_valid_pod_name_prefix(value: &str) -> bool {
    let bytes = value.as_bytes();

    let (Some(first), Some(last)) = (bytes.first(), bytes.last()) else {
        return false;
    };

    is_lowercase_alphanumeric(*first)
        && is_lowercase_alphanumeric(*last)
        && bytes
            .iter()
            .all(|byte| is_lowercase_alphanumeric(*byte) || *byte == b'-')
}

fn is_lowercase_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env, ffi::OsString, panic, sync::Mutex};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn parses_bare_numeric_node_id() {
        assert_eq!(parse_node_id("1").unwrap(), 1);
    }

    #[test]
    fn parses_statefulset_pod_name_node_id() {
        assert_eq!(parse_node_id("opendb-0").unwrap(), 0);
        assert_eq!(parse_node_id("opendb-node-12").unwrap(), 12);
    }

    #[test]
    fn rejects_malformed_node_ids() {
        for value in [
            "opendb",
            "opendb-",
            "opendb-a",
            "-1",
            "--1",
            "opendb--1",
            "foo/bar-1",
            " bad-1",
            "OpenDb-1",
            "opendb_-1",
        ] {
            assert!(parse_node_id(value).is_err(), "{value} should be invalid");
        }
    }

    #[test]
    fn clap_uses_node_id_parser() {
        let config = NodeConfig::try_parse_from(["opendb-node", "--node-id=opendb-12"]).unwrap();

        assert_eq!(config.node_id, 12);
    }

    #[test]
    fn clap_uses_node_id_parser_from_env() {
        with_env_var("OPENDB_NODE_ID", "opendb-2", || {
            let config = NodeConfig::try_parse_from(["opendb-node"]).unwrap();

            assert_eq!(config.node_id, 2);
        });
    }

    fn with_env_var<R>(key: &'static str, value: &str, test: impl FnOnce() -> R) -> R {
        let _lock = ENV_LOCK.lock().unwrap();
        let previous = env::var_os(key);

        // SAFETY: This test helper serializes process environment mutation with
        // ENV_LOCK and restores the previous value before releasing the lock.
        unsafe {
            env::set_var(key, value);
        }

        let result = panic::catch_unwind(panic::AssertUnwindSafe(test));
        restore_env_var(key, previous);

        match result {
            Ok(result) => result,
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    fn restore_env_var(key: &'static str, previous: Option<OsString>) {
        // SAFETY: Callers hold ENV_LOCK while restoring the process environment.
        unsafe {
            match previous {
                Some(value) => env::set_var(key, value),
                None => env::remove_var(key),
            }
        }
    }
}
