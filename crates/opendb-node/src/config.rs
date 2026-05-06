use clap::Parser;
use opendb_common::{OpenDbError, OpenDbResult};
use opendb_consensus::root_range::{OpenDbRaftNodeId, RootRangeAuthority, RootRangePeer};
use std::{collections::HashSet, net::SocketAddr, path::PathBuf};

const DEFAULT_ADVERTISE_ADDR: &str = "127.0.0.1:7000";

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

    #[arg(long, env = "OPENDB_INTERNAL_ADDR", default_value = "0.0.0.0:7000")]
    pub internal_addr: SocketAddr,

    #[arg(long, env = "OPENDB_ADVERTISE_ADDR", default_value = DEFAULT_ADVERTISE_ADDR)]
    pub advertise_addr: String,

    #[arg(long, env = "OPENDB_INITIAL_PEERS", value_parser = parse_initial_peers_value, default_value = "")]
    pub initial_peers: InitialPeers,

    #[arg(long, env = "OPENDB_BOOTSTRAP_NODE_ID", default_value_t = 0)]
    pub bootstrap_node_id: OpenDbRaftNodeId,
}

impl NodeConfig {
    pub fn uses_openraft(&self) -> bool {
        !self.initial_peers.0.is_empty()
    }

    pub fn root_range_peers(&self) -> Vec<RootRangePeer> {
        self.initial_peers
            .0
            .iter()
            .map(|peer| RootRangePeer {
                node_id: peer.node_id,
                addr: if peer.node_id == self.node_id
                    && self.advertise_addr != DEFAULT_ADVERTISE_ADDR
                {
                    self.advertise_addr.clone()
                } else {
                    peer.addr.clone()
                },
            })
            .collect()
    }

    pub fn validate_openraft_runtime(&self) -> OpenDbResult<()> {
        if !self.uses_openraft() {
            return Ok(());
        }

        if !self
            .initial_peers
            .0
            .iter()
            .any(|peer| peer.node_id == self.node_id)
        {
            return Err(OpenDbError::InvalidInput(format!(
                "node id {} is missing from OPENDB_INITIAL_PEERS",
                self.node_id
            )));
        }

        if !self
            .initial_peers
            .0
            .iter()
            .any(|peer| peer.node_id == self.bootstrap_node_id)
        {
            return Err(OpenDbError::InvalidInput(format!(
                "bootstrap node id {} is missing from OPENDB_INITIAL_PEERS",
                self.bootstrap_node_id
            )));
        }

        Ok(())
    }

    pub fn root_range_authority(&self) -> OpenDbResult<RootRangeAuthority> {
        if self.initial_peers.0.is_empty() {
            return Ok(RootRangeAuthority::Standalone);
        }

        let leader = self
            .initial_peers
            .0
            .iter()
            .find(|peer| peer.node_id == self.bootstrap_node_id)
            .ok_or_else(|| {
                OpenDbError::InvalidInput(format!(
                    "bootstrap node id {} is missing from OPENDB_INITIAL_PEERS",
                    self.bootstrap_node_id
                ))
            })?;

        if !self
            .initial_peers
            .0
            .iter()
            .any(|peer| peer.node_id == self.node_id)
        {
            return Err(OpenDbError::InvalidInput(format!(
                "node id {} is missing from OPENDB_INITIAL_PEERS",
                self.node_id
            )));
        }

        if self.node_id == self.bootstrap_node_id {
            Ok(RootRangeAuthority::leader(self.node_id))
        } else {
            Ok(RootRangeAuthority::follower(
                self.bootstrap_node_id,
                Some(leader.addr.clone()),
            ))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitialPeer {
    pub node_id: OpenDbRaftNodeId,
    pub addr: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InitialPeers(pub Vec<InitialPeer>);

fn parse_initial_peers_value(value: &str) -> Result<InitialPeers, String> {
    parse_initial_peers(value).map(InitialPeers)
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

fn parse_initial_peers(value: &str) -> Result<Vec<InitialPeer>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(Vec::new());
    }

    let mut seen = HashSet::new();
    let mut peers = Vec::new();

    for entry in value.split(',') {
        let entry = entry.trim();
        let Some((node_id, addr)) = entry.split_once('=') else {
            return Err(format!(
                "initial peer {entry:?} must use node_id=host:port syntax"
            ));
        };
        if node_id.is_empty() || addr.is_empty() {
            return Err(format!(
                "initial peer {entry:?} must include both node id and address"
            ));
        }
        let node_id: OpenDbRaftNodeId = node_id
            .parse()
            .map_err(|error| format!("initial peer node id {node_id:?} is invalid: {error}"))?;
        if !seen.insert(node_id) {
            return Err(format!(
                "initial peer node id {node_id} is declared more than once"
            ));
        }
        peers.push(InitialPeer {
            node_id,
            addr: addr.to_string(),
        });
    }

    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendb_consensus::root_range::RootRangeAuthority;
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

    #[test]
    fn parses_initial_peer_list() {
        assert_eq!(
            parse_initial_peers(
                "0=opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000,\
                 1=opendb-1.opendb-peer.opendb-system.svc.cluster.local:7000"
            )
            .unwrap(),
            vec![
                InitialPeer {
                    node_id: 0,
                    addr: "opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000".to_string(),
                },
                InitialPeer {
                    node_id: 1,
                    addr: "opendb-1.opendb-peer.opendb-system.svc.cluster.local:7000".to_string(),
                }
            ]
        );
    }

    #[test]
    fn rejects_malformed_initial_peer_list() {
        for value in [
            "0",
            "0=",
            "=opendb-0:7000",
            "node-0=opendb-0:7000",
            "0=opendb-0:7000,0=opendb-duplicate:7000",
        ] {
            assert!(
                parse_initial_peers(value).is_err(),
                "{value} should be invalid"
            );
        }
    }

    #[test]
    fn derives_root_range_authority_from_static_kube_peers() {
        let peers = "0=opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000,1=opendb-1.opendb-peer.opendb-system.svc.cluster.local:7000,2=opendb-2.opendb-peer.opendb-system.svc.cluster.local:7000";
        let leader = NodeConfig::try_parse_from([
            "opendb-node",
            "--node-id=0",
            "--initial-peers",
            peers,
            "--bootstrap-node-id=0",
        ])
        .unwrap();
        let follower = NodeConfig::try_parse_from([
            "opendb-node",
            "--node-id=2",
            "--initial-peers",
            peers,
            "--bootstrap-node-id=0",
        ])
        .unwrap();

        assert_eq!(
            leader.root_range_authority().unwrap(),
            RootRangeAuthority::leader(0)
        );
        assert_eq!(
            follower.root_range_authority().unwrap(),
            RootRangeAuthority::follower(
                0,
                Some("opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000".to_string())
            )
        );
    }

    #[test]
    fn rejects_multi_node_authority_when_self_is_missing_from_initial_peers() {
        let config = NodeConfig::try_parse_from([
            "opendb-node",
            "--node-id=9",
            "--initial-peers",
            "0=opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000,1=opendb-1.opendb-peer.opendb-system.svc.cluster.local:7000,2=opendb-2.opendb-peer.opendb-system.svc.cluster.local:7000",
            "--bootstrap-node-id=0",
        ])
        .unwrap();

        let result = config.root_range_authority();

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message))
                if message.contains("node id 9") && message.contains("OPENDB_INITIAL_PEERS")
        ));
    }

    #[test]
    fn rejects_single_peer_config_instead_of_falling_back_to_standalone() {
        let config = NodeConfig::try_parse_from([
            "opendb-node",
            "--node-id=9",
            "--initial-peers",
            "0=opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000",
            "--bootstrap-node-id=0",
        ])
        .unwrap();

        let result = config.root_range_authority();

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message))
                if message.contains("node id 9") && message.contains("OPENDB_INITIAL_PEERS")
        ));
    }

    #[test]
    fn derives_follower_authority_from_statefulset_pod_name_and_static_peers() {
        let config = NodeConfig::try_parse_from([
            "opendb-node",
            "--node-id=opendb-2",
            "--initial-peers",
            "0=opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000,1=opendb-1.opendb-peer.opendb-system.svc.cluster.local:7000,2=opendb-2.opendb-peer.opendb-system.svc.cluster.local:7000",
            "--bootstrap-node-id=0",
        ])
        .unwrap();

        assert_eq!(
            config.root_range_authority().unwrap(),
            RootRangeAuthority::follower(
                0,
                Some("opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000".to_string())
            )
        );
    }

    #[test]
    fn exposes_root_range_peers_for_openraft_runtime() {
        let config = NodeConfig::try_parse_from([
            "opendb-node",
            "--node-id=opendb-1",
            "--initial-peers",
            "0=opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000,1=opendb-1.opendb-peer.opendb-system.svc.cluster.local:7000",
        ])
        .unwrap();

        assert!(config.uses_openraft());
        assert_eq!(
            config.root_range_peers(),
            vec![
                RootRangePeer {
                    node_id: 0,
                    addr: "opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000".to_string(),
                },
                RootRangePeer {
                    node_id: 1,
                    addr: "opendb-1.opendb-peer.opendb-system.svc.cluster.local:7000".to_string(),
                },
            ]
        );
    }

    #[test]
    fn root_range_peers_use_advertise_addr_for_self() {
        let config = NodeConfig::try_parse_from([
            "opendb-node",
            "--node-id=opendb-1",
            "--advertise-addr=opendb-1.custom-peer:7000",
            "--initial-peers",
            "0=opendb-0.opendb-peer:7000,1=placeholder-self:7000",
        ])
        .unwrap();

        assert_eq!(
            config.root_range_peers(),
            vec![
                RootRangePeer {
                    node_id: 0,
                    addr: "opendb-0.opendb-peer:7000".to_string(),
                },
                RootRangePeer {
                    node_id: 1,
                    addr: "opendb-1.custom-peer:7000".to_string(),
                },
            ]
        );
    }

    #[test]
    fn validates_bootstrap_node_id_for_openraft_runtime() {
        let config = NodeConfig::try_parse_from([
            "opendb-node",
            "--node-id=0",
            "--initial-peers",
            "0=opendb-0.opendb-peer:7000,1=opendb-1.opendb-peer:7000",
            "--bootstrap-node-id=9",
        ])
        .unwrap();

        let result = config.validate_openraft_runtime();

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message))
                if message.contains("bootstrap node id 9")
                    && message.contains("OPENDB_INITIAL_PEERS")
        ));
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
