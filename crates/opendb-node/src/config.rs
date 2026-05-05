use clap::Parser;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug, Parser)]
#[command(name = "opendb-node", about = "OpenDB node")]
pub struct NodeConfig {
    #[arg(long, env = "OPENDB_NODE_ID")]
    pub node_id: u64,

    #[arg(long, env = "OPENDB_DATA_DIR", default_value = "/var/lib/opendb")]
    pub data_dir: PathBuf,

    #[arg(long, env = "OPENDB_PGWIRE_ADDR", default_value = "0.0.0.0:5432")]
    pub pgwire_addr: SocketAddr,

    #[arg(long, env = "OPENDB_HEALTH_ADDR", default_value = "0.0.0.0:8080")]
    pub health_addr: SocketAddr,
}
