mod config;
mod database;
mod health;
mod pgwire;

use anyhow::Context;
use clap::Parser;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::NodeConfig;
use crate::database::Database;
use opendb_consensus::root_range::RootRange;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = NodeConfig::parse();
    tokio::fs::create_dir_all(&config.data_dir)
        .await
        .with_context(|| format!("create data dir {}", config.data_dir.display()))?;

    let root_range = RootRange::new_with_authority(
        &config.data_dir,
        config
            .root_range_authority()
            .context("derive root range authority")?,
    );
    let database = Arc::new(Mutex::new(
        Database::open_with_root_range(root_range)
            .await
            .with_context(|| format!("open database at {}", config.data_dir.display()))?,
    ));
    tracing::info!(
        node_id = config.node_id,
        pgwire_addr = %config.pgwire_addr,
        health_addr = %config.health_addr,
        internal_addr = %config.internal_addr,
        advertise_addr = %config.advertise_addr,
        bootstrap_node_id = config.bootstrap_node_id,
        "starting opendb node"
    );

    tokio::try_join!(
        health::serve(config.health_addr),
        pgwire::serve(config.pgwire_addr, database),
    )?;

    Ok(())
}
