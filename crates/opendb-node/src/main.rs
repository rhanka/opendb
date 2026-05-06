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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = NodeConfig::parse();
    tokio::fs::create_dir_all(&config.data_dir)
        .await
        .with_context(|| format!("create data dir {}", config.data_dir.display()))?;

    let database = Arc::new(Mutex::new(
        Database::open(&config.data_dir)
            .await
            .with_context(|| format!("open database at {}", config.data_dir.display()))?,
    ));
    tracing::info!(
        node_id = config.node_id,
        pgwire_addr = %config.pgwire_addr,
        health_addr = %config.health_addr,
        "starting opendb node"
    );

    tokio::try_join!(
        health::serve(config.health_addr),
        pgwire::serve(config.pgwire_addr, database),
    )?;

    Ok(())
}
