mod config;
mod health;
mod pgwire;

use anyhow::Context;
use clap::Parser;
use opendb_sql::executor::SqlEngine;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::NodeConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = NodeConfig::parse();
    tokio::fs::create_dir_all(&config.data_dir)
        .await
        .with_context(|| format!("create data dir {}", config.data_dir.display()))?;

    let engine = Arc::new(Mutex::new(SqlEngine::default()));
    tracing::info!(
        node_id = config.node_id,
        pgwire_addr = %config.pgwire_addr,
        health_addr = %config.health_addr,
        "starting opendb node"
    );

    tokio::try_join!(
        health::serve(config.health_addr),
        pgwire::serve(config.pgwire_addr, engine),
    )?;

    Ok(())
}
