mod config;
mod database;
mod health;
mod pgwire;

use anyhow::Context;
use clap::Parser;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::NodeConfig;
use crate::database::{Database, DatabaseRecoveryStatus};
use crate::health::HealthState;
use opendb_consensus::root_range::{RootRange, RootRangePeerServer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = NodeConfig::parse();
    tokio::fs::create_dir_all(&config.data_dir)
        .await
        .with_context(|| format!("create data dir {}", config.data_dir.display()))?;

    let health_state = HealthState::new(!config.uses_openraft());
    let (root_range, peer_server) = open_root_range(&config).await?;
    let peer_server = peer_server.map(Arc::new);
    let opened_database = open_database(root_range, peer_server.clone(), &config).await?;
    health_state.set_recovery_status(opened_database.recovery_status().clone().into());
    let database = Arc::new(Mutex::new(opened_database));
    tracing::info!(
        node_id = config.node_id,
        pgwire_addr = %config.pgwire_addr,
        health_addr = %config.health_addr,
        internal_addr = %config.internal_addr,
        advertise_addr = %config.advertise_addr,
        bootstrap_node_id = config.bootstrap_node_id,
        "starting opendb node"
    );

    if let Some(peer_server) = peer_server {
        let readiness = maintain_root_range_readiness(
            Arc::clone(&peer_server),
            config.node_id,
            health_state.clone(),
        );
        tokio::try_join!(
            health::serve(config.health_addr, health_state),
            pgwire::serve(config.pgwire_addr, database),
            readiness,
            async move {
                peer_server
                    .serve(config.internal_addr)
                    .await
                    .context("serve root range raft peer RPC")
            },
        )?;
    } else {
        tokio::try_join!(
            health::serve(config.health_addr, health_state),
            pgwire::serve(config.pgwire_addr, database),
        )?;
    }

    Ok(())
}

async fn open_root_range(
    config: &NodeConfig,
) -> anyhow::Result<(RootRange, Option<RootRangePeerServer>)> {
    if config.uses_openraft() {
        config
            .validate_openraft_runtime()
            .context("validate root range openraft runtime config")?;
        let (root_range, peer_server) =
            RootRange::new_raft_backed(&config.data_dir, config.node_id, config.root_range_peers())
                .await
                .context("open raft-backed root range")?;
        peer_server
            .initialize_cluster()
            .await
            .context("initialize root range raft cluster")?;
        return Ok((root_range, Some(peer_server)));
    }

    Ok((
        RootRange::new_with_authority(
            &config.data_dir,
            config
                .root_range_authority()
                .context("derive root range authority")?,
        ),
        None,
    ))
}

async fn maintain_root_range_readiness(
    peer_server: Arc<RootRangePeerServer>,
    node_id: u64,
    health_state: HealthState,
) -> anyhow::Result<()> {
    loop {
        health_state.set_ready(peer_server.is_leader(node_id).await);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn open_database(
    root_range: RootRange,
    peer_server: Option<Arc<RootRangePeerServer>>,
    config: &NodeConfig,
) -> anyhow::Result<Database> {
    match peer_server {
        Some(peer_server) => Database::open_with_root_range_peer_server(root_range, peer_server)
            .await
            .with_context(|| format!("open database at {}", config.data_dir.display())),
        None => Database::open_with_root_range(root_range)
            .await
            .with_context(|| format!("open database at {}", config.data_dir.display())),
    }
}

impl From<DatabaseRecoveryStatus> for health::RecoveryStatus {
    fn from(status: DatabaseRecoveryStatus) -> Self {
        Self {
            root_descriptor_known: status.root_descriptor_known,
            wal_replay_completed: status.wal_replay_completed,
            last_replayed_tx_id: status.last_replayed_tx_id,
            last_replayed_ts: status.last_replayed_ts,
            archive_metadata_replayed: status.archive_metadata_replayed,
            latest_recovery_artifact: None,
        }
    }
}
