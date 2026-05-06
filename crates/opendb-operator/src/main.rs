mod crd;

use anyhow::Result;
use clap::Parser;
use kube::CustomResourceExt;

#[derive(Debug, Parser)]
#[command(author, version, about)]
enum Command {
    PrintCrd,
    Run,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match Command::parse() {
        Command::PrintCrd => {
            println!("{}", serde_yaml::to_string(&crd::OpenDbCluster::crd())?);
        }
        Command::Run => {
            let initial_status =
                crd::compute_open_db_cluster_status(crd::OpenDbClusterStatusSnapshot {
                    desired_replicas: crd::MIN_REPLICAS,
                    ready_pods: 0,
                    leader_pod: None,
                });

            tracing::info!(
                phase = %initial_status.phase,
                ready_replicas = initial_status.ready_replicas,
                "opendb operator-lite started"
            );
            tokio::signal::ctrl_c().await?;
        }
    }

    Ok(())
}
