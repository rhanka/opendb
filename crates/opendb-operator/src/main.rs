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
            tracing::info!("opendb operator-lite started");
            tokio::signal::ctrl_c().await?;
        }
    }

    Ok(())
}
