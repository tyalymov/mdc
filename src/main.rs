mod mdc_server;
mod common;

use mdc_server::config::Config;
use mdc_server::config::load_config;
use common::cli_args::CliArgs;
use anyhow::Result;
use clap::Parser;
use tracing_subscriber::FmtSubscriber;
use crate::mdc_server::server::MDCServer;

#[tokio::main]
async fn main() -> Result<()> {
    let cli_args: CliArgs = CliArgs::parse();
    
    let subscriber = FmtSubscriber::builder()
        .with_max_level(cli_args.log_level)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global default subscriber");

    tracing::info!("Starting Market Depth Capture tool");
    
    let mdc_server_config: Config = load_config(&cli_args.config)?;
    let mdc_server: MDCServer = MDCServer::new(mdc_server_config);
    
    mdc_server.start().await?;

    Ok(())
}
