use std::path::PathBuf;
use clap::Parser;
use tracing::Level;

fn parse_tracing_level(s: &str) -> anyhow::Result<Level, String> {
    match s.to_lowercase().as_str() {
        "trace" => Ok(Level::TRACE),
        "debug" => Ok(Level::DEBUG),
        "info"  => Ok(Level::INFO),
        "warn"  => Ok(Level::WARN),
        "error" => Ok(Level::ERROR),
        other => Err(format!("Unexpected log level: '{}'", other)),
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct CliArgs {
    #[arg(short = 'c', long = "config", default_value = "mdc.yaml")]
    pub config: PathBuf,

    #[arg(
        short = 'l',
        long = "log-level",
        value_parser = parse_tracing_level,
        default_value = "info"
    )]
    pub log_level: Level,
}