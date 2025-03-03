use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Configuration for the Market Data Capture (MDC) server.
///
/// This struct holds all the configuration parameters needed to run the MDC server
#[derive(Debug, Deserialize)]
pub struct Config {
    pub binance_rest_endpoint: String,
    pub binance_wss_endpoint: String,
    pub instrument: String,
    pub max_depth: u64,
    pub connections: u64,
    pub reconnect_timeout: u64,
    pub snapshot_update_interval: u64,
}

/// Parses a YAML string into a `Config` struct.
///
/// # Arguments
/// * `yaml_data` - A string containing YAML-formatted configuration data
///
/// # Returns
/// * `Result<Config>` - The parsed configuration if successful, or an error if parsing fails
///
/// # Errors
/// Returns an error if the YAML data is invalid or missing required fields
pub fn load_config_from_yaml_str(yaml_data: &str) -> Result<Config> {
    let config: Config = serde_yaml::from_str(yaml_data)
        .context("Failed to deserialize configuration from YAML")?;
    Ok(config)
}

/// Loads a configuration from a YAML file at the specified path.
///
/// # Arguments
/// * `path` - Path to the YAML configuration file
///
/// # Returns
/// * `Result<Config>` - The loaded configuration if successful, or an error if loading fails
///
/// # Errors
/// Returns an error if:
/// - The file cannot be read
/// - The file content is not valid YAML
/// - The YAML data is missing required fields
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<Config> {
    let data = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read configuration from: {:?}", path.as_ref()))?;
    let config = load_config_from_yaml_str(&data)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config_from_yaml_str() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let test_content = r#"
binance_rest_endpoint: "https://api.example.com"
binance_wss_endpoint: "wss://stream.example.com"
instrument: "BTCUSDT"
max_depth: 10
connections: 3
reconnect_timeout: 5000
snapshot_update_interval: 30000
"#;

        let config = load_config_from_yaml_str(test_content)?;

        assert_eq!(config.binance_rest_endpoint, "https://api.example.com");
        assert_eq!(config.binance_wss_endpoint, "wss://stream.example.com");
        assert_eq!(config.instrument, "BTCUSDT");
        assert_eq!(config.max_depth, 10);
        assert_eq!(config.connections, 3);
        assert_eq!(config.reconnect_timeout, 5000);
        assert_eq!(config.snapshot_update_interval, 30000);

        Ok(())
    }
}
