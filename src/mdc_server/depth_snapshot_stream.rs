use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use anyhow::{Result, Context};
use crate::mdc_server::models::{DepthSnapshot, MarketEvent, FromJson};
use reqwest;
use tracing;

/// This class periodically requests order book snapshots using Binance REST API
/// and sends them to the DepthEventDispatcher as a MarketEvent::DepthSnapshot message
pub struct DepthSnapshotStream {
    binance_rest_endpoint: String,
    instrument: String,
    max_depth: u64,
    update_interval: u64,
    output: mpsc::Sender<MarketEvent>,
}

impl DepthSnapshotStream {
    /// Create a new DepthSnapshotStream
    ///
    /// # Arguments
    /// * `binance_rest_endpoint` - The Binance REST API endpoint
    /// * `instrument` - The trading instrument (e.g., "BTCUSDT")
    /// * `max_depth` - The maximum depth of the order book to request (up to 5000)
    /// * `update_interval` - The interval between snapshot updates in milliseconds
    /// * `output` - Sender for MarketEvent messages to the DepthEventDispatcher
    pub fn new(
        binance_rest_endpoint: String,
        instrument: String,
        max_depth: u64,
        update_interval: u64,
        output: mpsc::Sender<MarketEvent>,
    ) -> Self {
        Self {
            binance_rest_endpoint,
            instrument,
            max_depth,
            update_interval,
            output,
        }
    }

    /// Get market data snapshot from the Binance REST API
    async fn get_snapshot(&self) -> Result<DepthSnapshot> {
        let url = format!("{}depth?symbol={}&limit={}", 
            self.binance_rest_endpoint, 
            self.instrument, 
            self.max_depth);
        
        let response = reqwest::get(&url)
            .await
            .context("Failed to send snapshot request")?
            .error_for_status()
            .context("Failed to get snapshot response")?;
        
        let response_text = response
            .text()
            .await
            .context("Failed to get response text for snapshot")?;

        tracing::trace!("Received depth snapshot from binance: '{:?}'", response_text);
        
        let snapshot = DepthSnapshot::from_json(&response_text)
            .context("Failed to parse snapshot")?;
        
        Ok(snapshot)
    }

    /// Run the DepthSnapshotStream as an asynchronous task
    ///
    /// This method will continuously request snapshots from the Binance REST API
    /// at the specified interval and send them to the DepthEventDispatcher
    pub async fn run(self) {
        tracing::info!("Starting DepthSnapshotStream with update interval: '{}' ms", self.update_interval);
        
        loop {
            match self.get_snapshot().await {
                Ok(snapshot) => {
                    if let Err(e) = self.output.send(MarketEvent::DepthSnapshot(snapshot)).await {
                        tracing::error!("Failed to send snapshot to DepthEventDispatcher: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to get market depth snapshot. Details: '{}'", e);
                }
            }
            
            sleep(Duration::from_millis(self.update_interval)).await;
        }
    }
}
