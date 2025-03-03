use crate::mdc_server::config::Config;
use crate::mdc_server::market_event_stream::MarketEventStream;
use crate::mdc_server::models::{DepthUpdate, TradeEvent, PriceUpdate, MarketEvent};
use crate::mdc_server::depth_event_dispatcher::DepthEventDispatcher;
use crate::mdc_server::book_processor::BookProcessor;
use crate::mdc_server::market_event_logger::MarketEventLogger;
use crate::mdc_server::order_book::OrderBook;
use crate::mdc_server::depth_snapshot_stream::DepthSnapshotStream;
use tokio::sync::mpsc;
use anyhow::{Result};

pub struct MDCServer {
    config: Config
}

impl MDCServer {
    pub(crate) fn new(config: Config) -> Self {
        MDCServer{config}
    }

    pub(crate) async fn start(&self) -> Result<()> {
        let (depth_update_sender, depth_update_receiver) = mpsc::channel::<MarketEvent>(100);
        let (trade_update_sender, trade_update_receiver) = mpsc::channel::<MarketEvent>(100);
        let (price_update_sender, price_update_receiver) = mpsc::channel::<MarketEvent>(100);
        let (dispatch_sender, dispatch_receiver) = mpsc::channel::<MarketEvent>(100);
        let (book_update_sender, book_update_receiver) = mpsc::channel::<OrderBook>(100);
        
        let mut tasks = Vec::new();
        
        for i in 0..self.config.connections {
            let depth_url = format!("{}{}@depth@100ms", 
                self.config.binance_wss_endpoint, 
                self.config.instrument.to_lowercase());
            
            let mut depth_stream = MarketEventStream::<DepthUpdate>::new(
                depth_url,
                depth_update_sender.clone(), 
                self.config.reconnect_timeout
            );

            tasks.push(tokio::spawn(async move {
                tracing::info!("Starting depth update stream: '{}'", i);
                depth_stream.run().await;
            }));
        }
        
        let trade_url = format!("{}{}@trade", 
            self.config.binance_wss_endpoint, 
            self.config.instrument.to_lowercase());
        
        let mut trade_stream = MarketEventStream::<TradeEvent>::new(
            trade_url,
            trade_update_sender.clone(),
            self.config.reconnect_timeout
        );

        tasks.push(tokio::spawn(async move {
            tracing::info!("Starting trade update stream");
            trade_stream.run().await;
        }));
        
        let price_url = format!(
            "{}{}@bookTicker", 
            self.config.binance_wss_endpoint, 
            self.config.instrument.to_lowercase()
        );
        
        let mut price_stream = MarketEventStream::<PriceUpdate>::new(
            price_url,
            price_update_sender.clone(),
            self.config.reconnect_timeout
        );

        tasks.push(tokio::spawn(async move {
            tracing::info!("Starting price update stream");
            price_stream.run().await;
        }));
        
        let snapshot_stream = DepthSnapshotStream::new(
            self.config.binance_rest_endpoint.clone(),
            self.config.instrument.clone(),
            self.config.max_depth,
            self.config.snapshot_update_interval,
            depth_update_sender.clone()
        );

        tasks.push(tokio::spawn(async move {
            tracing::info!("Starting depth snapshot stream");
            snapshot_stream.run().await;
        }));
        
        let dispatcher = DepthEventDispatcher::new(
            depth_update_receiver,
            dispatch_sender
        );

        tasks.push(tokio::spawn(async move {
            tracing::info!("Starting depth event dispatcher");
            dispatcher.run().await;
        }));
        
        let book_processor = BookProcessor::new(
            dispatch_receiver,
            book_update_sender
        );

        tasks.push(tokio::spawn(async move {
            tracing::info!("Starting book processor");
            book_processor.run().await;
        }));
        
        let market_event_logger = MarketEventLogger::new(
            trade_update_receiver,
            price_update_receiver,
            book_update_receiver
        );

        tasks.push(tokio::spawn(async move {
            tracing::info!("Starting market event logger");
            market_event_logger.run().await;
        }));
        
        for handle in tasks {
            handle.await?;
        }

        Ok(())
    }
}
