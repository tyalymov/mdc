use tokio::sync::mpsc;

use crate::mdc_server::models::{MarketEvent};
use crate::mdc_server::order_book::OrderBook;

/// EventLogger is responsible for logging market events to stdout
/// It receives events from three channels: MarketEvent (for trades), MarketEvent (for prices), and OrderBook
pub struct MarketEventLogger {
    trade_channel: mpsc::Receiver<MarketEvent>,
    price_channel: mpsc::Receiver<MarketEvent>,
    book_channel: mpsc::Receiver<OrderBook>,
}

impl MarketEventLogger {
    /// Create a new EventLogger
    ///
    /// # Arguments
    /// * `trade_channel` - Receiver for MarketEvent messages containing TradeEvents
    /// * `price_channel` - Receiver for MarketEvent messages containing PriceUpdates
    /// * `book_channel` - Receiver for OrderBook messages
    pub fn new(
        trade_channel: mpsc::Receiver<MarketEvent>,
        price_channel: mpsc::Receiver<MarketEvent>,
        book_channel: mpsc::Receiver<OrderBook>,
    ) -> Self {
        Self {
            trade_channel,
            price_channel,
            book_channel,
        }
    }

    /// Run the EventLogger as an asynchronous task
    ///
    /// This method will continuously process messages from all three channels
    /// and log them to stdout until all channels are closed
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(event) = self.trade_channel.recv() => {
                    match event {
                        MarketEvent::TradeEvent(trade) => { println!("TRADE: {}", trade); },
                        _ => { tracing::warn!("Unexpected event in trade channel: '{}'", event); }
                    }
                }
                Some(event) = self.price_channel.recv() => {
                    match event {
                        MarketEvent::PriceUpdate(price) => { println!("PRICE: {}", price); },
                        _ => { tracing::warn!("Unexpected event in price channel: '{}'", event); }
                    }
                }
                
                Some(book) = self.book_channel.recv() => {
                    println!("{}", book);
                }
                
                // If all channels are closed, break the loop
                else => break,
            }
        }
        
        return;
    }
}
