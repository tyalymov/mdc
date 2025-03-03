use tokio::sync::mpsc;
use crate::mdc_server::models::{MarketEvent, DepthSnapshot, DepthUpdate};
use crate::mdc_server::order_book::OrderBook;

/// BookProcessor is an asynchronous wrapper around OrderBook
/// It processes MarketEvent messages from an input channel and sends updated OrderBook instances to an output channel
pub struct BookProcessor {
    order_book: Option<OrderBook>,
    input: mpsc::Receiver<MarketEvent>,
    output: mpsc::Sender<OrderBook>,
}

impl BookProcessor {
    /// Create a new BookProcessor
    ///
    /// # Arguments
    /// * `input` - Receiver for MarketEvent messages
    /// * `output` - Sender for OrderBook updates
    pub fn new(input: mpsc::Receiver<MarketEvent>, output: mpsc::Sender<OrderBook>) -> Self {
        Self {
            order_book: None,
            input,
            output,
        }
    }

    /// Send the current OrderBook state to the output channel
    ///
    /// # Panics
    /// * If sending to the output channel fails
    /// * If order_book is None
    async fn send_current_state(&self) {
        let order_book = self
            .order_book
            .as_ref()
            .expect("Failed to send order book state: order book is not initialized");
            
        self.output
            .send(order_book.clone())
            .await
            .expect("Failed to send order book to output channel");
    }

    /// Process a DepthUpdate
    ///
    /// # Arguments
    /// * `update` - The DepthUpdate to process
    ///
    /// # Behavior
    /// * Apply the update to the current OrderBook
    ///
    /// # Panics
    /// * If order_book is None
    async fn process_update(&mut self, update: DepthUpdate) {
        tracing::debug!("Processing depth update: '{:?}'", update);
        
        let order_book = self
            .order_book
            .as_mut()
            .expect("Cannot process depth update: order_book is not initialized");
        
        for bid in update.bids {
            order_book.apply_update(OrderBook::bid(bid.price), bid.quantity);
        }

        for ask in update.asks {
            order_book.apply_update(OrderBook::ask(ask.price), ask.quantity);
        }
    }
    
    /// Process a DepthSnapshot
    ///
    /// # Arguments
    /// * `snapshot` - The DepthSnapshot to process
    ///
    /// # Behavior
    /// * Replace the current OrderBook with a new one created from the snapshot
    async fn process_snapshot(&mut self, snapshot: DepthSnapshot) {
        tracing::debug!("Processing depth snapshot: '{:?}'", snapshot);
        self.order_book = Some(OrderBook::new(&snapshot));
    }

    /// Run the BookProcessor as an asynchronous task
    ///
    /// This method will continuously process messages from the input channel until it is closed
    /// DepthUpdate and DepthSnapshot messages are processed, all other message types will cause a panic
    pub async fn run(mut self) {
        tracing::info!("Starting BookProcessor");
        
        while let Some(event) = self.input.recv().await {
            match event {
                MarketEvent::DepthUpdate(update) => {
                    self.process_update(update).await;
                    self.send_current_state().await;
                }
                MarketEvent::DepthSnapshot(snapshot) => {
                    self.process_snapshot(snapshot).await;
                    self.send_current_state().await;
                }
                _ => {
                    tracing::error!("BookProcessor received unexpected event type: '{}'. Discarding", event);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mdc_server::models::{DepthEntry};
    use tokio::sync::mpsc;

    // Helper function to create a test snapshot
    fn create_test_snapshot() -> DepthSnapshot {
        DepthSnapshot {
            last_update_id: 123456,
            bids: vec![
                DepthEntry { price: 100.0, quantity: 10.0 },
                DepthEntry { price: 99.5, quantity: 15.0 },
            ],
            asks: vec![
                DepthEntry { price: 100.5, quantity: 5.0 },
                DepthEntry { price: 101.0, quantity: 8.0 },
            ],
        }
    }

    #[tokio::test]
    async fn test_book_processor_initialization() {
        let (_input_tx, input_rx) = mpsc::channel::<MarketEvent>(100);
        let (output_tx, mut output_rx) = mpsc::channel::<OrderBook>(100);
        
        let snapshot = create_test_snapshot();
        
        let mut processor = BookProcessor::new(input_rx, output_tx);
        
        processor.process_snapshot(snapshot.clone()).await;
        processor.send_current_state().await;
        
        let received_book = output_rx.recv().await.unwrap();
        
        assert_eq!(received_book.bids.len(), 2);
        assert_eq!(received_book.asks.len(), 2);
        assert_eq!(received_book.bids.get(&OrderBook::bid(100.0)).unwrap(), &10.0);
        assert_eq!(received_book.bids.get(&OrderBook::bid(99.5)).unwrap(), &15.0);
        assert_eq!(received_book.asks.get(&OrderBook::ask(100.5)).unwrap(), &5.0);
        assert_eq!(received_book.asks.get(&OrderBook::ask(101.0)).unwrap(), &8.0);
    }

    #[tokio::test]
    async fn test_book_processor_update() {
        let (input_tx, input_rx) = mpsc::channel::<MarketEvent>(100);
        let (output_tx, mut output_rx) = mpsc::channel::<OrderBook>(100);
        
        let snapshot = create_test_snapshot();
        
        let update = DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1672515782136,
            symbol: "BTCUSDT".to_string(),
            first_update_id: 123457,
            last_update_id: 123458,
            bids: vec![
                DepthEntry { price: 100.0, quantity: 12.0 },
                DepthEntry { price: 99.0, quantity: 5.0 },
            ],
            asks: vec![
                DepthEntry { price: 100.5, quantity: 0.0 },
                DepthEntry { price: 101.5, quantity: 3.0 },
            ],
        };
        
        let processor = BookProcessor::new(input_rx, output_tx);
        tokio::spawn(processor.run());
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update)).await.unwrap();
        drop(input_tx);
        
        let _snapshot_book = output_rx.recv().await.unwrap();
        let update_book = output_rx.recv().await.unwrap();
        
        assert_eq!(update_book.bids.len(), 3);
        assert_eq!(update_book.asks.len(), 2);
        assert_eq!(update_book.bids.get(&OrderBook::bid(100.0)).unwrap(), &12.0);
        assert_eq!(update_book.bids.get(&OrderBook::bid(99.0)).unwrap(), &5.0);
        assert_eq!(update_book.asks.get(&OrderBook::ask(100.5)), None);
        assert_eq!(update_book.asks.get(&OrderBook::ask(101.5)).unwrap(), &3.0);
    }

    #[tokio::test]
    async fn test_book_processor_multiple_updates() {
        let (input_tx, input_rx) = mpsc::channel::<MarketEvent>(100);
        let (output_tx, mut output_rx) = mpsc::channel::<OrderBook>(100);
        
        let snapshot = DepthSnapshot {
            last_update_id: 123456,
            bids: vec![
                DepthEntry { price: 100.0, quantity: 10.0 },
            ],
            asks: vec![
                DepthEntry { price: 101.0, quantity: 5.0 },
            ],
        };
        
        let update1 = DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1672515782136,
            symbol: "BTCUSDT".to_string(),
            first_update_id: 123457,
            last_update_id: 123458,
            bids: vec![
                DepthEntry { price: 100.0, quantity: 12.0 },
            ],
            asks: vec![],
        };

        let update2 = DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1672515782137,
            symbol: "BTCUSDT".to_string(),
            first_update_id: 123459,
            last_update_id: 123460,
            bids: vec![],
            asks: vec![
                DepthEntry { price: 101.0, quantity: 8.0 },
            ],
        };
        
        let processor = BookProcessor::new(input_rx, output_tx);
        tokio::spawn(processor.run());
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update1)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update2)).await.unwrap();
        
        drop(input_tx);
        
        let _snapshot_book = output_rx.recv().await.unwrap();
        let book1 = output_rx.recv().await.unwrap();
        let book2 = output_rx.recv().await.unwrap();
        
        assert_eq!(book1.bids.len(), 1);
        assert_eq!(book1.asks.len(), 1);
        assert_eq!(book1.bids.get(&OrderBook::bid(100.0)).unwrap(), &12.0);
        assert_eq!(book1.asks.get(&OrderBook::ask(101.0)).unwrap(), &5.0);
        
        assert_eq!(book2.bids.len(), 1);
        assert_eq!(book2.asks.len(), 1);
        assert_eq!(book2.bids.get(&OrderBook::bid(100.0)).unwrap(), &12.0);
        assert_eq!(book2.asks.get(&OrderBook::ask(101.0)).unwrap(), &8.0);
    }

    #[tokio::test]
    async fn test_book_processor_accepts_snapshot_after_init() {
        let (input_tx, input_rx) = mpsc::channel::<MarketEvent>(100);
        let (output_tx, mut output_rx) = mpsc::channel::<OrderBook>(100);
        
        let initial_snapshot = create_test_snapshot();
        
        let second_snapshot = DepthSnapshot {
            last_update_id: 123460,
            bids: vec![
                DepthEntry { price: 99.0, quantity: 15.0 },
            ],
            asks: vec![
                DepthEntry { price: 102.0, quantity: 8.0 },
            ],
        };
        
        let processor = BookProcessor::new(input_rx, output_tx);
        tokio::spawn(processor.run());
        
        input_tx.send(MarketEvent::DepthSnapshot(initial_snapshot)).await.unwrap();
        input_tx.send(MarketEvent::DepthSnapshot(second_snapshot.clone())).await.unwrap();
        drop(input_tx);
        
        let _initial_book = output_rx.recv().await.unwrap();
        let received_book = output_rx.recv().await.unwrap();
        
        assert_eq!(received_book.bids.len(), 1);
        assert_eq!(received_book.asks.len(), 1);
        assert_eq!(received_book.bids.get(&OrderBook::bid(99.0)).unwrap(), &15.0);
        assert_eq!(received_book.asks.get(&OrderBook::ask(102.0)).unwrap(), &8.0);
    }
    
    #[tokio::test]
    #[should_panic(expected = "Cannot process depth update: order_book is not initialized")]
    async fn test_book_processor_rejects_update_before_snapshot() {
        let (input_tx, input_rx) = mpsc::channel::<MarketEvent>(100);
        let (output_tx, _output_rx) = mpsc::channel::<OrderBook>(100);
        
        let update = DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1672515782136,
            symbol: "BTCUSDT".to_string(),
            first_update_id: 123457,
            last_update_id: 123458,
            bids: vec![
                DepthEntry { price: 100.0, quantity: 12.0 },
            ],
            asks: vec![],
        };
        
        let processor = BookProcessor::new(input_rx, output_tx);
        let handle = tokio::spawn(processor.run());
        
        input_tx.send(MarketEvent::DepthUpdate(update)).await.unwrap();
        handle.await.unwrap();
    }
}
