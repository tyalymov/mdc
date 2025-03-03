use tokio::sync::mpsc;
use crate::mdc_server::models::{MarketEvent, DepthUpdate, DepthSnapshot};
use std::collections::BTreeMap;
use tracing;

/// DepthEventDispatcher manages the order of depth updates from multiple WebSocket connections
/// It ensures that updates are processed in the correct order and without duplicates
pub struct DepthEventDispatcher {
    input: mpsc::Receiver<MarketEvent>,
    output: mpsc::Sender<MarketEvent>,
    last_processed_update_id: Option<u64>,
    buffer: BTreeMap<u64, DepthUpdate>,
}

impl DepthEventDispatcher {
    /// Create a new DepthEventDispatcher
    ///
    /// # Arguments
    /// * `input` - Receiver for MarketEvent messages from multiple connections
    /// * `output` - Sender for filtered MarketEvent messages to the BookProcessor
    pub fn new(
        input: mpsc::Receiver<MarketEvent>,
        output: mpsc::Sender<MarketEvent>,
    ) -> Self {
        DepthEventDispatcher {
            input,
            output,
            last_processed_update_id: None,
            buffer: BTreeMap::new(),
        }
    }

    /// Process a DepthUpdate event by adding it to the buffer
    ///
    /// # Arguments
    /// * `update` - The DepthUpdate to process
    ///
    /// # Behavior
    /// * Always add the update to the buffer, using last_update_id as the key
    async fn process_update(&mut self, update: DepthUpdate) {
        let current_id_str = match self.last_processed_update_id {
            Some(id) => id.to_string(),
            None => "uninitialized".to_string(),
        };
        
        tracing::debug!(
            "Received depth update with ids: '{}-{}'. Current expected id: '{}'", 
            update.first_update_id, 
            update.last_update_id,
            current_id_str
        );
        
        self.buffer.insert(update.last_update_id, update);
    }

    /// Process a DepthSnapshot event by updating the current update ID
    ///
    /// # Arguments
    /// * `snapshot` - The DepthSnapshot to process
    ///
    /// # Behavior
    /// * Update the current update ID to the snapshot's last update ID
    async fn process_snapshot(&mut self, snapshot: &DepthSnapshot) {
        tracing::debug!("Received snapshot: '{:?}'", snapshot);
        
        if self.last_processed_update_id.is_none() {
            tracing::trace!("The snapshot if first. Forwarding it and initializing expected id to: '{:?}'", snapshot.last_update_id);
            self.last_processed_update_id = Some(snapshot.last_update_id);
            self.output
                .send(MarketEvent::DepthSnapshot(snapshot.clone()))
                .await
                .expect("Failed to forward DepthSnapshot to output channel");
            
            return;
        }
        
        let last_processed_update_id = self.last_processed_update_id.unwrap();
        
        if snapshot.last_update_id <= last_processed_update_id {
            tracing::trace!("Received snapshot, which update id '{}' is older then last processed update id '{}'. Skipping", snapshot.last_update_id, last_processed_update_id);
            return;
        }

        tracing::trace!("Received snapshot, which update id '{}' is newer, then last processed update id '{}'. Forwarding and starting update process from new update id", snapshot.last_update_id, last_processed_update_id);
        self.last_processed_update_id = Some(snapshot.last_update_id);

        self.output
            .send(MarketEvent::DepthSnapshot(snapshot.clone()))
            .await
            .expect("Failed to forward DepthSnapshot to output channel");
    }

    /// Process the buffer to send updates to the output channel
    ///
    /// # Behavior
    /// * Implement Binance's rules for maintaining a local order book:
    ///   1. Discard any event where `u` (last_update_id) is <= lastUpdateId of the snapshot
    ///   2. The first buffered event should have lastUpdateId within its [U;u] range
    /// * Process events in sequence
    /// * Send events to the output channel
    async fn process_buffer(&mut self) {
        let Some(last_processed_update_id) = self.last_processed_update_id else {
            tracing::trace!("No current_update_id set, skipping buffer processing");
            return;
        };
        
        tracing::trace!("Processing buffer. Current expected id: '{}'", last_processed_update_id);
        
        if self.buffer.is_empty() {
            tracing::trace!("The buffer is empty, nothing to process");
            return;
        }
        
        let mut expected_first_update_id = last_processed_update_id + 1;
        let mut processed_keys = Vec::new();
        
        for (last_update_id, depth_update) in self.buffer.iter() {
            if *last_update_id <= last_processed_update_id {
                processed_keys.push(*last_update_id);
                continue;
            }
            
            if !(depth_update.first_update_id <= expected_first_update_id && expected_first_update_id < depth_update.last_update_id) {
                break;
            }
            
            processed_keys.push(*last_update_id);
            expected_first_update_id = depth_update.last_update_id + 1;
            

            self.last_processed_update_id = Some(depth_update.last_update_id);

            tracing::trace!(
                "Forwarding depth updates: '{}'-'{}'. Updated last processed id to: '{}'", 
                depth_update.first_update_id, 
                depth_update.last_update_id, 
                depth_update.last_update_id
            );
            
            self.output
                .send(MarketEvent::DepthUpdate(depth_update.clone()))
                .await
                .expect("Failed to send DepthUpdate to output channel");
        }

        // Remove only the processed updates from the buffer
        for key in processed_keys {
            self.buffer.remove(&key);
        }
    }

    /// Run the DepthEventDispatcher
    ///
    /// This method will continuously process messages from the input channel
    /// and send filtered messages to the output channel
    pub async fn run(mut self) {
        tracing::info!("Starting DepthEventDispatcher");
        
        while let Some(event) = self.input.recv().await {
            match event {
                MarketEvent::DepthUpdate(update) => {
                    self.process_update(update).await;
                    self.process_buffer().await;
                }
                MarketEvent::DepthSnapshot(snapshot) => {
                    self.process_snapshot(&snapshot).await;
                    self.process_buffer().await;
                }
                _ => {
                    tracing::error!("Received unexpected event type: '{:?}'. Discarding", &event);               
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mdc_server::models::{DepthSnapshot};
    use tokio::sync::mpsc;
    use tokio::time::{sleep, Duration};
    use tokio::task::JoinHandle;
    
    fn make_update(first: u64, last: u64) -> DepthUpdate {
        DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1672515782136,
            symbol: "BTCUSDT".to_string(),
            first_update_id: first,
            last_update_id: last,
            bids: vec![],
            asks: vec![],
        }
    }
    
    fn make_snapshot(last: u64) -> DepthSnapshot {
        DepthSnapshot {
            last_update_id: last,
            bids: vec![],
            asks: vec![],
        }
    }
    
    async fn setup_test() -> (
        mpsc::Sender<MarketEvent>,
        mpsc::Receiver<MarketEvent>,
        JoinHandle<()>,
    ) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .try_init();
        
        let (input_tx, input_rx) = mpsc::channel::<MarketEvent>(100);
        let (output_tx, output_rx) = mpsc::channel::<MarketEvent>(100);
        
        let dispatcher = DepthEventDispatcher::new(input_rx, output_tx);
        let handle = tokio::spawn(dispatcher.run());

        (input_tx, output_rx, handle)
    }
    
    fn verify_update(event: MarketEvent, expected_first: u64, expected_last: u64) {
        match event {
            MarketEvent::DepthUpdate(update) => {
                assert_eq!(update.first_update_id, expected_first);
                assert_eq!(update.last_update_id, expected_last);
            },
            _ => panic!("Expected DepthUpdate"),
        }
    }
    
    fn verify_snapshot(event: MarketEvent, expected_last: u64) {
        match event {
            MarketEvent::DepthSnapshot(snap) => {
                assert_eq!(snap.last_update_id, expected_last);
            },
            _ => panic!("Expected DepthSnapshot"),
        }
    }

    #[tokio::test]
    async fn test_depth_event_dispatcher_in_order() {
        let (input_tx, mut output_rx, _handle) = setup_test().await;
        
        let snapshot = make_snapshot(100);
        let update1 = make_update(101, 105);
        let update2 = make_update(106, 110);
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update1)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update2)).await.unwrap();
        
        let received_snapshot = output_rx.recv().await.unwrap();
        let received1 = output_rx.recv().await.unwrap();
        let received2 = output_rx.recv().await.unwrap();

        verify_snapshot(received_snapshot, 100);
        verify_update(received1, 101, 105);
        verify_update(received2, 106, 110);
    }

    #[tokio::test]
    async fn test_depth_event_dispatcher_out_of_order() {
        let (input_tx, mut output_rx, _handle) = setup_test().await;
        
        let snapshot = make_snapshot(100);
        let update1 = make_update(106, 110);
        let update2 = make_update(101, 105);
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update1)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update2)).await.unwrap();
        
        let received_snapshot = output_rx.recv().await.unwrap();
        let received1 = output_rx.recv().await.unwrap();
        let received2 = output_rx.recv().await.unwrap();

        verify_snapshot(received_snapshot, 100);
        verify_update(received1, 101, 105);
        verify_update(received2, 106, 110);
    }

    #[tokio::test]
    async fn test_depth_event_dispatcher_duplicates() {
        let (input_tx, mut output_rx, _handle) = setup_test().await;
        
        let snapshot = make_snapshot(100);
        let update = make_update(101, 105);
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update.clone())).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update)).await.unwrap();
        
        let received_snapshot = output_rx.recv().await.unwrap();
        let received_update = output_rx.recv().await.unwrap();

        verify_snapshot(received_snapshot, 100);
        verify_update(received_update, 101, 105);
        
        tokio::select! {
            _ = sleep(Duration::from_millis(100)) => {}
            _ = output_rx.recv() => {
                panic!("Received unexpected update");
            }
        }
    }

    #[tokio::test]
    async fn test_depth_event_dispatcher_old_update() {
        let (input_tx, mut output_rx, _handle) = setup_test().await;
        
        let snapshot = make_snapshot(100);
        let old_update = make_update(95, 99);
        let valid_update = make_update(101, 105);
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(old_update)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(valid_update)).await.unwrap();
        
        let received_snapshot = output_rx.recv().await.unwrap();
        let received_update = output_rx.recv().await.unwrap();

        verify_snapshot(received_snapshot, 100);
        verify_update(received_update, 101, 105);
    }

    #[tokio::test]
    async fn test_depth_event_dispatcher_snapshot_forwarding() {
        let (input_tx, mut output_rx, _handle) = setup_test().await;
        let snapshot = make_snapshot(200);
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        
        let received = output_rx.recv().await.unwrap();
        verify_snapshot(received, 200);
    }

    #[tokio::test]
    async fn test_depth_event_dispatcher_complex_sequence() {
        let (input_tx, mut output_rx, _handle) = setup_test().await;
        
        let snapshot = make_snapshot(100);
        let update1 = make_update(101, 105);
        let update2 = make_update(106, 110);
        let update3 = make_update(111, 115);
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update2)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update3)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update1)).await.unwrap();
        
        let received_snapshot = output_rx.recv().await.unwrap();
        let received1 = output_rx.recv().await.unwrap();
        let received2 = output_rx.recv().await.unwrap();
        let received3 = output_rx.recv().await.unwrap();

        verify_snapshot(received_snapshot, 100);
        verify_update(received1, 101, 105);
        verify_update(received2, 106, 110);
        verify_update(received3, 111, 115);
    }

    #[tokio::test]
    async fn test_depth_event_dispatcher_multiple_snapshots() {
        let (input_tx, mut output_rx, _handle) = setup_test().await;
        
        let snapshot1 = make_snapshot(100);
        let snapshot2 = make_snapshot(200);
        
        let update1 = make_update(101, 105);
        let update2 = make_update(201, 205);
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot1)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update1)).await.unwrap();
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot2)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update2)).await.unwrap();
        
        let received_snapshot1 = output_rx.recv().await.unwrap();
        let received_update1 = output_rx.recv().await.unwrap();
        let received_snapshot2 = output_rx.recv().await.unwrap();
        let received_update2 = output_rx.recv().await.unwrap();

        verify_snapshot(received_snapshot1, 100);
        verify_update(received_update1, 101, 105);
        verify_snapshot(received_snapshot2, 200);
        verify_update(received_update2, 201, 205);
    }

    #[tokio::test]
    async fn test_depth_event_dispatcher_empty_buffer_after_filtering() {
        let (input_tx, mut output_rx, _handle) = setup_test().await;
        
        let snapshot = make_snapshot(100);
        let old_update = make_update(95, 100);
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(old_update)).await.unwrap();
        
        let received_snapshot = output_rx.recv().await.unwrap();
        verify_snapshot(received_snapshot, 100);
        
        tokio::select! {
            _ = sleep(Duration::from_millis(100)) => {}
            _ = output_rx.recv() => {
                panic!("Received unexpected update");
            }
        }
    }
    
    #[tokio::test]
    async fn test_depth_event_dispatcher_updates_before_snapshot() {
        let (input_tx, mut output_rx, _handle) = setup_test().await;
        
        let update1 = make_update(95, 99);
        let update2 = make_update(101, 105);
        let snapshot = make_snapshot(100);
        
        input_tx.send(MarketEvent::DepthUpdate(update1)).await.unwrap();
        input_tx.send(MarketEvent::DepthUpdate(update2)).await.unwrap();
        
        tokio::select! {
            _ = sleep(Duration::from_millis(100)) => {}
            _ = output_rx.recv() => {
                panic!("Received unexpected update before snapshot");
            }
        }
        
        input_tx.send(MarketEvent::DepthSnapshot(snapshot)).await.unwrap();
        
        let received_snapshot = output_rx.recv().await.unwrap();
        let received_update = output_rx.recv().await.unwrap();
        
        verify_snapshot(received_snapshot, 100);
        verify_update(received_update, 101, 105);
    }
}
