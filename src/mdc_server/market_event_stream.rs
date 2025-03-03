use std::error::Error;
use tokio_tungstenite::{connect_async, tungstenite};
use futures::{StreamExt, SinkExt};
use tokio::sync::mpsc;
use std::time::Duration;
use tokio::time::sleep;
use anyhow::Result;
use tungstenite::{Bytes, Message};
use tungstenite::protocol::CloseFrame;
use std::marker::PhantomData;
use crate::mdc_server::models::{MarketEvent, MarketEventSource};

/// A WebSocket client that connects to a market data stream and forwards events to a processing queue.
///
/// This struct maintains a persistent WebSocket connection to a specified URL, processes incoming
/// messages according to the generic type parameter `T`, and forwards the parsed events to an
/// event queue for further processing. It automatically handles reconnection in case of connection
/// failures.
///
/// The generic type parameter `T` must implement the `MarketEventSource` trait, which defines
/// how to parse JSON messages from the WebSocket stream into domain-specific event types.
pub struct MarketEventStream<T>
where T: MarketEventSource,
{
    url: String,
    event_queue: mpsc::Sender<MarketEvent>,
    reconnect_timeout: u64,
    _phantom: PhantomData<T>,
}

impl<T> MarketEventStream<T>
where T: MarketEventSource,
{
    /// Creates a new `MarketEventStream` instance.
    ///
    /// # Arguments
    /// * `url` - The WebSocket endpoint URL to connect to
    /// * `event_queue` - Channel for sending parsed market events to the processing pipeline
    /// * `reconnect_timeout` - Timeout in milliseconds to wait before attempting to reconnect after a connection failure
    ///
    /// # Returns
    /// A new `MarketEventStream` instance configured with the provided parameters
    pub fn new(url: String, event_queue: mpsc::Sender<MarketEvent>, reconnect_timeout: u64) -> Self {
        Self {
            url,
            event_queue,
            reconnect_timeout,
            _phantom: PhantomData,
        }
    }
    
    /// Starts the WebSocket connection and begins processing messages.
    ///
    /// This method runs in an infinite loop, maintaining the WebSocket connection
    /// and processing incoming messages. If the connection fails, it will automatically
    /// attempt to reconnect after the configured timeout period.
    ///
    /// This method does not return under normal circumstances and should typically
    /// be spawned as a separate task.
    pub async fn run(&mut self) {
        loop {
            match self.run_session().await {
                Ok(_) => {
                    tracing::trace!("Session '{}' finished", self.url);
                }
                Err(e) => {
                    tracing::error!("Session '{}' finished with error: '{}'. Reconnecting in '{}' ms", self.url, e, self.reconnect_timeout);
                    sleep(Duration::from_millis(self.reconnect_timeout)).await;
                }
            }
        }
    }
    
    /// Runs a single WebSocket session until completion or error.
    ///
    /// This method establishes a WebSocket connection, processes messages until
    /// the connection is closed or an error occurs, and then returns.
    ///
    /// # Returns
    /// * `Ok(())` if the session completed normally
    /// * `Err(...)` if an error occurred during the session
    async fn run_session(&mut self) -> Result<()> {
        let (ws_stream, _) = connect_async(&self.url).await?;
        let (mut ws_writer, mut ws_reader) = ws_stream.split();

        while let Some(msg) = ws_reader.next().await {
            tracing::trace!("Received message: '{:?}'", msg);
            
            match msg {
                Ok(Message::Text(text)) => { self.on_message(&text).await?; }
                Ok(Message::Ping(payload)) => { self.on_ping(&mut ws_writer, &payload).await?; }
                Ok(Message::Close(frame)) => { self.on_close(frame).await?; }
                Err(e) => { return Err(e.into()); }
                _ => {}
            }
        }
        Ok(())
    }
    
    /// Processes a text message received from the WebSocket.
    ///
    /// This method parses the JSON message into a domain-specific event type using
    /// the `MarketEventSource` implementation of type `T`, then forwards the event
    /// to the processing queue.
    ///
    /// # Arguments
    /// * `message` - The text message received from the WebSocket
    ///
    /// # Returns
    /// * `Ok(())` if the message was processed successfully
    /// * `Err(...)` if an error occurred during processing
    async fn on_message(&mut self, message: &str) -> Result<()> {
        let event = T::from_json(&message)?;
        tracing::trace!("Received market event: '{:?}'", event);
        self.event_queue.send(event.into_market_event()).await?;
        Ok(())
    }

    /// Responds to a ping message from the WebSocket server.
    ///
    /// This method sends a pong response with the same payload as the ping message,
    /// as required by the WebSocket protocol.
    ///
    /// # Arguments
    /// * `ws_writer` - The WebSocket writer to send the pong response
    /// * `payload` - The payload from the ping message
    ///
    /// # Returns
    /// * `Ok(())` if the pong was sent successfully
    /// * `Err(...)` if an error occurred while sending the pong
    async fn on_ping<S>(&mut self, ws_writer: &mut S, payload: &Bytes) -> Result<()> 
    where S: SinkExt<Message> + Unpin,
          <S as futures::Sink<Message>>::Error: Error + Send + Sync + 'static
    {
        tracing::trace!("Received ping message. payload: {:?}", payload);
        ws_writer.send(Message::Pong(payload.clone())).await?;
        Ok(())
    }

    /// Handles a close message from the WebSocket server.
    ///
    /// This method logs the close frame and returns, allowing the session to terminate
    /// gracefully. The `run` method will then handle reconnection if appropriate.
    ///
    /// # Arguments
    /// * `frame` - The close frame received from the server, if any
    ///
    /// # Returns
    /// * `Ok(())` always, as this is considered a normal termination
    async fn on_close(&mut self, frame: Option<CloseFrame>) -> Result<()> {
        tracing::trace!("Channel was closed: {:?}", frame);
        Ok(())
    }
}
