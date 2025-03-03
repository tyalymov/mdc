use serde::de;
use serde::{Deserialize, Deserializer};
use std::fmt;
use chrono::{TimeZone, Utc};

pub trait FromJson: Sized {
    fn from_json(s: &str) -> Result<Self, serde_json::Error>;
}

impl<T> FromJson for T where T: de::DeserializeOwned,
{
    fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

pub fn de_float_from_str<'a, D>(deserializer: D) -> Result<f64, D::Error>
where D: Deserializer<'a>,
{
    let str_val = String::deserialize(deserializer)?;
    str_val.parse::<f64>().map_err(de::Error::custom)
}

impl<'de> Deserialize<'de> for DepthEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let arr: Vec<String> = Vec::deserialize(deserializer)?;
        if arr.len() != 2 {
            return Err(de::Error::invalid_length(arr.len(), &"2"));
        }

        let price = arr[0]
            .parse::<f64>()
            .map_err(de::Error::custom)?;
        let quantity = arr[1]
            .parse::<f64>()
            .map_err(de::Error::custom)?;

        Ok(DepthEntry { price, quantity })
    }
}

#[derive(Debug, Clone)]
pub struct DepthEntry {
    pub price: f64,
    pub quantity: f64,
}

impl fmt::Display for DepthEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Price: '{}', Quantity: '{}'",
            self.price,
            self.quantity,
        )
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<DepthEntry>,
    pub asks: Vec<DepthEntry>,
}

impl fmt::Display for DepthSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Last update id: '{}'",
            self.last_update_id,
        )
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DepthUpdate {
    #[serde(rename = "e")]
    #[allow(dead_code)]
    pub event_type: String,
    #[serde(rename = "E")]
    #[allow(dead_code)]
    pub event_time: u64,
    #[serde(rename = "s")]
    #[allow(dead_code)]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub last_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<DepthEntry>,
    #[serde(rename = "a")]
    pub asks: Vec<DepthEntry>,
}

impl fmt::Display for DepthUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Symbol: '{}', First: '{}', Last: '{}', Time: '{}'",
            self.symbol,
            self.first_update_id,
            self.last_update_id,
            self.event_time,
        )
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TradeEvent {
    #[serde(rename = "e")]
    #[allow(dead_code)]
    pub event_type: String,
    #[serde(rename = "E")]
    #[allow(dead_code)]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub trade_id: u64, 
    #[serde(rename = "p", deserialize_with = "de_float_from_str")]
    pub price: f64,
    #[serde(rename = "q", deserialize_with = "de_float_from_str")]
    pub quantity: f64,
    #[serde(rename = "T")]
    pub trade_time: u64,
    #[serde(rename = "m")]
    #[allow(dead_code)]
    pub is_market_maker: bool,
    #[serde(rename = "M")]
    #[allow(dead_code)]
    pub ignore: bool,
}

impl fmt::Display for TradeEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Id: '{}', 'Symbol: '{}', Price: '{}', Quantity: '{}', Time: '{}'",
            self.trade_id,
            self.symbol,
            self.price,
            self.quantity,
            Utc.timestamp_millis_opt(self.trade_time as i64)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S%.3f")
        )
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct PriceUpdate {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b", deserialize_with = "de_float_from_str")]
    pub best_bid_price: f64,
    #[serde(rename = "B", deserialize_with = "de_float_from_str")]
    pub best_bid_quantity: f64,
    #[serde(rename = "a", deserialize_with = "de_float_from_str")]
    pub best_ask_price: f64,
    #[serde(rename = "A", deserialize_with = "de_float_from_str")]
    pub best_ask_quantity: f64,
}

impl fmt::Display for PriceUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Id: '{}', Symbol: '{}', Best bid - (price: '{}', quantity: '{}'), Best ask - (price: '{}' quantity: '{}')",
            self.update_id,
            self.symbol,
            self.best_bid_price,
            self.best_bid_quantity,
            self.best_ask_price,
            self.best_ask_quantity
        )
    }
}

/// An enum that can hold any of the market data types
#[derive(Debug, Clone)]
pub enum MarketEvent {
    DepthSnapshot(DepthSnapshot),
    DepthUpdate(DepthUpdate),
    TradeEvent(TradeEvent),
    PriceUpdate(PriceUpdate),
}

impl fmt::Display for MarketEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarketEvent::DepthSnapshot(ds) => write!(f, "DepthSnapshot: '{}'", ds),
            MarketEvent::DepthUpdate(du) => write!(f, "DepthUpdate: '{}'", du),
            MarketEvent::TradeEvent(te) => write!(f, "TradeEvent: '{}'", te),
            MarketEvent::PriceUpdate(pu) => write!(f, "PriceUpdate: '{}'", pu),
        }
    }
}

/// Trait for types that can be converted to a MarketEvent
pub trait IntoMarketEvent {
    fn into_market_event(self) -> MarketEvent;
}

/// Trait that combines all requirements for a type that can be used as a source for MarketEvent
pub trait MarketEventSource: FromJson + Send + Sync + std::fmt::Debug + IntoMarketEvent + 'static {}

// Implement MarketEventSource for all types that satisfy the requirements
impl<T> MarketEventSource for T 
where 
    T: FromJson + Send + Sync + std::fmt::Debug + IntoMarketEvent + 'static
{}

impl IntoMarketEvent for DepthSnapshot {
    fn into_market_event(self) -> MarketEvent {
        MarketEvent::DepthSnapshot(self)
    }
}

impl IntoMarketEvent for DepthUpdate {
    fn into_market_event(self) -> MarketEvent {
        MarketEvent::DepthUpdate(self)
    }
}

impl IntoMarketEvent for TradeEvent {
    fn into_market_event(self) -> MarketEvent {
        MarketEvent::TradeEvent(self)
    }
}

impl IntoMarketEvent for PriceUpdate {
    fn into_market_event(self) -> MarketEvent {
        MarketEvent::PriceUpdate(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn test_depth_entry_parsing() {
        let json_data = r#"
        [
            "123.45",
            "67.89"
        ]
        "#;
        
        let parsed : DepthEntry = DepthEntry::from_json(json_data).unwrap();
        assert_eq!(parsed.price, 123.45);
        assert_eq!(parsed.quantity, 67.89);
    }

    #[test]
    fn test_depth_snapshot_parsing() {
        let json_data = r#"
        {
            "lastUpdateId": 123456,
            "bids": [
                [ "123.45", "10.5" ],
                [ "122.99", "8.0"  ]
            ],
            "asks": [
                [ "124.45", "2.2" ]
            ]
        }
        "#;

        let parsed: DepthSnapshot = DepthSnapshot::from_json(json_data).unwrap();
        assert_eq!(parsed.last_update_id, 123456);

        assert_eq!(parsed.bids.len(), 2);
        assert_eq!(parsed.bids[0].price, 123.45);
        assert_eq!(parsed.bids[0].quantity, 10.5);
        assert_eq!(parsed.bids[1].price, 122.99);
        assert_eq!(parsed.bids[1].quantity, 8.0);

        assert_eq!(parsed.asks.len(), 1);
        assert_eq!(parsed.asks[0].price, 124.45);
        assert_eq!(parsed.asks[0].quantity, 2.2);
    }

    #[test]
    fn test_depth_update_parsing() {
        let json_data = r#"
        {
            "e": "depthUpdate",
            "E": 1672515782136,
            "s": "BNBBTC",
            "U": 157,
            "u": 160,
            "b": [
                [
                  "0.0024",
                  "10"
                ]
              ],
              "a": [
                [
                  "0.0026",
                  "100"
                ]
              ]
        }
        "#;

        let parsed: DepthUpdate = DepthUpdate::from_json(json_data).unwrap();
        assert_eq!(parsed.event_type, "depthUpdate");
        assert_eq!(parsed.event_time, 1672515782136);
        assert_eq!(parsed.symbol, "BNBBTC");
        assert_eq!(parsed.first_update_id, 157);
        assert_eq!(parsed.last_update_id, 160);
        assert_eq!(parsed.bids[0].price, 0.0024);
        assert_eq!(parsed.bids[0].quantity, 10.0);
        assert_eq!(parsed.asks[0].price, 0.0026);
        assert_eq!(parsed.asks[0].quantity, 100.0);
    }
    
    #[test]
    fn test_trade_event_parsing() {
        let json_data = r#"
        {
            "e": "trade",
            "E": 1675858459000,
            "s": "BTCUSDT",
            "t": 10003456,
            "p": "23456.78",
            "q": "0.00123",
            "T": 1675858460001,
            "m": true,
            "M": false
        }
        "#;

        let parsed: TradeEvent = TradeEvent::from_json(json_data).unwrap();
        assert_eq!(parsed.event_type, "trade");
        assert_eq!(parsed.event_time, 1675858459000);
        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.trade_id, 10003456);
        assert_eq!(parsed.price, 23456.78);
        assert_eq!(parsed.quantity, 0.00123);
        assert_eq!(parsed.trade_time, 1675858460001);
        assert_eq!(parsed.is_market_maker, true);
        assert_eq!(parsed.ignore, false);
    }

    #[test]
    fn test_price_update_parsing() {
        let json_data = r#"
        {
            "u": 555555,
            "s": "ETHBTC",
            "b": "0.06789",
            "B": "120",
            "a": "0.06795",
            "A": "98.5"
        }
        "#;

        let parsed: PriceUpdate = PriceUpdate::from_json(json_data).unwrap();
        assert_eq!(parsed.update_id, 555555);
        assert_eq!(parsed.symbol, "ETHBTC");
        assert_eq!(parsed.best_bid_price, 0.06789);
        assert_eq!(parsed.best_bid_quantity, 120.0);
        assert_eq!(parsed.best_ask_price, 0.06795);
        assert_eq!(parsed.best_ask_quantity, 98.5);
    }

    #[test]
    fn test_market_event_enum() {
        // Create instances of each type
        let depth_snapshot = DepthSnapshot {
            last_update_id: 123456,
            bids: vec![DepthEntry { price: 100.0, quantity: 10.0 }],
            asks: vec![DepthEntry { price: 101.0, quantity: 5.0 }],
        };

        let depth_update = DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1672515782136,
            symbol: "BTCUSDT".to_string(),
            first_update_id: 157,
            last_update_id: 160,
            bids: vec![DepthEntry { price: 100.0, quantity: 10.0 }],
            asks: vec![DepthEntry { price: 101.0, quantity: 5.0 }],
        };

        let trade_event = TradeEvent {
            event_type: "trade".to_string(),
            event_time: 1675858459000,
            symbol: "BTCUSDT".to_string(),
            trade_id: 10003456,
            price: 23456.78,
            quantity: 0.00123,
            trade_time: 1675858460001,
            is_market_maker: true,
            ignore: false,
        };

        let price_update = PriceUpdate {
            update_id: 555555,
            symbol: "ETHBTC".to_string(),
            best_bid_price: 0.06789,
            best_bid_quantity: 120.0,
            best_ask_price: 0.06795,
            best_ask_quantity: 98.5,
        };

        // Convert to MarketEvent using IntoMarketEvent trait
        let market_event1 = depth_snapshot.into_market_event();
        let market_event2 = depth_update.into_market_event();
        let market_event3 = trade_event.into_market_event();
        let market_event4 = price_update.into_market_event();

        // Check that they match the expected variants
        match market_event1 {
            MarketEvent::DepthSnapshot(_) => (),
            _ => panic!("Expected DepthSnapshot variant"),
        }

        match market_event2 {
            MarketEvent::DepthUpdate(_) => (),
            _ => panic!("Expected DepthUpdate variant"),
        }

        match market_event3 {
            MarketEvent::TradeEvent(_) => (),
            _ => panic!("Expected TradeEvent variant"),
        }

        match market_event4 {
            MarketEvent::PriceUpdate(_) => (),
            _ => panic!("Expected PriceUpdate variant"),
        }
    }

    #[test]
    fn test_market_event_source_trait() {
        // This test verifies that our types implement MarketEventSource
        fn assert_market_event_source<T: MarketEventSource>() {}
        
        // These should compile if the types implement MarketEventSource
        assert_market_event_source::<DepthSnapshot>();
        assert_market_event_source::<DepthUpdate>();
        assert_market_event_source::<TradeEvent>();
        assert_market_event_source::<PriceUpdate>();
    }

    #[test]
    fn test_mpsc_queue_with_market_events() {
        // Create a channel
        let (tx, rx) = mpsc::channel::<MarketEvent>();

        // Create instances of each type
        let depth_snapshot = DepthSnapshot {
            last_update_id: 123456,
            bids: vec![DepthEntry { price: 100.0, quantity: 10.0 }],
            asks: vec![DepthEntry { price: 101.0, quantity: 5.0 }],
        };

        let trade_event = TradeEvent {
            event_type: "trade".to_string(),
            event_time: 1675858459000,
            symbol: "BTCUSDT".to_string(),
            trade_id: 10003456,
            price: 23456.78,
            quantity: 0.00123,
            trade_time: 1675858460001,
            is_market_maker: true,
            ignore: false,
        };

        // Send events to the channel
        tx.send(depth_snapshot.into_market_event()).unwrap();
        tx.send(trade_event.into_market_event()).unwrap();

        // Receive and process events
        let mut depth_snapshot_count = 0;
        let mut trade_event_count = 0;

        for _ in 0..2 {
            match rx.recv().unwrap() {
                MarketEvent::DepthSnapshot(snapshot) => {
                    depth_snapshot_count += 1;
                    assert_eq!(snapshot.last_update_id, 123456);
                },
                MarketEvent::TradeEvent(trade) => {
                    trade_event_count += 1;
                    assert_eq!(trade.trade_id, 10003456);
                },
                _ => panic!("Unexpected event type"),
            }
        }

        assert_eq!(depth_snapshot_count, 1);
        assert_eq!(trade_event_count, 1);
    }
}
