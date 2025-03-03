use std::collections::BTreeMap;
use std::cmp::Ordering;
use std::fmt;
use crate::mdc_server::models::DepthSnapshot;

/// Represents a price level in the order book, distinguishing between bid and ask prices.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PriceKey {
    Bid(f64),
    Ask(f64),
}

impl PriceKey {
    /// Returns the underlying price value regardless of whether it's a bid or ask.
    pub fn price(&self) -> f64 {
        match self {
            PriceKey::Bid(price) => *price,
            PriceKey::Ask(price) => *price,
        }
    }
}

/// Implements custom ordering logic for `PriceKey` values:
/// - Bids are sorted in descending order (highest price first)
/// - Asks are sorted in ascending order (lowest price first)
/// - Comparing a bid with an ask (or vice versa) returns `None`
impl PartialOrd for PriceKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (PriceKey::Bid(a), PriceKey::Bid(b)) => b.partial_cmp(a),
            (PriceKey::Ask(a), PriceKey::Ask(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Eq for PriceKey {}

/// Extends the `PartialOrd` implementation to provide a total ordering for `PriceKey`.
impl Ord for PriceKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// A data structure that maintains the state of an order book, tracking bid and ask orders at various price levels.
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: BTreeMap<PriceKey, f64>,
    pub asks: BTreeMap<PriceKey, f64>,
}

/// Implements the `Display` trait for `OrderBook` to provide a human-readable representation.
impl fmt::Display for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut formatted_string = String::from("BOOK:\n");

        formatted_string.push_str("BIDS:\n");
        for (key, qty) in self.bids.iter() {
            formatted_string.push_str(&format!("  Price: '{}', Quantity: '{}'\n", key.price(), qty));
        }

        formatted_string.push_str("------------------------------------\n");

        formatted_string.push_str("ASKS:\n");
        for (key, qty) in self.asks.iter() {
            formatted_string.push_str(&format!("  Price: '{}', Quantity: '{}'\n", key.price(), qty));
        }

        write!(f, "{}", formatted_string)
    }
}

impl OrderBook {
    /// Creates a new `OrderBook` from a depth snapshot.
    ///
    /// # Arguments
    /// * `snapshot` - A reference to a `DepthSnapshot` containing initial bids and asks
    ///
    /// # Returns
    /// A new `OrderBook` instance populated with the bids and asks from the snapshot
    pub fn new(snapshot: &DepthSnapshot) -> Self {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        
        for entry in &snapshot.bids {
            bids.insert(PriceKey::Bid(entry.price), entry.quantity);
        }
        
        for entry in &snapshot.asks {
            asks.insert(PriceKey::Ask(entry.price), entry.quantity);
        }

        OrderBook { bids, asks }
    }

    /// Apply an update to the order book
    /// 
    /// # Arguments
    /// * `price_key` - The price key (Bid or Ask) with the price level to update
    /// * `quantity` - The new quantity at this price level
    /// 
    /// # Behavior
    /// * If quantity = 0, the price level will be removed
    /// * If the price level doesn't exist, it will be created
    /// * If the price level exists, it will be updated
    pub fn apply_update(&mut self, price_key: PriceKey, quantity: f64) {
        let book = match price_key {
            PriceKey::Bid(_) => &mut self.bids,
            PriceKey::Ask(_) => &mut self.asks,
        };

        if quantity == 0.0 {
            book.remove(&price_key);
            return;
        }

        book.insert(price_key, quantity);
    }

    /// Helper method to create a bid price key.
    ///
    /// # Arguments
    /// * `price` - The price value for the bid
    ///
    /// # Returns
    /// A `PriceKey::Bid` variant with the specified price
    pub fn bid(price: f64) -> PriceKey {
        PriceKey::Bid(price)
    }

    /// Helper method to create an ask price key.
    ///
    /// # Arguments
    /// * `price` - The price value for the ask
    ///
    /// # Returns
    /// A `PriceKey::Ask` variant with the specified price
    pub fn ask(price: f64) -> PriceKey {
        PriceKey::Ask(price)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mdc_server::models::DepthEntry;

    #[test]
    fn test_new_order_book() {
        let snapshot = DepthSnapshot {
            last_update_id: 123456,
            bids: vec![
                DepthEntry { price: 100.0, quantity: 10.0 },
                DepthEntry { price: 99.5, quantity: 15.0 },
            ],
            asks: vec![
                DepthEntry { price: 100.5, quantity: 5.0 },
                DepthEntry { price: 101.0, quantity: 8.0 },
            ],
        };
        
        let order_book = OrderBook::new(&snapshot);
        
        assert_eq!(order_book.bids.len(), 2);
        assert_eq!(order_book.bids.get(&PriceKey::Bid(100.0)), Some(&10.0));
        assert_eq!(order_book.bids.get(&PriceKey::Bid(99.5)), Some(&15.0));
        
        assert_eq!(order_book.asks.len(), 2);
        assert_eq!(order_book.asks.get(&PriceKey::Ask(100.5)), Some(&5.0));
        assert_eq!(order_book.asks.get(&PriceKey::Ask(101.0)), Some(&8.0));
    }

    #[test]
    fn test_apply_update_new_level() {
        let mut order_book = OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        
        order_book.apply_update(OrderBook::bid(100.0), 10.0);
        assert_eq!(order_book.bids.get(&PriceKey::Bid(100.0)), Some(&10.0));
        
        order_book.apply_update(OrderBook::ask(101.0), 5.0);
        assert_eq!(order_book.asks.get(&PriceKey::Ask(101.0)), Some(&5.0));
    }

    #[test]
    fn test_apply_update_existing_level() {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        bids.insert(PriceKey::Bid(100.0), 10.0);
        asks.insert(PriceKey::Ask(101.0), 5.0);

        let mut order_book = OrderBook { bids, asks };
        
        order_book.apply_update(PriceKey::Bid(100.0), 15.0);
        assert_eq!(order_book.bids.get(&PriceKey::Bid(100.0)), Some(&15.0));
        
        order_book.apply_update(PriceKey::Ask(101.0), 8.0);
        assert_eq!(order_book.asks.get(&PriceKey::Ask(101.0)), Some(&8.0));
    }

    #[test]
    fn test_apply_update_remove_level() {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        bids.insert(PriceKey::Bid(100.0), 10.0);
        bids.insert(PriceKey::Bid(99.5), 15.0);
        asks.insert(PriceKey::Ask(101.0), 5.0);
        asks.insert(PriceKey::Ask(102.0), 8.0);

        let mut order_book = OrderBook { bids, asks };
        
        order_book.apply_update(PriceKey::Bid(100.0), 0.0);
        assert_eq!(order_book.bids.get(&PriceKey::Bid(100.0)), None);
        assert_eq!(order_book.bids.len(), 1);
        
        order_book.apply_update(PriceKey::Ask(101.0), 0.0);
        assert_eq!(order_book.asks.get(&PriceKey::Ask(101.0)), None);
        assert_eq!(order_book.asks.len(), 1);
    }

    #[test]
    fn test_apply_update_nonexistent_level_zero_quantity() {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        bids.insert(PriceKey::Bid(100.0), 10.0);
        asks.insert(PriceKey::Ask(101.0), 5.0);

        let mut order_book = OrderBook { bids, asks };
        
        order_book.apply_update(PriceKey::Bid(99.0), 0.0);
        assert_eq!(order_book.bids.len(), 1);
        
        order_book.apply_update(PriceKey::Ask(102.0), 0.0);
        assert_eq!(order_book.asks.len(), 1);
    }

    #[test]
    fn test_multiple_updates() {
        let mut order_book = OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        
        order_book.apply_update(OrderBook::bid(100.0), 10.0);
        order_book.apply_update(OrderBook::bid(99.0), 15.0);
        order_book.apply_update(OrderBook::ask(101.0), 5.0);
        order_book.apply_update(OrderBook::ask(102.0), 8.0);
        
        assert_eq!(order_book.bids.len(), 2);
        assert_eq!(order_book.asks.len(), 2);
        
        order_book.apply_update(OrderBook::bid(100.0), 20.0);
        order_book.apply_update(OrderBook::ask(101.0), 10.0);
        
        assert_eq!(order_book.bids.get(&PriceKey::Bid(100.0)), Some(&20.0));
        assert_eq!(order_book.asks.get(&PriceKey::Ask(101.0)), Some(&10.0));
        
        order_book.apply_update(OrderBook::bid(99.0), 0.0);
        order_book.apply_update(OrderBook::ask(102.0), 0.0);
        
        assert_eq!(order_book.bids.len(), 1);
        assert_eq!(order_book.asks.len(), 1);
        assert_eq!(order_book.bids.get(&PriceKey::Bid(99.0)), None);
        assert_eq!(order_book.asks.get(&PriceKey::Ask(102.0)), None);
    }

    #[test]
    fn test_bid_ordering() {
        let mut order_book = OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        
        order_book.apply_update(OrderBook::bid(100.0), 10.0);
        order_book.apply_update(OrderBook::bid(102.0), 5.0);
        order_book.apply_update(OrderBook::bid(99.0), 15.0);
        order_book.apply_update(OrderBook::bid(101.0), 8.0);
        
        let bid_prices: Vec<f64> = order_book
            .bids
            .keys()
            .map(|k| k.price())
            .collect();
        
        assert_eq!(bid_prices, vec![102.0, 101.0, 100.0, 99.0]);
    }

    #[test]
    fn test_ask_ordering() {
        let mut order_book = OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        
        order_book.apply_update(OrderBook::ask(100.0), 10.0);
        order_book.apply_update(OrderBook::ask(102.0), 5.0);
        order_book.apply_update(OrderBook::ask(99.0), 15.0);
        order_book.apply_update(OrderBook::ask(101.0), 8.0);
        
        let ask_prices: Vec<f64> = order_book
            .asks
            .keys()
            .map(|k| k.price())
            .collect();
        
        assert_eq!(ask_prices, vec![99.0, 100.0, 101.0, 102.0]);
    }

    #[test]
    fn test_price_key_helpers() {
        let bid_key = OrderBook::bid(100.0);
        let ask_key = OrderBook::ask(100.0);
        
        assert!(matches!(bid_key, PriceKey::Bid(100.0)));
        assert!(matches!(ask_key, PriceKey::Ask(100.0)));
        
        assert_eq!(bid_key.price(), 100.0);
        assert_eq!(ask_key.price(), 100.0);
    }
}
