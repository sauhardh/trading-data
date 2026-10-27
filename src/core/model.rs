use super::side::Side;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceLevel {
    pub price: i64,
    pub quantity: i64,
}

#[derive(Debug, Clone)]
pub struct NormalizedSnapshot {
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub seq: i64,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub clock: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedDelta {
    pub symbol: String,
    pub side: Side,
    pub price_level: i64,
    pub delta_quantity: i64,
    pub seq: i64,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub clock: i64,
}

#[derive(Debug, Clone)]
pub enum ExchangeMessage {
    Snapshot(NormalizedSnapshot),
    Delta(NormalizedDelta),
    Subscribed { sid: i64 },
    Heartbeat,
    Ignored,
}
