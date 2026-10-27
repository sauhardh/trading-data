use super::model::{NormalizedDelta, NormalizedSnapshot};
use super::side::Side;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

use chrono::{DateTime, Utc};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub symbol: String,
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
    pub last_clock: i64,
}

impl OrderBook {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_clock: 0,
        }
    }

    pub fn apply_snapshot(&mut self, snap: &NormalizedSnapshot) {
        self.bids.clear();
        self.asks.clear();

        for l in &snap.bids {
            if l.quantity > 0 {
                self.bids.insert(l.price, l.quantity);
            }
        }

        for l in &snap.asks {
            if l.quantity > 0 {
                self.asks.insert(l.price, l.quantity);
            }
        }

        self.last_clock = snap.clock;
    }

    pub fn apply_delta(&mut self, delta: &NormalizedDelta) -> i64 {
        let side = match delta.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        let entry = side.entry(delta.price_level).or_insert(0);
        *entry += delta.delta_quantity;

        if *entry <= 0 {
            side.remove(&delta.price_level);
            0
        } else {
            self.last_clock = self.last_clock.max(delta.clock);
            *side.get(&delta.price_level).unwrap()
        }
    }

    pub fn to_sbe_snapshot(&self, now: DateTime<Utc>) -> SnapshotDomain {
        SnapshotDomain {
            received_at: now,
            exchange_timestamp: now,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&p, &q)| SnapshotBidsEntryDomain {
                    price_level: p,
                    quantity: q,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&p, &q)| SnapshotAsksEntryDomain {
                    price_level: p,
                    quantity: q,
                })
                .collect(),
            symbol: self.symbol.clone(),
        }
    }

    pub fn delta_to_sbe(&self, delta: &NormalizedDelta, abs_qty: i64) -> PriceLevelUpdateDomain {
        PriceLevelUpdateDomain {
            backtest_timestamp: delta.received_at,
            exchange_timestamp: delta.exchange_timestamp.unwrap_or(delta.received_at),
            price_level: delta.price_level,
            quantity: abs_qty,
            side: delta.side,
            symbol: delta.symbol.clone(),
        }
    }
}
