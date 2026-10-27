use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::fmt;

use crate::core::{ExchangeMessage, NormalizedDelta, NormalizedSnapshot, PriceLevel, Side};

const SCALE: u64 = 1_000_000;
const POW10: [u64; 7] = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    Empty,
    Invalid,
    TooPrecise,
    Overflow,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ParseError::Empty => "empty fixed-point value",
            ParseError::Invalid => "invalid fixed-point value",
            ParseError::TooPrecise => "more than 6 decimal places",
            ParseError::Overflow => "fixed-point overflow",
        })
    }
}

impl std::error::Error for ParseError {}

#[inline]
pub fn parse_fixed_6(s: &str) -> Result<i64, ParseError> {
    let b = s.as_bytes();

    let (neg, mut i) = match b.first() {
        None => return Err(ParseError::Empty),
        Some(b'-') => (true, 1),
        Some(b'+') => (false, 1),
        _ => (false, 0),
    };

    let start = i;
    let mut whole: u64 = 0;
    while i < b.len() {
        let d = b[i].wrapping_sub(b'0');
        if d > 9 {
            break;
        }
        whole = whole * 10 + d as u64;
        i += 1;
        if i - start > 19 {
            return Err(ParseError::Overflow);
        }
    }
    if i == start {
        return Err(ParseError::Invalid);
    }

    // Fractional part
    let mut frac: u64 = 0;
    let mut frac_digits = 0usize;
    if i < b.len() {
        if b[i] != b'.' {
            return Err(ParseError::Invalid);
        }
        i += 1;
        if i == b.len() {
            return Err(ParseError::Invalid); // reject "1."
        }
        while i < b.len() {
            let d = b[i].wrapping_sub(b'0');
            if d > 9 {
                return Err(ParseError::Invalid);
            }
            if frac_digits < 6 {
                frac = frac * 10 + d as u64;
                frac_digits += 1;
            } else if d != 0 {
                return Err(ParseError::TooPrecise); // allow trailing zeros only
            }
            i += 1;
        }
    }
    frac *= POW10[6 - frac_digits];

    let mag = whole
        .checked_mul(SCALE)
        .and_then(|v| v.checked_add(frac))
        .ok_or(ParseError::Overflow)?;

    if neg {
        if mag > i64::MAX as u64 + 1 {
            return Err(ParseError::Overflow);
        }
        Ok((mag as i64).wrapping_neg()) // handles i64::MIN correctly
    } else if mag > i64::MAX as u64 {
        Err(ParseError::Overflow)
    } else {
        Ok(mag as i64)
    }
}

#[inline]
pub fn parse_price(s: &str) -> Result<i64, ParseError> {
    parse_fixed_6(s)
}

#[inline]
pub fn parse_quantity(s: &str) -> Result<i64, ParseError> {
    let quantity = parse_fixed_6(s)?;

    if quantity < 0 {
        return Err(ParseError::Invalid);
    }

    Ok(quantity)
}

#[inline]
pub fn parse_levels(levels: &[[String; 2]]) -> Result<Vec<PriceLevel>> {
    levels
        .iter()
        .map(|[price, quantity]| {
            Ok(PriceLevel {
                price: parse_price(price)?,
                quantity: parse_quantity(quantity)?,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
pub struct KalshiSnapshot {
    #[serde(rename = "type")]
    msg_type: String,
    sid: i64,
    seq: i64,
    sending_ts_ms: Option<i64>,

    msg: KalshiSnapshotMessage,
}

#[derive(Debug, Deserialize)]
pub struct KalshiSnapshotMessage {
    market_id: String,
    market_ticker: String,
    clock: Option<i64>,
    #[serde(default)]
    yes_dollars_fp: Vec<[String; 2]>,
    #[serde(default)]
    no_dollars_fp: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
pub struct KalshiDelta {
    #[serde(rename = "type")]
    msg_type: String,
    sid: i64,
    seq: i64,
    sending_ts_ms: Option<i64>,

    msg: KalshiDeltaMessage,
}

#[derive(Debug, Deserialize)]
pub struct KalshiDeltaMessage {
    market_id: String,
    market_ticker: String,
    clock: Option<i64>,
    side: String,
    price_dollars: String,
    delta_fp: String,
    ts_ms: Option<i64>,
}

pub fn parse_kalshi_frame(raw: &str, received_at: DateTime<Utc>) -> Result<Vec<ExchangeMessage>> {
    let value: serde_json::Value =
        serde_json::from_str(raw).context("Failed to parse frame JSON")?;

    let msg_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");

    match msg_type {
        "orderbook_snapshot" => {
            let frame: KalshiSnapshot =
                serde_json::from_value(value).context("Failed to parse Kalshi snapshot")?;

            parse_snapshot(frame, received_at)
        }
        "orderbook_delta" => {
            let frame: KalshiDelta =
                serde_json::from_value(value).context("Failed to parse Kalshi delta")?;

            parse_delta(frame, received_at)
        }
        _ => Ok(vec![ExchangeMessage::Ignored]),
    }
}

pub fn parse_snapshot(
    frame: KalshiSnapshot,
    received_at: DateTime<Utc>,
) -> Result<Vec<ExchangeMessage>> {
    let bids = parse_levels(&frame.msg.yes_dollars_fp)?;
    let asks = parse_levels(&frame.msg.no_dollars_fp)?;

    let exchange_ts = frame
        .sending_ts_ms
        .and_then(DateTime::from_timestamp_millis);

    Ok(vec![ExchangeMessage::Snapshot(NormalizedSnapshot {
        symbol: frame.msg.market_ticker,
        bids,
        asks,
        clock: frame.msg.clock.unwrap_or(0),
        seq: frame.seq,
        exchange_timestamp: exchange_ts,
        received_at,
    })])
}

pub fn parse_delta(frame: KalshiDelta, received_at: DateTime<Utc>) -> Result<Vec<ExchangeMessage>> {
    let side = match frame.msg.side.as_str() {
        "yes" => Side::Buy,
        "no" => Side::Sell,
        other => bail!("Unknown Kalshi Side: {other}"),
    };

    let price_level = parse_price(&frame.msg.price_dollars)?;
    let delta_quantity = parse_fixed_6(&frame.msg.delta_fp)?;
    let exchange_ts = frame.msg.ts_ms.and_then(DateTime::from_timestamp_millis);

    Ok(vec![ExchangeMessage::Delta(NormalizedDelta {
        symbol: frame.msg.market_ticker,
        side,
        price_level,
        delta_quantity,
        seq: frame.seq,
        exchange_timestamp: exchange_ts,
        received_at,
        clock: frame.msg.clock.unwrap_or(0),
    })])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_fixed_6() {
        // Integer and fractional values
        assert_eq!(parse_fixed_6("0").unwrap(), 0);
        assert_eq!(parse_fixed_6("1").unwrap(), 1_000_000);
        assert_eq!(parse_fixed_6("0.01").unwrap(), 10_000);
        assert_eq!(parse_fixed_6("0.9600").unwrap(), 960_000);
        assert_eq!(parse_fixed_6("1.5").unwrap(), 1_500_000);
        assert_eq!(parse_fixed_6("3473.19").unwrap(), 3_473_190_000);

        // Negative values: important for deltas
        assert_eq!(parse_fixed_6("-0.5").unwrap(), -500_000);
        assert_eq!(parse_fixed_6("-100.25").unwrap(), -100_250_000);

        // Extra trailing zeros are accepted
        assert_eq!(parse_fixed_6("1.123456000").unwrap(), 1_123_456);

        // Invalid input
        assert_eq!(parse_fixed_6(""), Err(ParseError::Empty));
        assert_eq!(parse_fixed_6("-"), Err(ParseError::Invalid));
        assert_eq!(parse_fixed_6(".5"), Err(ParseError::Invalid));
        assert_eq!(parse_fixed_6("1."), Err(ParseError::Invalid));
        assert_eq!(parse_fixed_6("abc"), Err(ParseError::Invalid));
        assert_eq!(parse_fixed_6("1.2.3"), Err(ParseError::Invalid));

        // Excess precision
        assert_eq!(parse_fixed_6("1.1234567"), Err(ParseError::TooPrecise));

        // Overflow
        assert_eq!(parse_fixed_6("9223372036855"), Err(ParseError::Overflow));
    }

    #[test]
    fn test_parse_price() {
        assert_eq!(parse_price("0.0100").unwrap(), 10_000);
        assert_eq!(parse_price("0.9600").unwrap(), 960_000);
        assert_eq!(parse_price("1.000000").unwrap(), 1_000_000);
    }

    #[test]
    fn test_parse_quantity() {
        assert_eq!(parse_quantity("0").unwrap(), 0);
        assert_eq!(parse_quantity("3473.19").unwrap(), 3_473_190_000);
        assert_eq!(parse_quantity("0.5").unwrap(), 500_000);

        // Snapshot quantities must be nonnegative
        assert_eq!(parse_quantity("-1"), Err(ParseError::Invalid));
        assert_eq!(parse_quantity("-0.5"), Err(ParseError::Invalid));
    }

    #[test]
    fn test_parse_levels() {
        let levels = [
            ["0.0100".to_string(), "3473.19".to_string()],
            ["0.0200".to_string(), "1410.78".to_string()],
        ];

        let parsed = parse_levels(&levels).unwrap();

        assert_eq!(parsed.len(), 2);

        assert_eq!(parsed[0].price, 10_000);
        assert_eq!(parsed[0].quantity, 3_473_190_000);

        assert_eq!(parsed[1].price, 20_000);
        assert_eq!(parsed[1].quantity, 1_410_780_000);

        // Invalid level must fail
        let invalid_levels = [["0.0100".to_string(), "-5.00".to_string()]];

        assert!(parse_levels(&invalid_levels).is_err());
    }
    #[test]
    fn test_parse_snapshot() {
        let raw = r#"{
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "sending_ts_ms": 1792912814531,
        "msg": {
            "market_id": "test-market",
            "market_ticker": "TEST-MARKET",
            "yes_dollars_fp": [["0.0100", "7703.98"]],
            "no_dollars_fp": [["0.0200", "3473.19"]]
        }
    }"#;

        let received_at = Utc::now();
        let messages = parse_kalshi_frame(raw, received_at).unwrap();

        assert_eq!(messages.len(), 1);

        match &messages[0] {
            ExchangeMessage::Snapshot(snapshot) => {
                assert_eq!(snapshot.symbol, "TEST-MARKET");
                assert_eq!(snapshot.seq, 1);

                assert_eq!(snapshot.bids.len(), 1);
                assert_eq!(snapshot.bids[0].price, 10_000);
                assert_eq!(snapshot.bids[0].quantity, 7_703_980_000);

                assert_eq!(snapshot.asks.len(), 1);
                assert_eq!(snapshot.asks[0].price, 20_000);
                assert_eq!(snapshot.asks[0].quantity, 3_473_190_000);

                assert!(snapshot.exchange_timestamp.is_some());
                assert_eq!(snapshot.received_at, received_at);
            }
            other => panic!("Expected Snapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_yes_delta() {
        let raw = r#"{
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 2,
        "sending_ts_ms": 1792912814538,
        "msg": {
            "market_id": "test-market",
            "market_ticker": "TEST-MARKET",
            "side": "yes",
            "price_dollars": "0.0600",
            "delta_fp": "333.00",
            "ts_ms": 1792912814531
        }
    }"#;

        let received_at = Utc::now();
        let messages = parse_kalshi_frame(raw, received_at).unwrap();

        assert_eq!(messages.len(), 1);

        match &messages[0] {
            ExchangeMessage::Delta(delta) => {
                assert_eq!(delta.symbol, "TEST-MARKET");
                assert_eq!(delta.side, Side::Buy);
                assert_eq!(delta.price_level, 60_000);
                assert_eq!(delta.delta_quantity, 333_000_000);
                assert_eq!(delta.seq, 2);

                assert!(delta.exchange_timestamp.is_some());
                assert_eq!(delta.received_at, received_at);
            }
            other => panic!("Expected Delta, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_no_delta() {
        let raw = r#"{
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 3,
        "sending_ts_ms": 1792912814538,
        "msg": {
            "market_id": "test-market",
            "market_ticker": "TEST-MARKET",
            "side": "no",
            "price_dollars": "0.0200",
            "delta_fp": "100.00",
            "ts_ms": 1792912814531
        }
    }"#;

        let messages = parse_kalshi_frame(raw, Utc::now()).unwrap();

        match &messages[0] {
            ExchangeMessage::Delta(delta) => {
                assert_eq!(delta.side, Side::Sell);
                assert_eq!(delta.price_level, 20_000);
                assert_eq!(delta.delta_quantity, 100_000_000);
                assert_eq!(delta.seq, 3);
            }
            other => panic!("Expected Delta, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_negative_delta_quantity() {
        let raw = r#"{
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 4,
        "sending_ts_ms": 1792912814538,
        "msg": {
            "market_id": "test-market",
            "market_ticker": "TEST-MARKET",
            "side": "yes",
            "price_dollars": "0.0600",
            "delta_fp": "-10.00",
            "ts_ms": 1792912814531
        }
    }"#;

        let messages = parse_kalshi_frame(raw, Utc::now()).unwrap();

        match &messages[0] {
            ExchangeMessage::Delta(delta) => {
                assert_eq!(delta.delta_quantity, -10_000_000);
            }
            other => panic!("Expected Delta, got {other:?}"),
        }
    }

    #[test]
    fn test_reject_unknown_delta_side() {
        let raw = r#"{
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 2,
        "sending_ts_ms": 1792912814538,
        "msg": {
            "market_id": "test-market",
            "market_ticker": "TEST-MARKET",
            "side": "unknown",
            "price_dollars": "0.0600",
            "delta_fp": "10.00",
            "ts_ms": 1792912814531
        }
    }"#;

        assert!(parse_kalshi_frame(raw, Utc::now()).is_err());
    }

    #[test]
    fn test_ignore_unknown_message_type() {
        let raw = r#"{"type": "heartbeat", "sid": 1}"#;

        let messages = parse_kalshi_frame(raw, Utc::now()).unwrap();

        assert!(matches!(messages.as_slice(), [ExchangeMessage::Ignored]));
    }

    #[test]
    fn test_reject_invalid_json() {
        assert!(parse_kalshi_frame(r#"{"type": "orderbook_delta""#, Utc::now()).is_err());
    }

    #[test]
    fn test_reject_malformed_delta() {
        let raw = r#"{
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 2,
        "sending_ts_ms": 1792912814538,
        "msg": {
            "market_id": "test-market",
            "market_ticker": "TEST-MARKET",
            "side": "yes",
            "price_dollars": "invalid",
            "delta_fp": "10.00",
            "ts_ms": 1792912814531
        }
    }"#;

        assert!(parse_kalshi_frame(raw, Utc::now()).is_err());
    }
}
