use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::Request;

pub mod auth;
pub mod parse;
pub mod rest;

use self::auth::{WEBSOCKET_URL, sign, timestamp_ms};
use self::parse::parse_kalshi_frame;
use self::rest::fetch_active_symbols;
use crate::core::ExchangeMessage;
use crate::exchange::Exchange;

pub struct KalshiExchange {
    pub api_key_id: String,
    pub private_key_pem: String,
}

#[async_trait]
impl Exchange for KalshiExchange {
    fn name(&self) -> &'static str {
        "Kalshi"
    }

    async fn fetch_active_symbols(&self) -> anyhow::Result<Vec<String>> {
        fetch_active_symbols(&self.api_key_id, &self.private_key_pem).await
    }

    fn build_ws_request(&self) -> anyhow::Result<Request<()>> {
        let timestamp = timestamp_ms();
        let message = format!("{}GET/trade-api/ws/v2", timestamp);
        let signature = sign(&self.private_key_pem, message.as_bytes())?;
        let mut request = WEBSOCKET_URL.into_client_request()?;
        request
            .headers_mut()
            .insert("KALSHI-ACCESS-KEY", self.api_key_id.parse()?);
        request
            .headers_mut()
            .insert("KALSHI-ACCESS-SIGNATURE", signature.parse()?);
        request
            .headers_mut()
            .insert("KALSHI-ACCESS-TIMESTAMP", timestamp.to_string().parse()?);
        Ok(request)
    }

    fn format_subscribe(&self, id: i64, symbols: &[String]) -> String {
        serde_json::json!({
            "id": id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": symbols,
            }
        })
        .to_string()
    }

    fn parse_ws_frame(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<Vec<ExchangeMessage>> {
        parse_kalshi_frame(raw, received_at)
    }
}
