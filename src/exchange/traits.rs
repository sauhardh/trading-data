use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::http::Request;

use crate::core::ExchangeMessage;

#[async_trait]
pub trait Exchange: Send + Sync + 'static {
    /// Provider name
    fn name(&self) -> &'static str;

    /// Discovers active symbols via REST
    async fn fetch_active_symbols(&self) -> anyhow::Result<Vec<String>>;

    /// Builds the WebSocket HTTP Upgrade request (handles headers/auth)
    fn build_ws_request(&self) -> anyhow::Result<Request<()>>;

    /// Formats a subscribe message for a batch of symbols
    fn format_subscribe(&self, id: i64, symbols: &[String]) -> String;

    /// Parses an incoming WebSocket text frame into normalized domain messages
    fn parse_ws_frame(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<Vec<ExchangeMessage>>;
}
