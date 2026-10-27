use super::auth::{sign, timestamp_ms};
use anyhow::{self, Context};
use serde::Deserialize;

const BASE_URL: &str = "https://external-api.kalshi.com/trade-api/v2";

#[derive(Deserialize, Debug)]
struct MarketsResponse {
    markets: Vec<Market>,
    cursor: Option<String>,
}

#[derive(Deserialize, Debug)]
struct Market {
    ticker: String,
    #[allow(dead_code)]
    status: Option<String>,
    #[allow(dead_code)]
    title: Option<String>,
    #[allow(dead_code)]
    market_id: Option<String>,
}

pub async fn fetch_active_symbols(
    api_key_id: &str,
    private_key_pem: &str,
) -> anyhow::Result<Vec<String>> {
    let client = reqwest::Client::new();
    let mut all_tickers = Vec::new();
    let mut cursor: Option<String> = None;
    let mut page = 0usize;

    loop {
        page += 1;
        let mut url = format!("{BASE_URL}/markets?status=open&mve_filter=exclude&limit=200");
        if let Some(ref c) = cursor
            && !c.is_empty()
        {
            url.push_str(&format!("&cursor={c}"));
        }

        let timestamp = timestamp_ms();
        let path = "/trade-api/v2/markets";
        let message = format!("{}GET{}", timestamp, path);
        let signature = sign(private_key_pem, message.as_bytes())?;

        let resp: MarketsResponse = client
            .get(&url)
            .header("KALSHI-ACCESS-KEY", api_key_id)
            .header("KALSHI-ACCESS-TIMESTAMP", timestamp.to_string())
            .header("KALSHI-ACCESS-SIGNATURE", &signature)
            .send()
            .await?
            .json()
            .await
            .context("Failed to parse response")?;

        all_tickers.extend(resp.markets.into_iter().map(|m| m.ticker));

        if page % 25 == 0 || resp.cursor.is_none() {
            tracing::info!(
                page,
                markets_fetched = all_tickers.len(),
                "Catalog discovery progress"
            );
        }

        match resp.cursor {
            Some(c) if !c.is_empty() => cursor = Some(c),
            _ => break,
        }
    }

    Ok(all_tickers)
}
