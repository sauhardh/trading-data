use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::core::ExchangeMessage;
use crate::exchange::Exchange;

pub enum ShardCommand {
    Subscribe(Vec<String>),
}

/// Manages multiple WebSocket connections (shards).
pub struct WebSocketPool {
    handles: Vec<tokio::task::JoinHandle<()>>,
    command_txs: Vec<mpsc::Sender<ShardCommand>>,
}

impl WebSocketPool {
    pub async fn spawn(
        symbols: Vec<String>,
        num_shards: usize,
        exchange: Arc<dyn Exchange>,
        tx: mpsc::Sender<ExchangeMessage>,
    ) -> Self {
        let num_shards = num_shards.min(symbols.len()).max(1);
        let mut command_txs = Vec::with_capacity(num_shards);
        let mut handles = Vec::new();

        // 1. Distribute symbols evenly across the fixed number of shards
        let mut shards: Vec<Vec<String>> = vec![Vec::new(); num_shards];
        for (i, symbol) in symbols.into_iter().enumerate() {
            shards[i % num_shards].push(symbol);
        }

        let total_markets = shards.iter().map(|s| s.len()).sum::<usize>();
        tracing::info!(
            shards = num_shards,
            markets = total_markets,
            exchange = exchange.name(),
            "Spawning WebSocket shards"
        );

        for (shard_id, shard_symbols) in shards.into_iter().enumerate() {
            // Pacing delay: space out connection attempts by 250ms
            if shard_id > 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }

            let (cmd_tx, cmd_rx) = mpsc::channel::<ShardCommand>(100);
            command_txs.push(cmd_tx);

            let tx_clone = tx.clone();
            let ex_clone = exchange.clone();

            let handle = tokio::spawn(async move {
                run_shard(shard_id, shard_symbols, ex_clone, tx_clone, cmd_rx).await;
            });

            handles.push(handle);
        }

        Self {
            handles,
            command_txs,
        }
    }

    pub fn abort_all(&self) {
        for handle in &self.handles {
            handle.abort();
        }
    }

    pub async fn subscribe_new_symbols(&self, new_symbols: Vec<String>) {
        if new_symbols.is_empty() {
            return;
        }

        let num_shards = self.command_txs.len();
        let mut buckets: Vec<Vec<String>> = vec![Vec::new(); num_shards];

        for (i, sym) in new_symbols.into_iter().enumerate() {
            buckets[i % num_shards].push(sym);
        }

        for (shard_id, batch) in buckets.into_iter().enumerate() {
            if !batch.is_empty() {
                let _ = self.command_txs[shard_id]
                    .send(ShardCommand::Subscribe(batch))
                    .await;
            }
        }
    }
}

pub async fn run_shard(
    shard_id: usize,
    symbols: Vec<String>,
    exchange: Arc<dyn Exchange>,
    tx_clone: mpsc::Sender<ExchangeMessage>,
    mut cmd_rx: mpsc::Receiver<ShardCommand>,
) {
    let mut backoff = Duration::from_millis(200);

    loop {
        tracing::info!(
            shard_id,
            symbols = symbols.len(),
            exchange = exchange.name(),
            "Initiating shard connection"
        );

        match connect_and_listen(
            shard_id,
            &symbols,
            &exchange,
            &tx_clone,
            &mut cmd_rx,
        )
        .await
        {
            Ok(()) => {
                tracing::info!(shard_id, "Shard connection closed gracefully");
                backoff = Duration::from_millis(200);
            }

            Err(e) => {
                tracing::warn!(
                    shard_id,
                    error = %e,
                    backoff_ms = backoff.as_millis(),
                    "Shard disconnected, scheduling reconnect"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }
    }
}

async fn connect_and_listen(
    shard_id: usize,
    symbols: &[String],
    exchange: &Arc<dyn Exchange>,
    tx: &mpsc::Sender<ExchangeMessage>,
    cmd_rx: &mut mpsc::Receiver<ShardCommand>,
) -> anyhow::Result<()> {
    let request = exchange.build_ws_request()?;

    let (mut ws_stream, _) = connect_async(request).await?;
    tracing::info!(shard_id, exchange = exchange.name(), "WebSocket handshake complete");

    // Subscribe to all markets in this shard in chunks of 100 tickers per message
    for (batch_idx, batch) in symbols.chunks(100).enumerate() {
        let req_id = (shard_id * 10_000 + batch_idx) as i64 + 1;
        let sub_cmd = exchange.format_subscribe(req_id, batch);

        ws_stream
            .send(Message::Text(sub_cmd.into()))
            .await?;
    }

    tracing::info!(
        shard_id,
        markets = symbols.len(),
        "Subscriptions dispatched, actively listening"
    );

    loop {
        tokio::select! {
            // Branch A: New symbols sent from the 5-minute discovery timer!
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    ShardCommand::Subscribe(new_tickers) => {
                        tracing::info!(
                            shard_id,
                            new_symbols = new_tickers.len(),
                            "Dynamically subscribing to new symbols"
                        );
                        for (batch_idx, batch) in new_tickers.chunks(100).enumerate() {
                            let req_id = (shard_id * 100_000 + batch_idx) as i64 + 1;
                            let sub_cmd = exchange.format_subscribe(req_id, batch);

                            ws_stream
                                .send(Message::Text(sub_cmd.into()))
                                .await?;
                        }
                    }
                }
            }

            // Branch B: Incoming WebSocket packet from the exchange
            msg_result = ws_stream.next() => {
                let Some(msg_result) = msg_result else {
                    tracing::warn!(shard_id, "WebSocket stream closed by exchange");
                    break;
                };

                let received_at = Utc::now();
                match msg_result? {
                    Message::Text(text) => match exchange.parse_ws_frame(&text, received_at) {
                        Ok(message) => {
                            for msg in message {
                                if !matches!(msg, ExchangeMessage::Ignored | ExchangeMessage::Heartbeat)
                                    && tx.send(msg).await.is_err()
                                {
                                    return Ok(()); // Receiver dropped (shutdown)
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(shard_id, error = %e, "Frame parsing warning");
                        }
                    },
                    Message::Ping(data) => {
                        ws_stream.send(Message::Pong(data)).await?;
                    }
                    Message::Close(_) => {
                        tracing::info!(shard_id, "Server sent WebSocket close frame");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
