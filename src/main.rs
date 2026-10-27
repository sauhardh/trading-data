use engineering_challenge::core::{ExchangeMessage, OrderBook};
use engineering_challenge::exchange::Exchange;
use engineering_challenge::exchange::kalshi::KalshiExchange;
use engineering_challenge::network::WebSocketPool;
use engineering_challenge::util::AlignedSbeWriter;

use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    // Initialize structured logging with microsecond timestamps and env filter
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "engineering_challenge=info,tokio_tungstenite=warn".into()),
        )
        .with_target(false)
        .compact()
        .init();

    let api_key_id = env::var("KALSHI_API_KEY_ID")
        .expect("KALSHI_API_KEY_ID must be set in .env or environment");

    let key_path = env::var("KALSHI_PRIVATE_KEY_PATH")
        .expect("KALSHI_PRIVATE_KEY_PATH must be set in .env or environment");

    let private_key_pem = std::fs::read_to_string(&key_path)
        .unwrap_or_else(|_| panic!("Failed to read private key from {key_path}"));

    let output_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "market_data.sbe".to_string());

    let output_path = PathBuf::from(output_path);

    tracing::info!(path = ?output_path, "Configured SBE output destination");

    let mut writer = AlignedSbeWriter::create(&output_path, CHUNK_SIZE_64_MEBIBYTES)?;

    let exchange: Arc<dyn Exchange> = Arc::new(KalshiExchange {
        api_key_id: api_key_id.clone(),
        private_key_pem: private_key_pem.clone(),
    });

    tracing::info!(exchange = exchange.name(), "Initialized exchange adapter");

    // 1. Discover all active open symbols via REST (excluding MVEs)
    tracing::info!(
        exchange = exchange.name(),
        "Fetching active markets via REST"
    );

    let symbols = exchange.fetch_active_symbols().await?;

    // For continuous discovery of new symbols
    let mut known_symbols: HashSet<String> = symbols.iter().cloned().collect();

    let mut discovery_interval = tokio::time::interval(Duration::from_secs(300));

    // Consume the initial tick because Tokio's interval
    // fires immediately on the first tick.
    discovery_interval.tick().await;

    tracing::info!(count = symbols.len(), "Discovered active tradeable markets");

    if symbols.is_empty() {
        tracing::warn!("No active symbols found. Exiting.");
        return Ok(());
    }

    // 2. Setup channels and spawn WebSocket shards
    let (tx, mut rx) = mpsc::channel::<ExchangeMessage>(10_000);

    let pool = WebSocketPool::spawn(symbols, 10, exchange.clone(), tx).await;

    // 3. In-memory order books mapping: Symbol -> OrderBook
    let mut orderbook: HashMap<String, OrderBook> = HashMap::new();

    // 4. 60-second snapshot timer
    let mut snapshot_interval = tokio::time::interval(Duration::from_secs(60));

    // Consume initial tick.
    snapshot_interval.tick().await;

    let mut delta_count: u64 = 0;
    let mut snapshot_count: u64 = 0;

    tracing::info!("Market data collector running. Press Ctrl+C (SIGINT) to stop cleanly");

    // 5. Main event loop
    loop {
        tokio::select! {
            // A. Incoming data from WebSocket shards
            Some(msg) = rx.recv() => {
                match msg {
                    ExchangeMessage::Snapshot(snap) => {
                        let is_first = orderbook.is_empty();
                        let book = orderbook
                            .entry(snap.symbol.clone())
                            .or_insert_with(|| {
                                OrderBook::new(snap.symbol.clone())
                            });

                        book.apply_snapshot(&snap);

                        if is_first {
                            tracing::info!(symbol = snap.symbol, "Initial orderbook snapshots streaming into memory");
                        }
                    }

                    ExchangeMessage::Delta(delta) => {
                        let book = orderbook
                            .entry(delta.symbol.clone())
                            .or_insert_with(|| {
                                OrderBook::new(delta.symbol.clone())
                            });

                        let abs_qty = book.apply_delta(&delta);

                        let sbe_update =
                            book.delta_to_sbe(&delta, abs_qty);

                        writer.write_msg(&sbe_update)?;

                        delta_count += 1;

                        if delta_count == 1 {
                            tracing::info!("First live market delta applied & written to SBE chunk buffer");
                        } else if delta_count % 50_000 == 0 {
                            tracing::info!(
                                total_deltas = delta_count,
                                live_books = orderbook.len(),
                                "Active delta throughput milestone"
                            );
                        }
                    }

                    _ => {}
                }
            }

            // B. 60-second periodic snapshot timer
            _ = snapshot_interval.tick() => {
                let now = Utc::now();
                let mut snaps_emitted = 0;

                for book in orderbook.values() {
                    let sbe_snap = book.to_sbe_snapshot(now);

                    writer.write_msg(&sbe_snap)?;

                    snaps_emitted += 1;
                }

                snapshot_count += snaps_emitted;

                tracing::info!(
                    snapshots = snaps_emitted,
                    total_deltas = delta_count,
                    "Emitted full snapshots"
                );
            }

            // C. Periodic market discovery
            _ = discovery_interval.tick() => {
                tracing::debug!("Checking for newly opened or settled markets");

                match exchange.fetch_active_symbols().await {
                    Ok(latest_symbols) => {
                        let latest_set: HashSet<String> =
                            latest_symbols.into_iter().collect();

                        // Find added symbols
                        let added: Vec<String> = latest_set
                            .difference(&known_symbols)
                            .cloned()
                            .collect();

                        // Find removed symbols
                        let removed: Vec<String> = known_symbols
                            .difference(&latest_set)
                            .cloned()
                            .collect();

                        if !added.is_empty() {
                            tracing::info!(
                                new_markets = added.len(),
                                "Discovered new markets, dispatching subscriptions"
                            );

                            pool.subscribe_new_symbols(added.clone()).await;

                            for sym in &added {
                                known_symbols.insert(sym.clone());
                            }
                        }

                        if !removed.is_empty() {
                            tracing::info!(
                                closed_markets = removed.len(),
                                "Markets closed/settled, removing from memory"
                            );

                            for sym in &removed {
                                orderbook.remove(sym);
                                known_symbols.remove(sym);
                            }
                        }

                        if added.is_empty() && removed.is_empty() {
                            tracing::debug!("Market discovery check complete: no changes");
                        }
                    }

                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "Failed to refresh market symbols"
                        );
                    }
                }
            }

            // D. Graceful shutdown on SIGINT (Ctrl+C)
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT received! Performing graceful shutdown");
                break;
            }
        }
    }

    // 6. Shutdown cleanup
    tracing::info!("Stopping all WebSocket shards");
    pool.abort_all();

    tracing::info!("Flushing SBE buffer to disk");
    writer.flush()?;

    drop(writer);

    tracing::info!(
        total_deltas = delta_count,
        total_snapshots = snapshot_count,
        file = ?output_path,
        "Clean exit complete"
    );

    Ok(())
}
