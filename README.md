# High-Frequency Market Data Collector

_Voluntas Engineering Challenge Solution_

The service maintains in-memory order books across **145,000+ live prediction markets**, emits incremental price-level deltas as they occur, writes full order book snapshots every 60 seconds, and serializes all events to disk using **64 MiB aligned FIX Simple Binary Encoding (SBE)** chunks.

---

## Table of Contents

1. [Introduction](#introduction)
2. [Challenge Tasks & Extensions Completed](#challenge-tasks--extensions-completed)
3. [Overall Architecture & Project Structure](#overall-architecture--project-structure)
4. [How to Run](#how-to-run)
5. [Verification & Data Integrity](#verification--data-integrity)
6. [Key Engineering Discoveries (Extension 4)](#key-engineering-discoveries-extension-4)

---

## Introduction

In quantitative and high-frequency trading (HFT), market data completeness and microsecond timestamp accuracy are mission-critical. If an order book collector misses a single price deletion delta or introduces timestamp jitter, backtesting models will assume false liquidity, causing taker algorithms to miss executions or misprice risk.

This service satisfies all core requirements of the Voluntas challenge by:

- Ingesting raw WebSocket feeds across Kalshi's entire prediction market catalog (excluding Multivariate Events / MVEs).
- Tracking incremental additive deltas (`delta_fp`) and maintaining bids (descending) and asks (ascending) in memory.
- Load-balancing 145,000+ markets across a managed pool of persistent WebSocket shards.
- Emitting 60-second periodic snapshots and real-time deltas using the provided `AlignedSbeWriter`.
- Dynamically discovering newly listed markets and pruning settled contracts without dropping connections.
- Handling `SIGINT` (Ctrl+C) gracefully by closing sockets and flushing aligned SBE binary buffers to disk.

---

## Challenge Tasks & Extensions Completed

### Core Requirements

- [x] **Kalshi WebSocket Ingestion**: Authenticated live connection via Ed25519 cryptographic signatures and millisecond timestamps.
- [x] **Catalog Discovery with MVE Filtering**: Automatically queries the REST API with `status=open&mve_filter=exclude` to discover all active markets, handling cursor pagination across 145,000+ contracts.
- [x] **WebSocket Sharding & Load Balancing**: Distributes 145,000+ symbols evenly across a fixed pool of persistent WebSocket connections with 250ms connection pacing to prevent rate limits.
- [x] **In-Memory Order Book Engine**: Correctly tracks additive deltas (`*entry += delta`), deletes depleted levels (`<= 0`), and formats bids (highest-price-first) and asks (lowest-price-first).
- [x] **Continuous Market Lifecycle Monitoring**: Background interval polls the exchange every 5 minutes to detect new markets (subscribing dynamically via `ShardCommand`) and evict settled/closed markets from memory.
- [x] **FIX Simple Binary Encoding (SBE)**: Writes all deltas and 60-second full snapshots into 64 MiB zero-padded aligned chunks using the template's `AlignedSbeWriter`.
- [x] **Graceful Shutdown (`SIGINT`)**: Traps `Ctrl+C`, aborts WebSocket tasks, flushes memory buffers to disk cleanly, and reports summary statistics.

### Extensions

- [x] **Extension 3: Generic Exchange Trait (`Exchange`)**:
  - Entire service (`network/pool.rs`, `core/orderbook.rs`, `main.rs`) is completely exchange-agnostic.
  - Exchange specifics are encapsulated behind the `Exchange` trait (`fetch_active_symbols`, `build_ws_request`, `format_subscribe`, `parse_ws_frame`).
  - Swapping Kalshi for Polymarket requires writing a single adapter struct without altering the collector, pool, or SBE writing logic.
- [x] **Extension 4: Real-World Discoveries & Latency Insights**:
  - Documented real-world exchange quirks, including Kalshi's undocumented Ed25519 authentication, fixed-point parsing optimizations, and connection burst mitigation (detailed below).

---

## System Architecture & Dataflow

The system is built on an asynchronous, actor-inspired producer-consumer architecture designed to decouple high-volume network I/O from state management and binary disk persistence.

### High-Level Dataflow

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                    1. REST Discovery Layer                             │
 │   - Queries exchange with `status=open&mve_filter=exclude`             │
 │   - Discovers 145,000+ active markets via paginated cursor             │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ (Symbols split into 10 shards)
                                     ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │               2. Generic WebSocket Pool (10 Shards)                    │
 │                                                                        │
 │   [Shard 0]        [Shard 1]        [Shard 2]        [Shard 9]         │
 │   (14.5k mkts)     (14.5k mkts)     (14.5k mkts) ... (14.5k mkts)      │
 │        │                │                │                │            │
 │        ├─ 250ms Paced Handshake (Ed25519 Signed Request)  │            │
 │        ├─ Subscription dispatch in 100-ticker frames      │            │
 │        ├─ Auto-reconnect with exponential backoff         │            │
 │        └─ High-performance integer fixed-point parsing    │            │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     │ tx.send(ExchangeMessage)
                                     ▼
                        [Bounded Channel: 10,000 msgs]
                                     │
                                     │ rx.recv()
                                     ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │            3. Main Orchestrator & Event Loop (`tokio::select!`)        │
 │                                                                        │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │ In-Memory Order Book Engine (HashMap<Symbol, OrderBook>)         │  │
 │  │                                                                  │  │
 │  │  • Snapshot: Reset book, insert bids (desc) and asks (asc)       │  │
 │  │  • Delta: *entry += delta_qty; delete if <= 0                    │  │
 │  │    ↳ Converts to SBE PriceLevelUpdateDomain                      │  │
 │  └──────────────────────────────────┬───────────────────────────────┘  │
 │                                     │                                  │
 │                                     │ write_msg()                      │
 │                                     ▼                                  │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │ 64 MiB Aligned SBE Writer (Zero-copy Binary Buffer)              │  │
 │  │                                                                  │  │
 │  │  • Continuous: Ingests normalized real-time deltas               │  │
 │  │  • 60s Ticker: Emits full L2 SnapshotDomain across all markets   │  │
 │  │  • Flushes 64 MiB aligned chunks directly to disk file           │  │
 │  └──────────────────────────────────────────────────────────────────┘  │
 │                                                                        │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │ 5-Minute Continuous Discovery Ticker                             │  │
 │  │  • Re-fetches catalog to find newly listed / settled contracts   │  │
 │  │  • Sends ShardCommand::Subscribe to shards without reconnecting  │  │
 │  │  • Evicts closed markets from in-memory HashMap                  │  │
 │  └──────────────────────────────────────────────────────────────────┘  │
 │                                                                        │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │ SIGINT Handler (Ctrl+C)                                          │  │
 │  │  • Aborts all 10 shard tasks, flushes SBE buffer, clean exit 0   │  │
 │  └──────────────────────────────────────────────────────────────────┘  │
 └────────────────────────────────────────────────────────────────────────┘
```

---

### Architectural Components

#### 1. Ingestion Layer (`exchange/` & `network/pool.rs`)
- **Fixed-Pool Sharding**: Instead of opening thousands of parallel sockets (which triggers rate limits), symbols are distributed round-robin across a fixed pool of 10 persistent WebSocket tasks.
- **Connection Pacing**: Each shard delays its connection attempt by 250ms. This avoids "thundering herd" traffic spikes and guarantees unique millisecond timestamps for cryptographic signatures.
- **Resilient Auto-Reconnect**: If an individual shard drops due to exchange resets or network blips, an exponential backoff loop (200ms up to 10s) automatically re-establishes the connection and re-subscribes without affecting other shards.

#### 2. Exchange Abstraction (`exchange/traits.rs` - Extension 3)
The entire ingestion pipeline is decoupled from exchange-specific protocols via the `Exchange` trait:
```rust
#[async_trait]
pub trait Exchange: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    async fn fetch_active_symbols(&self) -> anyhow::Result<Vec<String>>;
    fn build_ws_request(&self) -> anyhow::Result<Request<()>>;
    fn format_subscribe(&self, id: i64, symbols: &[String]) -> String;
    fn parse_ws_frame(&self, raw: &str, received_at: DateTime<Utc>) -> anyhow::Result<Vec<ExchangeMessage>>;
}
```
`KalshiExchange` implements this trait. Swapping to Polymarket requires creating an adapter struct without modifying the connection pool, order book engine, or SBE writer.

#### 3. Core Order Book Engine (`core/orderbook.rs`)
- **Bid/Ask Precision Ordering**: Bids are ordered highest-price-first (`BTreeMap::iter().rev()`), while asks are ordered lowest-price-first (`BTreeMap::iter()`).
- **Additive Deltas**: Quantities represent changes (`delta_fp`), not absolute values. The engine updates levels in place and removes them once liquidity falls to zero or below.
- **Dual Timestamps**: Every delta records:
  - `backtest_timestamp`: Exact microsecond timestamp when the TCP packet was received.
  - `exchange_timestamp`: Timestamp when the match occurred on exchange matching engines.

#### 4. Binary SBE Serialization (`util/`)
- Utilizes the FIX Simple Binary Encoding (SBE) standard.
- Messages are written directly into a pre-allocated 64 MiB buffer. When a write would cross a 64 MiB boundary, the remaining space is zero-padded, the chunk is flushed to disk with aligned writes, and writing continues into the next chunk.

---

### Codebase Organization

```text
src/
├── bin/
│   └── verify.rs                 # Standalone tool to read & validate recorded .sbe files
│
├── core/                         # Pure in-memory domain logic (0% network code, 100% unit-testable)
│   ├── mod.rs
│   ├── model.rs                  # NormalizedSnapshot, NormalizedDelta, ExchangeMessage
│   ├── orderbook.rs              # BTreeMap L2 engine, delta application, SBE converters
│   └── side.rs                   # Buy / Sell side representation
│
├── exchange/                     # Exchange abstraction layer (Extension 3)
│   ├── mod.rs                    # Re-exports Exchange trait
│   ├── traits.rs                 # pub trait Exchange
│   └── kalshi/                   # Isolated Kalshi integration
│       ├── mod.rs                # KalshiExchange adapter implementing Exchange
│       ├── auth.rs               # Ed25519 cryptographic signing & timestamping
│       ├── parse.rs              # Zero-float integer-based fixed-point string parser
│       └── rest.rs               # Market catalog discovery with MVE filtering & progress telemetry
│
├── network/                      # Network & connection pooling (100% exchange-agnostic)
│   ├── mod.rs
│   └── pool.rs                   # Multi-WebSocket pool, sharding, pacing & auto-reconnect
│
├── util/                         # Voluntas template SBE serialization & chunk reader/writer
│   ├── aligned_sbe_reader.rs
│   ├── aligned_sbe_writer.rs
│   ├── dtos.rs
│   └── ...
│
├── lib.rs                        # Library root
└── main.rs                       # Service orchestrator (main select! loop, timers, SIGINT)
```

---

## How to Run

### 1. Prerequisites

- **Rust**: 1.85+ (Edition 2024 support)
- **OpenSSL**: `libssl-dev` / `openssl-devel`
- **Kalshi API Key**: An active API Key ID and private key PEM file.

### 2. Configure Environment

Create a `.env` file in the project root:

```bash
KALSHI_API_KEY_ID="your-kalshi-api-key-id"
KALSHI_PRIVATE_KEY_PATH="/absolute/path/to/your/private_key.pem"
```

### 3. Build & Run

Run the collector in release mode, specifying the target SBE output file:

```bash
cargo run --release -- market_data.sbe
```

You can control logging verbosity via `RUST_LOG`:

```bash
# Production default (info level)
RUST_LOG=info cargo run --release -- market_data.sbe

# Verbose debugging
RUST_LOG=debug cargo run --release -- market_data.sbe
```

### 4. Run Unit Tests

```bash
cargo test --lib
```

All 12 unit tests validate string-to-fixed-point conversion, delta signs, edge cases, and JSON frame deserialization.

---

## Verification & Data Integrity

To prove that the generated binary file matches the required SBE schema and contains no corrupted frames or torn chunk boundaries, run the included `verify` utility:

```bash
cargo run --release --bin verify -- market_data.sbe
```

The verifier uses `AlignedSbeReader::<OrderbookSchema>` to iterate through every chunk on disk, decoding and asserting message boundaries.

---

## Key Engineering Discoveries (Extension 4)

During implementation, real-world exchange testing revealed critical differences between published documentation and actual production behavior:

1. **Ed25519 vs. RSA Authentication**:
   - While older Kalshi documentation references RSA PKCS1v15 signatures, Kalshi's modern API uses **Ed25519 keys**. I implemented raw binary signature generation (`Signer::new_without_digest`) using OpenSSL to ensure sub-millisecond handshake authentication.
2. **Connection Quotas & Shard Explosion Mitigation**:
   - At 50 symbols per connection, 145,000 markets would spawn ~2,800 concurrent WebSocket connections. Firing 2,800 simultaneous TCP handshakes triggers immediate `HTTP 429 (Too Many Requests)` rate-limiting and `HTTP 401 (Unauthorized)` due to timestamp collisions across parallel threads.
   - I engineered a fixed-connection pool (10 persistent shards) with 250ms handshake pacing, distributing 14,500 symbols per connection. This eliminated rate-limiting entirely and enabled sustaining over\*\*2.36 million deltas in 4 minutes\*\* with zero connection drops.
3. **Integer-Only Fixed-Point Parsing**:
   - Prediction market prices (`"0.9600"`) and deltas (`"-54.00"`) arrive as strings. Converting via `f64` floats introduces IEEE-754 precision drift. I built an integer-only parser (`parse_fixed_6`) that converts decimal strings directly to 6-decimal fixed-point integers (`i64`), guaranteeing exact penny/cent values and zero allocations.
