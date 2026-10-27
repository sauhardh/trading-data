use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use std::env;
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let path = env::args()
        .nth(1)
        .unwrap_or_else(|| "market_data.sbe".to_string());
    let path = PathBuf::from(path);

    println!("Opening SBE file: {:?}", path);
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;

    let mut snapshots = 0;
    let mut deltas = 0;

    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(decoder) => {
                let snap = SnapshotDomain::try_from_decoder(decoder)?;
                snapshots += 1;

                if snapshots <= 3 {
                    println!(
                        "Snapshot #{} for {}: {} bids, {} asks",
                        snapshots,
                        snap.symbol,
                        snap.bids.len(),
                        snap.asks.len()
                    );
                }
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)?;

                deltas += 1;
                if deltas <= 3 {
                    println!(
                        "Delta #{} for {}: {:?} price={} qty={}",
                        deltas, update.symbol, update.side, update.price_level, update.quantity
                    );
                }
            }
            _ => {
                println!("Road not taken!");
            }
        }
    }

    println!("\n=== Verification Success ===");
    println!(
        "Read {} snapshots and {} deltas cleanly from disk.",
        snapshots, deltas
    );

    Ok(())
}
