use std::time::Duration;
use rust_decimal::{
    Decimal,
    prelude::ToPrimitive
};
use tokio::{sync::mpsc, time::{MissedTickBehavior, interval}};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct BookSnapShot {
    pub instrument_id: String,
    pub exchange_ts: i64,
    pub timestamp: i64,
    pub bids: Vec<Vec<Decimal>>,
    pub asks: Vec<Vec<Decimal>>
}

impl BookSnapShot {
    pub fn bids_f64(&self) -> Vec<Vec<f64>> {
        self.bids.iter().map(|inner_vec| {
            inner_vec
                .iter()
                .map(|v| v.to_f64().unwrap_or(0.0))
                .collect()
        }).collect()
    }
    pub fn asks_f64(&self) -> Vec<Vec<f64>> {
        self.asks.iter().map(|inner_vec| {
            inner_vec
                .iter()
                .map(|v| v.to_f64().unwrap_or(0.0))
                .collect()
        }).collect()
    }
}

pub struct SnapshotManager {
    snapshot_rx: mpsc::Receiver<BookSnapShot>,
    quest_db_tx: mpsc::Sender<Vec<BookSnapShot>>
}

impl SnapshotManager {
    pub fn new(
        snapshot_rx: mpsc::Receiver<BookSnapShot>,
        quest_db_tx: mpsc::Sender<Vec<BookSnapShot>>
    ) -> Self {
        Self {
            snapshot_rx,
            quest_db_tx
        }
    }
    
    pub async fn run_ingestion_pipeline(mut self, shutdown_token: CancellationToken) {
        let batch_size = 1000;
        
        let mut batch = Vec::with_capacity(batch_size);
        
        loop {
            tokio::select! {
                // Every 50ms, a snapshot of the L2 books is sent to this thread
                // If the batch is full, it get sent to quest db thread
                Some(msg) = self.snapshot_rx.recv() => {
                    batch.push(msg);
                    
                    if batch.len() >= batch_size {
                        tracing::info!("Inges {}", batch.len());
                        self.flush_batch(&mut batch, batch_size).await;
                    }
                }
                _ = shutdown_token.cancelled() => {
                    if !batch.is_empty() {
                        let _ = self.quest_db_tx.send(batch).await;
                    }
                    return;
                }
                else => break
            }
        }
    }
    async fn flush_batch(&self, batch: &mut Vec<BookSnapShot>, batch_size: usize) {
        let buffer = std::mem::replace(batch, Vec::with_capacity(batch_size));
        
        if let Err(e) = self.quest_db_tx.send(buffer).await {
            tracing::error!("Failed to sent batch {:?}", e)
        }
    }
}