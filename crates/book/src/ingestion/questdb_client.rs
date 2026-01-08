use questdb::ingress::{Buffer, Sender, TimestampNanos};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use serde_json::{json};

use crate::{common::config::QuestDBConfig, ingestion::orderbook::BookSnapShot, meta::pipline_logger::AuditEvent};

#[derive(Debug)]
pub struct QuestDBClient {
    sender: Sender,
    data_rx: mpsc::Receiver<Vec<BookSnapShot>>,
}

impl QuestDBClient {
    pub fn new(data_rx: mpsc::Receiver<Vec<BookSnapShot>>) -> Self {
        let quest_db_config = QuestDBConfig::load();
        tracing::info!("dbg {}", quest_db_config.db_url);
        let sender = Sender::from_conf(quest_db_config.db_url)
            .expect("Unable to instantinate quest db client");
        Self { 
            sender, 
            data_rx 
        }
    }
    fn ingest(&mut self, buffer: &mut Buffer, snapshots: &[BookSnapShot]) -> questdb::Result<()> {
        for snapshot in snapshots {
            buffer
                .table("okx_live_price")?
                .symbol("symbol", &snapshot.instrument_id)?
                .column_ts("exchange_ts", TimestampNanos::new(snapshot.exchange_ts))?
                .column_arr("bids", &snapshot.bids_f64())?
                .column_arr("asks", &snapshot.asks_f64())?
                .at(TimestampNanos::new(snapshot.timestamp))?;
        }
        self.sender.flush(buffer)?;
        Ok(())
    }

    pub async fn run(mut self, audit_tx: mpsc::Sender<AuditEvent>,shutdown_token: CancellationToken) {
        let mut buffer = self.sender.new_buffer();

        loop {
            tokio::select! {
                data = self.data_rx.recv() => {
                    if let Some(snapshots) = data {
                        let res = tokio::task::block_in_place(|| {
                            self.ingest(&mut buffer, &snapshots)
                        });
                        
                        if let Err(e) = res {
                            let affected_instruments: Vec<String> = snapshots.into_iter().map(|v| {
                                v.instrument_id
                            }).collect();
                            let error_details = json!({
                                    "error_msg": e.to_string(),      
                                    "error_debug": format!("{:?}", e), 
                                    "affected_ids": affected_instruments, 
                                    "snapshot_count": affected_instruments.len()
                                });
                            let _ = audit_tx.try_send(AuditEvent::IngestFail { 
                                details: Some(error_details)
                            });
                        }
                    } else {
                        break;
                    }
                }
                _ = shutdown_token.cancelled() => {
                    return;
                }
            }
        }
    }
}
