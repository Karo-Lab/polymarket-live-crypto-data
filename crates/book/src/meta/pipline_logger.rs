use deadpool_postgres::Pool;
use tokio::sync::{mpsc};
use tokio_util::sync::CancellationToken;
use tokio_postgres::types::Json;

#[derive(Debug)]
pub enum AuditEvent {
    SeqIdGap {
        symbol: String, expected: i64, actual: i64
    },
    CheckSumFailure {
        symbol: String, expected: i32, actual: i32
    },
    IngestFail {
        details: Option<serde_json::Value>
    }
}

#[derive(Debug)]
pub struct Pipelinelogger {
    audit_rx: mpsc::Receiver<AuditEvent>,
    pool: Pool
}

impl Pipelinelogger {
    pub fn new(audit_rx: mpsc::Receiver<AuditEvent>, pool: Pool) -> Self {
        Self { audit_rx, pool }
    }
    
    pub async fn run(mut self, shutdown_token: CancellationToken) {
        loop {
            tokio::select! {
                event = self.audit_rx.recv() => {
                    match event {
                        Some(e) => {
                            self.handle_event(e).await
                        }
                        None => {
                            break;
                        }
                    }
                }
                _ = shutdown_token.cancelled() => {
                    self.audit_rx.close();
                    while let Some(event) = self.audit_rx.recv().await {
                        self.handle_event(event).await;
                    }
                    break;
                }
            }
        }
    }
    
    async fn handle_event(&self, event: AuditEvent) {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("Failed to get connection to Postgres {:?}", e);
                return;
            }
        };
        
        let (event_type, symbol, expected, actual, details) = match event {
                AuditEvent::SeqIdGap { symbol, expected, actual } => (
                    "SEQ_GAP",
                    symbol,
                    Some(expected),
                    Some(actual),
                    None
                ),
                AuditEvent::CheckSumFailure { symbol, expected, actual } => (
                    "CHECKSUM_FAIL",
                    symbol,
                    Some(expected as i64),
                    Some(actual as i64),
                    None
                ),
                AuditEvent::IngestFail { details } => {
                    ("INGEST_FAIL",
                    "".to_string(),
                    None,
                    None,
                    details)
                }
            };
        
            let stmt = "
                INSERT INTO pipeline_audit 
                (symbol, event_type, expected_val, actual_val, details, ts) 
                VALUES ($1, $2, $3, $4, $5, NOW())
            ";
        
            if let Err(e) = client.execute(stmt, &[
                &symbol, 
                &event_type, 
                &expected, 
                &actual, 
                &Json(&details)
            ]).await {
                tracing::warn!("Failed to insert audit log for {}: {}", symbol, e);
            }
    }
}