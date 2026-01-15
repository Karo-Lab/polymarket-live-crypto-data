use deadpool_postgres::Pool;
use exchanges_common::telementry::{
    AuditEvent, ErrorPayload, ErrorSeverity, IngestionEvent, LogEventCategory,
};
use exchanges_common::traits::{IngestionBackend, TableSchema};
use exchanges_common::{log_error, log_info};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{Duration, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

use exchanges_common::error::IngestionError;

#[derive(Debug)]
pub struct AuditClient {
    rx: mpsc::Receiver<AuditEvent>,
    buffer: Vec<AuditEvent>,
    pool: Pool,
    max_batch_time: Duration,
    shutdown_token: CancellationToken,
}

impl AuditClient {
    pub fn new(
        rx: mpsc::Receiver<AuditEvent>,
        pool: Pool,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            rx,
            buffer: Vec::with_capacity(100),
            pool,
            max_batch_time: Duration::from_secs(10),
            shutdown_token,
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) {
        let mut ticker = interval(self.max_batch_time);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            if self.shutdown_token.is_cancelled() {
                log_info!(
                    LogEventCategory::Audit,
                    "shutdown",
                    "audit",
                    "AuditClient shutdown"
                );
                break;
            }
            tokio::select! {
                maybe_event = self.rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            self.buffer.push(event);
                            if self.buffer.len() >= 10000 {
                                if let Err(e) = self.flush().await {
                                    tracing::error!("Audit System Failure: {:?}", e);
                                }
                            }
                        },
                        None => {
                            let _ = self.flush().await;
                            break;
                        }
                    }
                }

                _ = ticker.tick() => {
                    if !self.buffer.is_empty() {
                         if let Err(e) = self.flush().await {
                            tracing::error!("Audit System Failure: {:?}", e);
                        }
                    }
                }

                _ = self.shutdown_token.cancelled() => {
                    let _ = self.flush().await;
                    log_info!(LogEventCategory::System,"shutdown","auditc_client","Audit client shut down");
                    break;
                }
            }
        }
    }

    async fn flush(&mut self) -> Result<(), IngestionError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        

        let client =
            self.pool.get().await.map_err(|e| {
                IngestionError::DbError(format!("Failed to get DB connection: {}", e))
            })?;

        let stmt = client
            .prepare_cached(
                "
            INSERT INTO pipeline_audit
            (timestamp, topic, level, message, details)
            VALUES ($1, $2, $3, $4, $5)
        ",
            )
            .await
            .map_err(|e| IngestionError::DbError(e.to_string()))?;

        let buffer_len = self.buffer.len();

        for event in self.buffer.drain(..) {
            client
                .execute(
                    &stmt,
                    &[
                        &event.timestamp,
                        &event.topic,
                        &event.level.as_ref().to_string(),
                        &event.message,
                        &event.details.unwrap_or(Value::Null),
                    ],
                )
                .await
                .map_err(|e| IngestionError::DbError(e.to_string()))?;
        }
        log_info!(
            LogEventCategory::Ingestion,
            "audit",
            "audit_service",
            buffer_len
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct IngestionController<B> {
    backend: B,
}

impl<B: IngestionBackend> IngestionController<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub async fn ingest<T: TableSchema>(&mut self, batch: &[T]) -> Result<(), IngestionError> {
        for item in batch {
            let table = item.table_name();
            let columns = item.get_columns();
            self.backend.push(table, columns).await?;
        }
        self.backend.flush().await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct IngestionClient<B: IngestionBackend> {
    controller: IngestionController<B>,
    rx: mpsc::Receiver<IngestionEvent>,
    buffer: Vec<IngestionEvent>,
    max_batch_size: usize,
    max_batch_time: Duration,
    shutdown_token: CancellationToken,
}

impl<B: IngestionBackend> IngestionClient<B> {
    pub fn new(
        controller: IngestionController<B>,
        rx: mpsc::Receiver<IngestionEvent>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            controller,
            rx,
            buffer: Vec::with_capacity(2000),
            max_batch_size: 1000,
            max_batch_time: Duration::from_secs(20),
            shutdown_token,
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) {
        let mut ticker = interval(self.max_batch_time);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            if self.shutdown_token.is_cancelled() {
                log_info!(
                    LogEventCategory::Ingestion,
                    "shutdown",
                    "ingestion_client",
                    "IngestionClient shut down"
                );
                break;
            }

            tokio::select! {
                maybe_event = self.rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            self.buffer.push(event);
                            if self.buffer.len() >= self.max_batch_size {
                                let buffer_len = self.buffer.len();
                                self.flush().await;
                                log_info!(LogEventCategory::Ingestion, "flush", "ingestion_actor", buffer_len);
                            }
                        },
                        None => {
                            let buffer_len = self.buffer.len();
                            self.flush().await;
                            log_info!(LogEventCategory::Ingestion, "flush", "ingestion_actor", buffer_len);
                            break;
                        }
                    }
                }

                _ = ticker.tick() => {
                    if !self.buffer.is_empty() {
                        let buffer_len = self.buffer.len();
                        self.flush().await;
                        log_info!(LogEventCategory::Ingestion, "flush", "ingestion_actor", buffer_len);
                    }
                }

                _ = self.shutdown_token.cancelled() => {
                    self.flush().await;
                    log_info!(LogEventCategory::System,"cancelled","ingestion_actor","Ingestion actor shutdown");
                    break;
                }
            }
        }
    }

    async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        if let Err(e) = self.controller.ingest(&self.buffer).await {
            let err_str = e.to_string();
            let error_payload = ErrorPayload::new(
                "ingestion",
                err_str.as_str(),
                ErrorSeverity::Critical,
                false,
            );
            log_error!(
                LogEventCategory::Ingestion,
                "ingestion",
                "ingestion_service",
                error_payload
            );
        }
        self.buffer.clear();
    }
}
