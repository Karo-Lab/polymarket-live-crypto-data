use deadpool_postgres::Pool;
use exchanges_common::models::{AuditEvent, IngestionEvent};
use exchanges_common::traits::{IngestionBackend, TableSchema};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{Duration, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

use exchanges_common::error::IngestionError;

#[derive(Debug)]
pub struct AuditActor {
    rx: mpsc::Receiver<AuditEvent>,
    buffer: Vec<AuditEvent>,
    pool: Pool,
    max_batch_time: Duration,
}

impl AuditActor {
    pub fn new(rx: mpsc::Receiver<AuditEvent>, pool: Pool) -> Self {
        Self {
            rx,
            buffer: Vec::with_capacity(100),
            pool,
            max_batch_time: Duration::from_secs(10),
        }
    }

    #[tracing::instrument(skip(self, shutdown))]
    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut ticker = interval(self.max_batch_time);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
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

                _ = shutdown.cancelled() => {
                    let _ = self.flush().await;
                    tracing::info!("Audit actor shut down");
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
        tracing::info!("Inserted Auditlog");
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
pub struct IngestionActor<B: IngestionBackend> {
    controller: IngestionController<B>,
    rx: mpsc::Receiver<IngestionEvent>,
    buffer: Vec<IngestionEvent>,
    max_batch_size: usize,
    max_batch_time: Duration,
}

impl<B: IngestionBackend> IngestionActor<B> {
    pub fn new(controller: IngestionController<B>, rx: mpsc::Receiver<IngestionEvent>) -> Self {
        Self {
            controller,
            rx,
            buffer: Vec::with_capacity(2000),
            max_batch_size: 1000,
            max_batch_time: Duration::from_secs(10),
        }
    }

    #[tracing::instrument(skip(self, shutdown))]
    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut ticker = interval(self.max_batch_time);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe_event = self.rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            self.buffer.push(event);
                            if self.buffer.len() >= self.max_batch_size {
                                self.flush().await;
                            }
                        },
                        None => {
                            self.flush().await;
                            break;
                        }
                    }
                }

                _ = ticker.tick() => {
                    if !self.buffer.is_empty() {
                        self.flush().await;
                    }
                }

                _ = shutdown.cancelled() => {
                    self.flush().await;
                    tracing::info!("Ingestion actor shut down");
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
            tracing::error!("Ingestion Error: {:?}", e);
        }
        self.buffer.clear();
    }
}
