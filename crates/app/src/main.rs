mod config;

use connectors::{
    core::{AuditClient, IngestionClient, IngestionController},
    ingestion::QuestDBClient,
};
use exchanges_common::{
    log_error, log_info,
    models::TaskSupervisor,
    telementry::{AuditEvent, ErrorPayload, ErrorSeverity, IngestionEvent, LogEventCategory},
};

use deadpool_postgres::{ManagerConfig, PoolConfig, Runtime, tokio_postgres::NoTls};
use exchanges_common::models::{ExchangeConnector, LocalL2OrderBook, OrderBookCommand};
use std::{collections::HashMap, time::Duration};

use exchanges::bybit::{BybitAdapter, BybitConnectorAdapter};
use rustls::crypto::{CryptoProvider, ring::default_provider};
use tokio::{sync::mpsc, time::sleep};

use crate::config::{PostgresDBConfig, QuestDBConfig, SystemLogging};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let (_console_log_guard, _file_log_guard) = SystemLogging::init_logging("./logs/crytp_orderbook_ingestion", "app.log");
    let _install_tls = {
        CryptoProvider::install_default(default_provider())
            .expect("Unable to install rusttls crypto provider")
    };

    let supervisor = TaskSupervisor::new();

    let (restart_signal_tx, restart_signal_rx) = mpsc::channel(1);
    let (ingestion_tx, ingestion_rx) = mpsc::channel::<IngestionEvent>(1024);
    let (audit_tx, audit_rx) = mpsc::channel::<AuditEvent>(1024);

    // SOLUSDT orderbook channels
    let (sol_cmd_tx, sol_cmd_rx) = mpsc::channel::<OrderBookCommand>(1024);

    let pg_pool = init_postgres_pool().await;
    let questdb_client = init_questdb_client();

    // Snapshot ticker
    let snapshot_ticker = vec![sol_cmd_tx.clone()];
    supervisor.spawn_service("snapshot_ticker", move || {
        let tickers = snapshot_ticker.clone();
        async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(50));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                for tx in &tickers {
                    let _ = tx.try_send(OrderBookCommand::TakeSnapShot);
                }
            }
        }
    });

    supervisor.spawn_worker("audit_actor", |token| async move {
        let client = AuditClient::new(audit_rx, pg_pool, token.clone());
        client.run().await;
        Ok::<(), String>(())
    });

    // Init ingestion actor
    supervisor.spawn_worker("ingestion_actor", |token| async move {
        let controller = IngestionController::new(questdb_client);
        let client = IngestionClient::new(controller, ingestion_rx, token.clone());
        client.run().await;
        Ok::<(), String>(())
    });

    let mut okx_instrument_map = HashMap::new();
    okx_instrument_map.insert(
        u128::from_le_bytes([
            b'S', b'O', b'L', b'-', b'U', b'S', b'D', b'T', b'-', b'S', b'W', b'A', b'P', 0, 0, 0,
        ]),
        sol_cmd_tx.clone(),
    );
    let mut bybit_instrument_map = HashMap::new();
    bybit_instrument_map.insert(
        u128::from_le_bytes([
            b'S', b'O', b'L', b'U', b'S', b'D', b'T', 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]),
        sol_cmd_tx.clone(),
    );

    let bybit_sol_book_restart = restart_signal_tx.clone();
    let sol_cmd_rx_safe = sol_cmd_rx;

    let book_shutdown = supervisor.global_shutdown.clone();

    supervisor.spawn_worker("bybit_sol_book", |token| async move {
        let book = LocalL2OrderBook::new(
            BybitAdapter,
            sol_cmd_rx_safe,
            bybit_sol_book_restart,
            ingestion_tx,
            audit_tx,
            token,
        );
        book.run().await;
        Ok::<(), String>(())
    });

    supervisor.spawn_worker("exchange_connector", |token| async move {
        let conn = ExchangeConnector::new(
            BybitConnectorAdapter,
            bybit_instrument_map,
            restart_signal_rx,
            token,
        );
        conn.run().await;
        Ok::<(), String>(())
    });

    match tokio::signal::ctrl_c().await {
        Ok(()) => log_info!(
            LogEventCategory::System,
            "shutdown",
            "main",
            "Ctrl-C received"
        ),
        Err(err) => {
            let err_ = err.to_string();
            log_error!(
                LogEventCategory::System,
                "shutdown",
                "main",
                ErrorPayload::new("signal_error", &err_, ErrorSeverity::Fatal, false,)
            )
        }
    }

    supervisor.shutdown().await;

    sleep(Duration::from_secs(1)).await;
    log_info!(
        LogEventCategory::System,
        "shutdown",
        "main",
        "System shutdown complete"
    );
}

async fn init_postgres_pool() -> deadpool_postgres::Pool {
    let pg_env = PostgresDBConfig::load();
    let mut cfg = deadpool_postgres::Config::new();

    cfg.url = Some(pg_env.db_url.clone());
    cfg.pool = Some(PoolConfig::new(20));
    cfg.manager = Some(ManagerConfig {
        recycling_method: deadpool_postgres::RecyclingMethod::Fast,
    });

    let pool = cfg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("Failed to create postgres pool");

    match pool.get().await {
        Ok(_) => log_info!(
            LogEventCategory::System,
            "connected",
            "postgres",
            "Postgres connected"
        ),
        Err(e) => {
            let err = e.to_string();
            let payload = ErrorPayload::new(
                "Failed to connect to postgres",
                &err,
                ErrorSeverity::Critical,
                false,
            );
            log_error!(LogEventCategory::System, "failed", "postgres", payload);
            panic!("Failed to connect to postgres");
        }
    }
    pool
}

fn init_questdb_client() -> QuestDBClient {
    let quest_env = QuestDBConfig::load();
    QuestDBClient::new(quest_env.db_url)
}
