mod config;

use connectors::{
    core::{AuditClient, IngestionClient, IngestionController},
    ingestion::QuestDBClient,
};
use exchanges_common::{
    log_error, log_info,
    models::{ExchangeConnector, OrderBookCommand, OrderBooksIndexerWorker, TaskSupervisor},
    telementry::{AuditEvent, ErrorPayload, ErrorSeverity, IngestionEvent, LogEventCategory},
};

use deadpool_postgres::{ManagerConfig, PoolConfig, Runtime, tokio_postgres::NoTls};
use std::time::Duration;

use exchanges::{binance::BinanceConnectorAdapter, bybit::BybitConnectorAdapter};
use rustls::crypto::{CryptoProvider, ring::default_provider};
use tokio::{
    sync::{broadcast, mpsc},
    time::sleep,
};

use crate::config::{PostgresDBConfig, QuestDBConfig, SystemLogging};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let (_console_log_guard, _file_log_guard) =
        SystemLogging::init_logging("./logs/crytp_orderbook_ingestion", "app.log");
    let _install_tls = {
        CryptoProvider::install_default(default_provider())
            .expect("Unable to install rusttls crypto provider")
    };

    let supervisor = TaskSupervisor::new();

    let (restart_signal_tx, _) = broadcast::channel(1024);
    let (ingestion_tx, ingestion_rx) = mpsc::channel::<IngestionEvent>(10_000);
    let (audit_tx, audit_rx) = mpsc::channel::<AuditEvent>(1024);
    let (book_cmd_tx, book_cmd_rx) = mpsc::channel::<OrderBookCommand>(32_768);

    // let pg_pool = init_postgres_pool().await;
    // let questdb_client = init_questdb_client();

    // Snapshot ticker
    let snapshot_ticker = book_cmd_tx.clone();
    supervisor.spawn_service("snapshot_ticker", move || {
        let tx = snapshot_ticker.clone();
        async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(50));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                let _ = tx.try_send(OrderBookCommand::TakeSnapShot);
            }
        }
    });

    // supervisor.spawn_worker("audit_worket", |token| async move {
    //     let client = AuditClient::new(audit_rx, pg_pool, token.clone());
    //     client.run().await;
    //     Ok::<(), String>(())
    // });

    // supervisor.spawn_worker("ingestion_worker", |token| async move {
    //     let controller = IngestionController::new(questdb_client);
    //     let client = IngestionClient::new(controller, ingestion_rx, token.clone());
    //     client.run().await;
    //     Ok::<(), String>(())
    // });

    let ecs_restart_signal_tx = restart_signal_tx.clone();
    let ecs_ingestion_tx = ingestion_tx.clone();
    let ecs_audit_tx = audit_tx.clone();
    supervisor.spawn_worker("orderbook_indexer_worker", |token| async move {
        let engine = OrderBooksIndexerWorker::new(
            book_cmd_rx,
            ecs_restart_signal_tx,
            ecs_ingestion_tx,
            ecs_audit_tx,
            token,
        );
        engine.run().await;
        Ok::<(), String>(())
    });

    let bybit_instruments = vec![
        "SOLUSDT".to_string(),
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "XRPUSDT".to_string(),
    ];
    let bybit_data_sender = book_cmd_tx.clone();
    let bybit_restart_rx = restart_signal_tx.subscribe();
    supervisor.spawn_worker("bybit_exchange_connector", |token| async move {
        let conn = ExchangeConnector::new(
            BybitConnectorAdapter,
            bybit_instruments,
            bybit_data_sender,
            bybit_restart_rx,
            token,
        );
        conn.run().await;
        Ok::<(), String>(())
    });

    let binance_instruments = vec![
        "btcusdt".to_string(),
        "ethusdt".to_string(),
        "solusdt".to_string(),
        "xrpusdt".to_string(),
    ];
    let binance_data_sender = book_cmd_tx.clone();
    let binance_restart_rx = restart_signal_tx.subscribe();
    supervisor.spawn_worker("binance_exchange_connector", |token| async move {
        let conn = ExchangeConnector::new(
            BinanceConnectorAdapter::default(),
            binance_instruments,
            binance_data_sender,
            binance_restart_rx,
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
