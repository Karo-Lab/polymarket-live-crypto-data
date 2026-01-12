mod config;

use connectors::{core::{AuditActor, IngestionActor, IngestionController}, questdb::QuestDBClient};
use exchanges_common::models::{
    AuditEvent, IngestionEvent,
};

use deadpool_postgres::{ManagerConfig, PoolConfig, Runtime, tokio_postgres::NoTls};
use exchanges_common::models::{
    GenericExchangeConnector, OrderBookActor, OrderBookCommand,
};
use std::collections::HashMap;

use exchanges::{
    bybit::{BybitAdapter, BybitConnectorAdapter},
    okx::{OkxAdapter, OkxConnectorAdapter},
};
use rustls::crypto::{CryptoProvider, ring::default_provider};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::{PostgresDBConfig, QuestDBConfig, SystemLogging};

#[tokio::main]
async fn main() {
    let _log_guard = SystemLogging::init_logging();
    let _install_tls = {
        CryptoProvider::install_default(default_provider())
            .expect("Unable to install rusttls crypto provider")
    };

    let shutdown_token = CancellationToken::new();

    let (restart_signal_tx, restart_signal_rx) = mpsc::channel(1);

    let (ingestion_tx, ingestion_rx) = mpsc::channel::<IngestionEvent>(1024);
    let (audit_tx, audit_rx) = mpsc::channel::<AuditEvent>(1024);
    
    // Polymarket book config
    let (sol_cmd_tx, sol_cmd_rx) = mpsc::channel::<OrderBookCommand>(1024);
    
    // Snapshot ticker
    let snapshot_ticker = vec![sol_cmd_tx.clone()];
    let snapshot_ticker_shutdown = shutdown_token.clone();
    let _snapshot_ticker_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(50));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    for tx in &snapshot_ticker {
                        let _ = tx.try_send(OrderBookCommand::TakeSnapShot);
                    }
                }
                _ = snapshot_ticker_shutdown.cancelled() => break,
            }
        }
    });
    
    // Init audit actor
    let audit_actor = init_audit_actor(audit_rx).await;
    let audit_shutdown = shutdown_token.clone();
    let audit_task = tokio::spawn(audit_actor.run(audit_shutdown));

    // Init ingestion actor
    let ingestion_actor = init_ingestion_actor(ingestion_rx);
    let ingestion_shutdown = shutdown_token.clone();
    let ingestion_task = tokio::spawn(ingestion_actor.run(ingestion_shutdown));


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

    let okx_btc_book_restart = restart_signal_tx.clone();
    let okx_btc_book = OrderBookActor::new(
        BybitAdapter,
        sol_cmd_rx,
        okx_btc_book_restart,
        ingestion_tx,
        audit_tx,
        shutdown_token.clone(),
    );
    let okx_btc_book_task = tokio::spawn(okx_btc_book.run());

    let book_conn = GenericExchangeConnector::new(
        BybitConnectorAdapter,
        bybit_instrument_map,
        restart_signal_rx,
        shutdown_token.clone(),
    );
    let book_conn_task = tokio::spawn(book_conn.run());

    tokio::select! {
        _ = book_conn_task => {
            shutdown_token.cancel();
        }
        _ = okx_btc_book_task => {
            shutdown_token.cancel();
        }
        _ = ingestion_task => {
            shutdown_token.cancel();
        }
        _ = audit_task => {
            shutdown_token.cancel();
            }
        _ = tokio::signal::ctrl_c() => {
            shutdown_token.cancel();
        }
    }
}

async fn init_audit_actor(rx: mpsc::Receiver<AuditEvent>) -> AuditActor {
    let pg_env = PostgresDBConfig::load();
    let mut cfg = deadpool_postgres::Config::new();
    
    cfg.url = Some(pg_env.db_url.clone()); 
    cfg.pool = Some(PoolConfig::new(20));
    cfg.manager = Some(ManagerConfig {
        recycling_method: deadpool_postgres::RecyclingMethod::Fast,
    });

    let pool = cfg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("Failed to create pool config");

    match pool.get().await {
        Ok(_) => println!("Postgres connection established successfully."),
        Err(e) => println!("FATAL: Could not connect to Postgres: {:?}", e),
    }

    AuditActor::new(rx, pool)
}

fn init_ingestion_actor(rx: mpsc::Receiver<IngestionEvent>) -> IngestionActor<QuestDBClient> {
    let quest_env = QuestDBConfig::load();
    let quest_client = QuestDBClient::new(quest_env.db_url);

    let controller = IngestionController::new(quest_client);

    IngestionActor::new(controller, rx)
}
