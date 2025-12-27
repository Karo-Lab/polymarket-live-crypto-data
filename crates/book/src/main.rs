mod common;
mod source;
mod ingestion;
mod meta;

use std::collections::HashMap;
use futures::future::join_all;
use rustls::crypto::{ring::default_provider, CryptoProvider};
use tokio::{sync::{mpsc}};
use tokio_util::sync::CancellationToken;

use crate::{
    common::logging::SystemLogging, ingestion::{orderbook::{BookSnapShot, SnapshotManager}, questdb_client::QuestDBClient}, meta::{pipline_logger::{Pipelinelogger}, postgres_client::create_pool}, source::okx::{OkxOrderBook, OkxOrderBookDataStream, OrderBookCommand}
};

#[tokio::main]
async fn main() {
    let _log_guard = SystemLogging::init_logging();
    let _install_tls = {
        CryptoProvider::install_default(default_provider())
            .expect("Unable to install rusttls crypto provider")
    };
    
    let dead_pool = create_pool();

    let (btc_cmd_tx, btc_cmd_rx) = mpsc::channel::<OrderBookCommand>(1024);
    let (eth_cmd_tx, eth_cmd_rx) = mpsc::channel::<OrderBookCommand>(1024);
    let (sol_cmd_tx, sol_cmd_rx) = mpsc::channel::<OrderBookCommand>(1024);
    let (xrp_cmd_tx, xrp_cmd_rx) = mpsc::channel::<OrderBookCommand>(1024);
    
    let snapshot_heartbeat_senders = vec![
        btc_cmd_tx.clone(),
        eth_cmd_tx.clone(),
        sol_cmd_tx.clone(),
        xrp_cmd_tx.clone(),
    ];
        
    let (ingestion_pipeline_tx, ingestion_pipeline_rx) = mpsc::channel::<BookSnapShot>(1024);

    let mut channel_ob_map = HashMap::with_capacity(4);
    let mut hex_topic_map = HashMap::with_capacity(4);

    let btc_topic = "BTC-USDT-SWAP".to_string();
    let eth_topic = "ETH-USDT-SWAP".to_string();
    let sol_topic = "SOL-USDT-SWAP".to_string();
    let xrp_topic = "XRP-USDT-SWAP".to_string();

    channel_ob_map.insert(btc_topic.clone(), btc_cmd_tx);
    channel_ob_map.insert(eth_topic.clone(), eth_cmd_tx);
    channel_ob_map.insert(sol_topic.clone(), sol_cmd_tx);
    channel_ob_map.insert(xrp_topic.clone(), xrp_cmd_tx);

    let sol_hex: u128 = u128::from_le_bytes([
        b'S', b'O', b'L', b'-', b'U', b'S', b'D', b'T', b'-', b'S', b'W', b'A', b'P', 0, 0, 0,
    ]);
    let btc_hex: u128 = u128::from_le_bytes([
        b'B', b'T', b'C', b'-', b'U', b'S', b'D', b'T', b'-', b'S', b'W', b'A', b'P', 0, 0, 0,
    ]);
    let eth_hex: u128 = u128::from_le_bytes([
        b'E', b'T', b'H', b'-', b'U', b'S', b'D', b'T', b'-', b'S', b'W', b'A', b'P', 0, 0, 0,
    ]);
    let xrp_hex: u128 = u128::from_le_bytes([
        b'X', b'R', b'P', b'-', b'U', b'S', b'D', b'T', b'-', b'S', b'W', b'A', b'P', 0, 0, 0,
    ]);

    hex_topic_map.insert(btc_hex, btc_topic);
    hex_topic_map.insert(eth_hex, eth_topic);
    hex_topic_map.insert(sol_hex, sol_topic);
    hex_topic_map.insert(xrp_hex, xrp_topic);

    let okx_book_stream = OkxOrderBookDataStream::new(channel_ob_map, hex_topic_map);

    let okx_btc_ob = OkxOrderBook::new("BTC-USDT-SWAP".to_string());
    let okx_eth_ob = OkxOrderBook::new("ETH-USDT-SWAP".to_string());
    let okx_sol_ob = OkxOrderBook::new("SOL-USDT-SWAP".to_string());
    let okx_xrp_ob = OkxOrderBook::new("XRP-USDT-SWAP".to_string());

    let shutdown = CancellationToken::new();

    let (restart_signal_tx, restart_signal_rx) = mpsc::channel(1);
    let (audit_tx, audit_rx) = mpsc::channel(1);
    
    let btc_restart_sig_cln = restart_signal_tx.clone();
    let btc_shutdown_sig = shutdown.clone();
    let btc_ingestion_tx = ingestion_pipeline_tx.clone();
    let btc_audit_tx = audit_tx.clone();
    let okx_book_btc_handler_task = tokio::spawn(async move {
        let _ = okx_btc_ob
            .run(btc_cmd_rx, btc_restart_sig_cln,btc_ingestion_tx,btc_audit_tx,btc_shutdown_sig)
            .await;
    });
    
    let eth_restart_sig_cln = restart_signal_tx.clone();
    let eth_shutdown_sig = shutdown.clone();
    let eth_ingestion_tx = ingestion_pipeline_tx.clone();
    let eth_audit_tx = audit_tx.clone();
    let okx_book_eth_handler_task = tokio::spawn(async move {
        let _ = okx_eth_ob
            .run(eth_cmd_rx, eth_restart_sig_cln,eth_ingestion_tx,eth_audit_tx, eth_shutdown_sig)
            .await;
    });
    
    let sol_restart_sig_cln = restart_signal_tx.clone();
    let sol_shutdown_sig = shutdown.clone();
    let sol_ingestion_tx = ingestion_pipeline_tx.clone();
    let sol_audit_tx = audit_tx.clone();
    let okx_book_sol_handler_task = tokio::spawn(async move {
        let _ = okx_sol_ob
            .run(sol_cmd_rx, sol_restart_sig_cln,sol_ingestion_tx,sol_audit_tx, sol_shutdown_sig)
            .await;
    });
    
    let xrp_restart_sig_cln = restart_signal_tx.clone();
    let xrp_shutdown_sig = shutdown.clone();
    let xrp_ingestion_tx = ingestion_pipeline_tx.clone();
    let xrp_audit_tx = audit_tx.clone();
    let okx_book_xrp_handler_task = tokio::spawn(async move {
        let _ = okx_xrp_ob
            .run(xrp_cmd_rx, xrp_restart_sig_cln,xrp_ingestion_tx,xrp_audit_tx,xrp_shutdown_sig)
            .await;
    });

    let okx_ob_streaming_shutdown_signal = shutdown.clone();
    let mut okx_book_streaming_task = tokio::spawn(async move {
        okx_book_stream
            .start_stream(restart_signal_rx, okx_ob_streaming_shutdown_signal)
            .await;
    });

    let pipeline_logger_shutdown = shutdown.clone();
    let pipline_logger = Pipelinelogger::new(audit_rx, dead_pool);
    let pipline_logger_task = tokio::spawn(async move {
        pipline_logger
            .run(pipeline_logger_shutdown).await;
    });
    
    let snapshot_heartbeat_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(50));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let _ = ticker.tick().await;
    
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    for tx in &snapshot_heartbeat_senders {
                        let _ = tx.try_send(OrderBookCommand::TakeSnapShot);
                    }
                }
                _ = snapshot_heartbeat_shutdown.cancelled() => break,
            }
        }
        tracing::info!("Snapshot Heartbeat timer stopped");
    });

    let (quest_db_tx, quest_db_rx) = mpsc::channel::<Vec<BookSnapShot>>(1024);
    
    let snapshot_manager = SnapshotManager::new(ingestion_pipeline_rx, quest_db_tx);
    let snapshot_manager_shutdown = shutdown.clone();
    let ingestion_task = tokio::spawn(async move {
        snapshot_manager.run_ingestion_pipeline(snapshot_manager_shutdown).await;
    });
    
    let quest_db_client = QuestDBClient::new(quest_db_rx);
    let quest_db_client_shutdown = shutdown.clone();
    let quest_db_audit_tx = audit_tx.clone();
    let quest_db_client_task = tokio::spawn(async move {
        quest_db_client.run(quest_db_audit_tx,quest_db_client_shutdown).await;
    });
    
    let okx_book_handlers_task = {
        let mut _vec = Vec::with_capacity(4);
        _vec.push(okx_book_btc_handler_task);
        _vec.push(okx_book_eth_handler_task);
        _vec.push(okx_book_sol_handler_task);
        _vec.push(okx_book_xrp_handler_task);

        join_all(_vec)
    };
    tracing::info!("Okx local orderbook boot");

    tokio::select! {
        res = &mut okx_book_streaming_task => {
            match res {
                Ok(_) => tracing::info!("Order book stream task stopped naturally"),
                Err(e) => tracing::error!("Order book stream task panicked: {:?}", e),
            }
            shutdown.cancel();
        }
        _ = okx_book_handlers_task => {
            tracing::info!("Order book handler task stopped naturally");
            shutdown.cancel();
        }
        _ = ingestion_task => {
            tracing::info!("Order book snapshot manager handler task stopped naturally");
            shutdown.cancel();
        }
        _ = quest_db_client_task => {
            tracing::info!("Quest db client stop");
            shutdown.cancel();
        }
        _ = pipline_logger_task => {
            tracing::info!("Pipeline logegr stop");
            shutdown.cancel();
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT received, shutting down...");
            shutdown.cancel();
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), okx_book_streaming_task).await;
        }
    }
    tracing::info!("Shutdown complete");
}
