use core::models::{BookSnapShot, GenericExchangeConnector, OrderBookActor};

use exchanges::{okx::OkxAdapter, polymarket::PolyCLOBAdapter};
use rustls::crypto::{CryptoProvider, ring::default_provider};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::SystemLogging;


mod config;

#[tokio::main]
async fn main() {
    let _log_guard = SystemLogging::init_logging();
    let _install_tls = {
        CryptoProvider::install_default(default_provider())
            .expect("Unable to install rusttls crypto provider")
    };    
    
    let shutdown_token = CancellationToken::new();
    
    let (restart_signal_tx, restart_signal_rx) = mpsc::channel(1);
    let (snapshot_ingestion_tx, snapshot_ingestion_rx) = mpsc::channel::<BookSnapShot>(1024);
    let (book_audit_tx, snapshot_ingestion_rx) = mpsc::channel(1024);

    // Polymarket book config
    let (poly_book_cmd_tx, book_cmd_rx) = mpsc::channel(1024);
    
    let okx_book = OrderBookActor::new(
        OkxAdapter, 
        book_cmd_rx, 
        restart_signal_tx, 
        snapshot_ingestion_tx, 
        book_audit_tx, 
        shutdown_token
    );
    
    // let book_conn = GenericExchangeConnector::new(
    //     OkxConnectorAdapter, 
    //     data_sender_map, 
    //     restart_signal, 
    //     shutdown_token
    // );
}
