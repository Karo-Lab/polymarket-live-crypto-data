use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, HashMap, HashSet}, io::Write, sync::Arc
};
use bytes::Bytes;
use crc32fast::Hasher;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::{sync::{RwLock, mpsc}};
use tokio_tungstenite::{connect_async, tungstenite::{Message, Utf8Bytes}};
use tokio_util::sync::CancellationToken;
use futures_util::{StreamExt, SinkExt};
use crate::traits::{ExchangeAdapter, ExchangeConnectorAdapter, WsReader, WsWriter};


#[derive(Debug)]
pub enum OrderBookCommand {
    Update(Utf8Bytes),
    TakeSnapShot
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderBookL2 {
    pub exchange: String,
    pub ts: String,
    pub las_seq_id: u64,
    pub instrument_id: String,
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
}

impl OrderBookL2 {
    pub fn new(exchange: String) -> Self {
        Self {
            exchange,
            ts: String::new(),
            las_seq_id: 0,
            instrument_id: String::new(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new()
        }
    }
    
    pub fn modify_bid(&mut self, price: Decimal, size: Decimal) {
        if size.is_zero() {
            self.bids.remove(&price);
        } else {
            self.bids.insert(price, size);
        }
    }
    pub fn modify_asks(&mut self, price: Decimal, size: Decimal) {
        if size.is_zero() {
            self.asks.remove(&price);
        } else {
            self.asks.insert(price, size);
        }
    }
}

#[derive(Debug)]
pub struct BookSnapShot {
    pub instrument_id: String,
    pub exchange_ts: i64,
    pub timestamp: i64,
    pub bids: Vec<Vec<Decimal>>,
    pub asks: Vec<Vec<Decimal>>
}

#[derive(Debug)]
pub struct BookTbT {
    pub instrument_id: String,
    pub exchange_ts: i64,
    pub timestamp: i64,
    pub price: Decimal,
    pub size: Decimal
}

pub struct HashWriter<'a>(pub &'a mut Hasher);

impl<'a> Write for HashWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }
    
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub enum BookState {
    AwaitingSnapshot,
    Synced,
    Corrupted
}

#[derive(Debug)]
pub enum BookAuditEvent {
    SeqIdGap {
        symbol: String, expected: u64, actual: u64
    },
    CheckSumFailure {
        symbol: String, expected: i32, actual: i32
    },
    IngestFail {
        details: Option<serde_json::Value>
    }
}

pub struct OrderBookActor<A: ExchangeAdapter> {
    adapter: A,
    command_receiver: mpsc::Receiver<OrderBookCommand>,
    restart_signal_tx: mpsc::Sender<String>,
    snapshot_ingestion_tx: mpsc::Sender<BookSnapShot>,
    book_audit_tx: mpsc::Sender<BookAuditEvent>,
    shutdown_token: CancellationToken,
    book_core: OrderBookL2,
    state: BookState
}
impl<A: ExchangeAdapter> OrderBookActor<A> {
    pub fn new(
        adapter: A, 
        command_receiver: mpsc::Receiver<OrderBookCommand>,
        restart_signal_tx: mpsc::Sender<String>,
        snapshot_ingestion_tx: mpsc::Sender<BookSnapShot>,
        book_audit_tx: mpsc::Sender<BookAuditEvent>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            adapter,
            command_receiver,
            restart_signal_tx,
            snapshot_ingestion_tx,
            book_audit_tx,
            shutdown_token,
            book_core: OrderBookL2::new(A::EXCHANGE_NAME.to_string()),
            state: BookState::AwaitingSnapshot
        }
    }
    
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.command_receiver.recv() => {
                    match cmd {
                        Some(command) => {
                            self.process(command);
                        }
                        None => {
                            break
                        }
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    return ;
                }
                else => break
            }
        }
    }
    
    fn process(&mut self, command: OrderBookCommand) {
        match command {
            OrderBookCommand::Update(data) => {
                match serde_json::from_slice::<A::InputBookData<'_>>(data.as_bytes()) {
                    Ok(v) => {
                        let current_state = std::mem::replace(&mut self.state, BookState::Corrupted);
                        
                        self.state = match current_state {
                            BookState::AwaitingSnapshot => {
                                self.book_core.bids.clear();
                                self.book_core.asks.clear();
                                                                
                                self.adapter.apply(&mut self.book_core, &v);
                                
                                BookState::Synced
                            },
                            BookState::Synced => {
                                let mut state = BookState::Synced;
                                let incoming_seq = self.adapter.get_seq_id(&v);
                                
                                if incoming_seq != 0 && incoming_seq != self.book_core.las_seq_id {
                                    let _audit_seqence_failure = self.book_audit_tx.try_send(BookAuditEvent::SeqIdGap { 
                                        symbol: self.book_core.instrument_id.clone(), 
                                        expected: incoming_seq, 
                                        actual: self.book_core.las_seq_id, 
                                    });
                                    
                                    state = BookState::Corrupted;
                                }
                                
                                self.adapter.apply(&mut self.book_core, &v);
                                
                                if self.book_core.las_seq_id % 100 == 0 {
                                    state = if self.adapter.verify_integrity(&mut self.book_core, &v) {BookState::Synced} else {BookState::Corrupted};
                                }
                                state
                            }
                            BookState::Corrupted => {
                                let _send_restart =self.restart_signal_tx.try_send(self.book_core.instrument_id.clone());
                                
                                BookState::AwaitingSnapshot
                            }
                        };
                    }
                    _ => {}
                }
            }
            OrderBookCommand::TakeSnapShot => {
                let depth = 50;
                if let Some(ts_now) = chrono::Utc::now().timestamp_nanos_opt() {
                    let mut bid_prices = Vec::with_capacity(depth);
                    let mut bid_sizes = Vec::with_capacity(depth);
                    
                    let mut ask_prices = Vec::with_capacity(depth);
                    let mut ask_sizes = Vec::with_capacity(depth);
                    
                    let instrument_id_cln = self.book_core.instrument_id.clone();
                    
                    for ((bp,bsz),(ap,asz)) in self.book_core.bids.iter().rev().take(depth).zip(self.book_core.asks.iter().take(depth)) {
                        bid_prices.push(*bp);
                        bid_sizes.push(*bsz);
                        
                        ask_prices.push(*ap);
                        ask_sizes.push(*asz);
                    }
                    
                    if let Ok(value) = self.book_core.ts.parse::<i64>() {
                        let exchange_ts = value * 1_000_000;
                        let snapshot = BookSnapShot {
                            instrument_id: instrument_id_cln,
                            exchange_ts,
                            timestamp: ts_now,
                            bids: vec![bid_prices, bid_sizes],
                            asks: vec![ask_prices, ask_sizes]                            
                        };
                        
                        let _ = self.snapshot_ingestion_tx.try_send(snapshot);
                    }
                }
            }
        }
    }
}

pub struct GenericExchangeConnector<A: ExchangeConnectorAdapter> {
    adapter: A,
    data_sender_map: HashMap<u128, mpsc::Sender<OrderBookCommand>>,
    restart_signal: mpsc::Receiver<String>,
    shutdown_token: CancellationToken,
    active_subs: Arc<RwLock<HashSet<String>>>
}

impl<A: ExchangeConnectorAdapter> GenericExchangeConnector<A> {
    pub fn new(
        adapter: A, 
        data_sender_map: HashMap<u128, mpsc::Sender<OrderBookCommand>>,
        restart_signal: mpsc::Receiver<String>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            adapter,
            data_sender_map,
            restart_signal,
            shutdown_token,
            active_subs: Arc::new(RwLock::new(HashSet::new()))
        }
    }
    
    pub async fn run(mut self) {
        loop {
            let url = self.adapter.get_url();
            
            let mut backoff = 1u64;
            let mut is_reconnected = false;
            
            match connect_async(url).await {
                Ok((websocket_, _)) => {
                    let (mut ws_write, mut ws_read) = websocket_.split();
                    self.handle_connection(&mut ws_write, &mut ws_read);
                }
                Err(e) => {
                    
                }
            }
            
            tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
            backoff = (backoff * 2 ).min(60);
        }
    }
    
    async fn handle_connection(&mut self, ws_writer: &mut WsWriter, ws_reader: &mut WsReader) {        
        let ping_duration = self.adapter.ping_interval().unwrap_or(tokio::time::Duration::from_secs(30));
        let mut ping_timer = tokio::time::interval(ping_duration);
        ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        loop {
            tokio::select! {
                Some(signal) = self.restart_signal.recv() => {
                    let symbol = {
                        let reader = self.active_subs.read().await;
                        reader.get(&signal).cloned() 
                    };                 
                    if let Some(symbol_str) = symbol {
                        let symbols = [symbol_str]; 
                        
                        let un_sub = self.adapter.create_subscription(&symbols, true);
                        let _ = ws_writer.send(un_sub).await;
                        
                        let re_sub = self.adapter.create_subscription(&symbols, false);
                        let _ = ws_writer.send(re_sub).await;
                    }
                }
                
                Some(msg) = ws_reader.next() => {
                    match msg {
                        Ok(Message::Text(txt)) => {
                            if let Ok(value) = self.adapter.route_message(&txt) {
                                if let Some(tx) = self.data_sender_map.get(&value) {
                                    let _ = tx.try_send(OrderBookCommand::Update(txt.clone()));
                                }
                            } else {
                                continue
                            }
                        }
                        Ok(Message::Ping(p)) => {
                            let _ = ws_writer.send(Message::Pong(p)).await;
                        }
                        Ok(Message::Close(_)) => {
                            break;
                        }
                        Err(e) => {
                            break;
                        }
                        _ => {}
                    }
                }
                _ = ping_timer.tick() => {
                    self.adapter.create_ping(ws_writer);
                }
                _ = self.shutdown_token.cancelled() => {
                    return;
                }
            }
        }
    }
}