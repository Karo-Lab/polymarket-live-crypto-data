use crate::{
    traits::{
        ColumnValue, ExchangeAdapter, ExchangeConnectorAdapter, TableSchema,
        WsReader, WsWriter,
    },
};
use chrono::{DateTime, Utc};
use crc32fast::Hasher;
use derive_builder::Builder;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Display,
    io::Write,
    sync::Arc,
};
use tokio::{
    sync::{RwLock, mpsc}
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Utf8Bytes},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum OrderBookCommand {
    Update(Utf8Bytes),
    TakeSnapShot,
}

impl Display for OrderBookCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderBookCommand::Update(_) => write!(f, "Update"),
            OrderBookCommand::TakeSnapShot => write!(f, "Snapshot"),
        }
    }
}

#[derive(Debug)]
pub struct OrderBookL2 {
    pub exchange: Arc<String>,
    pub ts: i64,
    pub last_seq_id: i64,
    pub instrument_id: Arc<String>,
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
}

impl OrderBookL2 {
    pub fn new(exchange: String) -> Self {
        Self {
            exchange: Arc::new(exchange),
            ts: 0,
            last_seq_id: 0,
            instrument_id: Arc::new(String::new()),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
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
    pub exchange: Arc<String>,
    pub instrument_id: Arc<String>,
    pub receipt_time: i64,
    pub timestamp: i64,
    pub bids: Vec<Vec<Decimal>>,
    pub asks: Vec<Vec<Decimal>>,
}

impl BookSnapShot {
    pub fn bids_f64(&self) -> Vec<Vec<f64>> {
        self.bids
            .iter()
            .map(|inner_vec| {
                inner_vec
                    .iter()
                    .map(|v| v.normalize().to_f64().unwrap_or(0.0))
                    .collect()
            })
            .collect()
    }
    pub fn asks_f64(&self) -> Vec<Vec<f64>> {
        self.asks
            .iter()
            .map(|inner_vec| {
                inner_vec
                    .iter()
                    .map(|v| v.normalize().to_f64().unwrap_or(0.0))
                    .collect()
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct BookTbT {
    pub exchange: Arc<String>,
    pub instrument_id: Arc<String>,
    pub receipt_time: i64,
    pub timestamp: i64,
    pub side: Arc<String>,
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug)]
pub struct BookTbTBatch {
    pub exchange: Arc<str>,
    pub instrument_id: Arc<str>,
    pub exchange_ts: i64,
    pub timestamp: i64,
    pub side: Arc<str>,
    pub prices: Decimal,
    pub size: Decimal,
}

#[derive(Debug)]
pub struct LevelDelta<'b> {
    pub side: &'b str,
    pub price: Decimal,
    pub old_size: Decimal,
    pub new_size: Decimal,
    pub diff: Decimal,
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

#[derive(Debug, PartialEq, Eq)]
pub enum BookState {
    AwaitingSnapshot,
    Synced,
    Corrupted,
}

impl Display for BookState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BookState::AwaitingSnapshot => write!(f, "BookState::AwaitingSnapshot"),
            BookState::Synced => write!(f, "BookState::Synced"),
            BookState::Corrupted => write!(f, "BookState::Corrupted"),
        }
    }
}

#[derive(Debug)]
pub enum BookAuditEvent {
    SeqIdGap {
        symbol: String,
        expected: i64,
        actual: i64,
    },
    CheckSumFailure {
        symbol: String,
        expected: i32,
        actual: i32,
    },
    IngestFail {
        details: Option<serde_json::Value>,
    },
}

pub struct OrderBookActor<A: ExchangeAdapter> {
    adapter: A,
    command_receiver: mpsc::Receiver<OrderBookCommand>,
    restart_signal_tx: mpsc::Sender<Arc<String>>,
    ingestion_tx: mpsc::Sender<IngestionEvent>,
    audit_tx: mpsc::Sender<AuditEvent>,
    shutdown_token: CancellationToken,
    book_core: OrderBookL2,
    state: BookState,
}
impl<E> OrderBookActor<E>
where
    E: ExchangeAdapter,
{
    pub fn new(
        adapter: E,
        command_receiver: mpsc::Receiver<OrderBookCommand>,
        restart_signal_tx: mpsc::Sender<Arc<String>>,
        ingestion_tx: mpsc::Sender<IngestionEvent>,
        audit_tx: mpsc::Sender<AuditEvent>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            adapter,
            command_receiver,
            restart_signal_tx,
            ingestion_tx,
            audit_tx,
            shutdown_token,
            book_core: OrderBookL2::new(E::EXCHANGE_NAME.to_string()),
            state: BookState::AwaitingSnapshot,
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

    #[tracing::instrument(skip(self, command))]
    fn process(&mut self, command: OrderBookCommand) {
        match command {
            OrderBookCommand::Update(data) => {
                match serde_json::from_slice::<E::InputBookData<'_>>(data.as_bytes()) {
                    Ok(v) => {
                        let current_state =
                            std::mem::replace(&mut self.state, BookState::Corrupted);

                        self.state = match current_state {
                            BookState::AwaitingSnapshot => {
                                self.book_core.bids.clear();
                                self.book_core.asks.clear();
                                self.book_core.instrument_id =
                                    Arc::new(self.adapter.get_instrument(&v).to_string());
                                self.adapter.apply(&mut self.book_core, &v);
                                let incoming_seq = self.adapter.get_seq_id(&v);
                                self.book_core.last_seq_id = incoming_seq;
                                self.book_core.ts = self.adapter.get_timestamp(&v);
                                let state = BookState::Synced;
                                state
                            }
                            BookState::Synced => {
                                let mut state = BookState::Synced;
                                let incoming_seq_id = self.adapter.get_seq_id(&v);
                                let incoming_prev_seq = self.adapter.get_prev_seq_id(&v);
                                self.book_core.ts = self.adapter.get_timestamp(&v);

                                if incoming_prev_seq != 0
                                    && incoming_prev_seq != self.book_core.last_seq_id
                                {
                                    let audit_event = AuditEventBuilder::default()
                                        .topic("SequenceIdGap")
                                        .level(AuditLevel::Error)
                                        .message("Gapped deteced")
                                        .build()
                                        .map_err(|e| e.to_string());

                                    match audit_event {
                                        Ok(event) => {
                                            let _audit_seqence_failure =
                                                self.audit_tx.try_send(event);
                                            tracing::debug!(%state, id = incoming_seq_id,prev_id = self.book_core.last_seq_id, "Corrupted sequence detected: SeqGap");
                                        }
                                        Err(e) => {
                                            tracing::error!(%e, "Unable to create audit message")
                                        }
                                    }
                                    state = BookState::Corrupted;
                                } else {
                                    tracing::debug!(
                                        "BBO: {:?} - BAO: {:?}",
                                        self.book_core.bids.last_key_value(),
                                        self.book_core.asks.first_key_value()
                                    );
                                    if self.book_core.last_seq_id % 100 == 0 {
                                        state = if self
                                            .adapter
                                            .verify_integrity(&mut self.book_core, &v)
                                        {
                                            BookState::Synced
                                        } else {
                                            let audit_event = AuditEventBuilder::default()
                                                .topic("SequenceIdGap")
                                                .level(AuditLevel::Error)
                                                .message("CheckSum Failure")
                                                .build()
                                                .map_err(|e| e.to_string());

                                            match audit_event {
                                                Ok(event) => {
                                                    let _audit_seqence_failure =
                                                        self.audit_tx.try_send(event);
                                                    tracing::debug!(%state, id = incoming_seq_id,prev_id = self.book_core.last_seq_id, "Corrupted sequence detected: CheckSum Failure");
                                                }
                                                Err(e) => {
                                                    tracing::error!(%e, "Unable to create audit message")
                                                }
                                            }
                                            BookState::Corrupted
                                        };
                                    }
                                }
                                self.book_core.last_seq_id = incoming_seq_id;
                        
                                if let Some(ts_now) = chrono::Utc::now().timestamp_nanos_opt() {
                                    let timestamp = self.book_core.ts.clone() * 1_000_000;
                                    let exchange = self.book_core.exchange.clone();
                                    let instrument_id = self.book_core.instrument_id.clone();
                                    let delta_changes = self.adapter.apply(&mut self.book_core, &v);
                                    tracing::debug!(?delta_changes, "Delta changes");
                                    for delta in delta_changes {
                                        let tick = BookTbT {
                                            exchange: exchange.clone(),
                                            instrument_id: instrument_id.clone(),
                                            side: Arc::new(delta.side.to_string()),
                                            price: delta.price,
                                            size: delta.new_size,
                                            timestamp,
                                            receipt_time: ts_now
                                        };
                                        let _ = self
                                            .ingestion_tx
                                            .try_send(IngestionEvent::Tick(tick));
                                    }
                                }

                                state
                            }
                            BookState::Corrupted => {
                                let _send_restart = self
                                    .restart_signal_tx
                                    .try_send(self.book_core.instrument_id.clone());
                                let state = BookState::AwaitingSnapshot;
                                tracing::debug!(%state);
                                state
                            }
                        };
                    }
                    Err(e) => {
                        tracing::debug!(error = e.to_string(), "Unable to serialized");
                    }
                }
            }
            OrderBookCommand::TakeSnapShot => {
                let depth = 50;
                if let Some(ts_now) = chrono::Utc::now().timestamp_nanos_opt() {
                    let mut bid_prices = Vec::with_capacity(depth);
                    let mut bid_sizes = Vec::with_capacity(depth);

                    let mut ask_prices = Vec::with_capacity(depth);
                    let mut ask_sizes = Vec::with_capacity(depth);

                    for ((bp, bsz), (ap, asz)) in self
                        .book_core
                        .bids
                        .iter()
                        .rev()
                        .take(depth)
                        .zip(self.book_core.asks.iter().take(depth))
                    {
                        bid_prices.push(*bp);
                        bid_sizes.push(*bsz);

                        ask_prices.push(*ap);
                        ask_sizes.push(*asz);
                    }

                    let exchange_ts = self.book_core.ts * 1_000_000;
                    if exchange_ts != 0 {
                        let snapshot = BookSnapShot {
                            timestamp: exchange_ts,
                            receipt_time: ts_now,
                            exchange: self.book_core.exchange.clone(),
                            instrument_id: self.book_core.instrument_id.clone(),
                            bids: vec![bid_prices, bid_sizes],
                            asks: vec![ask_prices, ask_sizes],
                        };

                        let _ = self
                            .ingestion_tx
                            .try_send(IngestionEvent::Snapshot(snapshot));
                    }
                }
            }
        }
    }
}

pub struct GenericExchangeConnector<A: ExchangeConnectorAdapter> {
    adapter: A,
    data_sender_map: HashMap<u128, mpsc::Sender<OrderBookCommand>>,
    restart_signal: mpsc::Receiver<Arc<String>>,
    shutdown_token: CancellationToken,
    active_subs: Arc<RwLock<HashSet<String>>>,
}

impl<A: ExchangeConnectorAdapter> GenericExchangeConnector<A> {
    pub fn new(
        adapter: A,
        data_sender_map: HashMap<u128, mpsc::Sender<OrderBookCommand>>,
        restart_signal: mpsc::Receiver<Arc<String>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            adapter,
            data_sender_map,
            restart_signal,
            shutdown_token,
            active_subs: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn run(mut self) {
        let mut backoff = 1u64;
        loop {
            let url = self.adapter.get_url();
            let mut _is_reconnected = false;

            match connect_async(url).await {
                Ok((websocket_, _)) => {
                    backoff = 1u64;
                    let (mut ws_write, mut ws_read) = websocket_.split();
                    let init_instruments: Vec<String> = self
                        .data_sender_map
                        .keys()
                        .map(|&sym| {
                            String::from_utf8_lossy(&sym.to_le_bytes())
                                .trim_end_matches('\0')
                                .to_string()
                        })
                        .collect();

                    let msg = self.adapter.create_subscription(&init_instruments, false);
                    for inst in init_instruments.into_iter() {
                        let mut writer = self.active_subs.write().await;
                        tracing::info!(
                            exchange = self.adapter.get_source_name(),
                            instrument = inst,
                            "Subscribed to"
                        );
                        writer.insert(inst);
                        drop(writer);
                    }
                    if let Ok(_) = ws_write.send(msg).await {
                        self.handle_connection(&mut ws_write, &mut ws_read).await;
                    } else {
                        tracing::info!("Error to send subscribe message")
                    }
                }
                Err(_e) => {}
            }
            tracing::info!(%backoff,"Sleep for");
            tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(60);
        }
    }

    async fn handle_connection(&mut self, ws_writer: &mut WsWriter, ws_reader: &mut WsReader) {
        let ping_duration = self
            .adapter
            .ping_interval()
            .unwrap_or(tokio::time::Duration::from_secs(30));
        let mut ping_timer = tokio::time::interval(ping_duration);
        ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(signal) = self.restart_signal.recv() => {
                    let check = {
                        let reader = self.active_subs.read().await;
                        reader.contains(signal.as_ref())
                    };
                    // if check {
                    //     tracing::info!("Corrputed Signal received {:?}", signal);
                    //     let symbols = [signal.as_ref().to_owned()];

                    //     let un_sub = self.adapter.create_subscription(&symbols, true);
                    //     let _ = ws_writer.send(un_sub).await;

                    //     let re_sub = self.adapter.create_subscription(&symbols, false);
                    //     let _ = ws_writer.send(re_sub).await;
                    // } else {
                    //     tracing::info!("Corrputed Signal failed received {:?}", signal);
                    // }
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
                        Err(_e) => {
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

#[derive(Debug)]
pub enum IngestionEvent {
    Snapshot(BookSnapShot),
    Tick(BookTbT),
}

impl TableSchema for BookSnapShot {
    fn table_name(&self) -> &str {
        "exchanges_live_price"
    }
    fn get_columns(&self) -> Vec<(&str, ColumnValue<'_>)> {
        vec![
            ("symbol", ColumnValue::Symbol(&self.instrument_id)),
            ("receipt_time", ColumnValue::Timestamp(self.receipt_time)),
            ("exchange", ColumnValue::Varchar(&self.exchange)),
            ("bids", ColumnValue::Array2dDouble(self.bids_f64())),
            ("asks", ColumnValue::Array2dDouble(self.asks_f64())),
            ("timestamp", ColumnValue::Timestamp(self.timestamp)),
        ]
    }
}

impl TableSchema for BookTbT {
    fn table_name(&self) -> &str {
        "exchanges_live_tbt"
    }
    fn get_columns(&self) -> Vec<(&str, ColumnValue<'_>)> {
        vec![
            ("symbol", ColumnValue::Symbol(&self.instrument_id)),
            ("receipt_time", ColumnValue::Timestamp(self.receipt_time)),
            ("exchange", ColumnValue::Varchar(&self.exchange)),
            ("side", ColumnValue::Varchar(&self.side)),
            (
                "price",
                ColumnValue::Double(self.price.normalize().to_f64().unwrap_or(0.0)),
            ),
            (
                "size",
                ColumnValue::Double(self.size.normalize().to_f64().unwrap_or(0.0)),
            ),
            ("timestamp", ColumnValue::Timestamp(self.timestamp)),
        ]
    }
}

impl TableSchema for IngestionEvent {
    fn table_name(&self) -> &str {
        match self {
            IngestionEvent::Snapshot(s) => s.table_name(),
            IngestionEvent::Tick(t) => t.table_name(),
        }
    }
    fn get_columns(&self) -> Vec<(&str, ColumnValue<'_>)> {
        match self {
            IngestionEvent::Snapshot(s) => s.get_columns(),
            IngestionEvent::Tick(t) => t.get_columns(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum AuditLevel {
    Info,
    Warning,
    Error,
    Critical,
}

impl AsRef<str> for AuditLevel {
    fn as_ref(&self) -> &str {
        match self {
            AuditLevel::Critical => "critical",
            AuditLevel::Error => "error",
            AuditLevel::Warning => "warning",
            AuditLevel::Info => "info",
        }
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned")]
#[builder(setter(into))]
pub struct AuditEvent {
    #[builder(default = "Utc::now()")]
    pub timestamp: DateTime<Utc>,
    pub topic: String,
    pub level: AuditLevel,
    pub message: String,

    #[builder(default, setter(strip_option))]
    pub details: Option<Value>,
}

impl std::fmt::Display for AuditEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ts = self.timestamp.to_rfc3339();
        write!(
            f,
            "Timestamp: {} - topic: {} - level: {} - message: {}",
            ts,
            self.topic,
            self.level.as_ref(),
            self.message
        )
    }
}

impl AuditEvent {
    pub fn new(
        topic: impl Into<String>,
        msg: impl Into<String>,
        details: Option<Value>,
        level: AuditLevel,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            topic: topic.into(),
            level,
            message: msg.into(),
            details,
        }
    }
}

