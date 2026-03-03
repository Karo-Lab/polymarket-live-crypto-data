use crate::{
    constants::{BACKOFF_PERIOD, TASK_DURATION_THRESHOLD},
    log_error, log_info,
    telementry::{
        AuditEvent, AuditEventBuilder, AuditLevel, ErrorPayload, ErrorSeverity, IngestionEvent,
        LogEventCategory,
    },
    traits::{ColumnValue, ExchangeConnectorAdapter, TableSchema, WsReader, WsWriter},
};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use slotmap::{DenseSlotMap, SecondaryMap, new_key_type};
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    sync::Arc,
};
use tokio::{
    sync::{RwLock, broadcast, mpsc},
    time::{Duration, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug)]
pub enum OrderBookCommand {
    Update(NormalizedBookEvent),
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

impl AsRef<str> for OrderBookCommand {
    fn as_ref(&self) -> &str {
        match self {
            OrderBookCommand::Update(_) => "Update",
            OrderBookCommand::TakeSnapShot => "Snapshot",
        }
    }
}

pub const DEFAULT_PRICE_SCALE: u32 = 10_000;
pub const DEFAULT_QTY_SCALE: u64 = 1_000_000;
const DEFAULT_PUBLISH_DEPTH: usize = 50;
const BINANCE_LOCAL_BOOK_DEPTH: usize = 200;
const BINANCE_PUBLISH_DEPTH: usize = 20;

new_key_type! { pub struct SymbolId; }

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub u32);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Size(pub u64);

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum BookSide {
    Bid = 0,
    Ask = 1,
}

#[derive(Debug, Clone, Copy)]
pub struct BookLevel {
    pub price: Price,
    pub size: Size,
}

#[derive(Debug, Clone)]
pub struct NormalizedBookEvent {
    pub exchange: String,
    pub symbol: String,
    pub timestamp: i64,
    pub seq_id: i64,
    pub prev_seq_id: i64,
    pub is_snapshot: bool,
    pub price_scale: u32,
    pub size_scale: u64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

#[derive(Debug, Clone)]
pub struct RestartSignal {
    pub exchange: Arc<String>,
    pub instrument_id: Arc<String>,
}

/// Data layout of OrderBookIndexer
///
/// 1. The Gateway (id_resolver):
///    Translates the external exchange string ID into our ultra-fast internal ECS Entity ID.
///    e.g., "0xPolymarketTokenYES" -> ClobTokenId(1)
///
/// 2. The Main ECS Table (Struct of Arrays):
/// Row Entity  | Cold Metadata (tokens) | Hot Bids Column (bids)          | Hot Asks Column (asks)
/// ClobTokenId | tick_size (u32)        | prices (Price)  | sizes (Size)  | prices (Price)  | sizes (Size)
/// 1           | 10                     | [99, 98, 97]    | [500, 100, 0] | [100, 101]      | [200, 50]
/// 2           | 1                      | [450, 440, 430] | [1000, 50, 0] | [452, 455]      | [10, 20]
#[derive(Debug, Default)]
pub struct OrderBookIndexer {
    pub id_resolver: HashMap<String, SymbolId>,
    pub symbols: DenseSlotMap<SymbolId, OrderBookMeta>,
    pub bids: SecondaryMap<SymbolId, OrderBookData>,
    pub asks: SecondaryMap<SymbolId, OrderBookData>,
}

#[derive(Debug)]
pub struct OrderBookMeta {
    pub exchange: String,
}

#[derive(Debug, Default)]
pub struct OrderBookData {
    pub prices: Vec<Price>,
    pub sizes: Vec<Size>,
}

impl OrderBookIndexer {
    pub fn symbol_key(exchange: &str, symbol: &str) -> String {
        format!("{exchange}:{symbol}")
    }

    pub fn new() -> Self {
        Self {
            id_resolver: HashMap::new(),
            symbols: DenseSlotMap::with_capacity_and_key(200),
            bids: SecondaryMap::with_capacity(200),
            asks: SecondaryMap::with_capacity(200),
        }
    }

    pub fn new_book(&mut self, exchange: String, symbol: String) -> SymbolId {
        let key = Self::symbol_key(&exchange, &symbol);
        if let Some(&existing_id) = self.id_resolver.get(&key) {
            return existing_id;
        }

        let id = self.symbols.insert(OrderBookMeta { exchange });

        self.bids.insert(id, OrderBookData::default());
        self.asks.insert(id, OrderBookData::default());

        self.id_resolver.insert(key, id);

        id
    }

    pub fn clear_book(&mut self, symbol_idx: SymbolId) {
        if let Some(book) = self.bids.get_mut(symbol_idx) {
            book.prices.clear();
            book.sizes.clear();
        }
        if let Some(book) = self.asks.get_mut(symbol_idx) {
            book.prices.clear();
            book.sizes.clear();
        }
    }

    pub fn apply_book_update(
        &mut self,
        symbol_idx: SymbolId,
        price: Price,
        new_size: Size,
        side: BookSide,
    ) {
        self.appy_book_update(symbol_idx, price, new_size, side);
    }

    pub fn appy_book_update(
        &mut self,
        symbol_idx: SymbolId,
        price: Price,
        new_size: Size,
        side: BookSide,
    ) {
        let book = match side {
            BookSide::Bid => match self.bids.get_mut(symbol_idx) {
                Some(b) => b,
                None => return,
            },
            BookSide::Ask => match self.asks.get_mut(symbol_idx) {
                Some(b) => b,
                None => return,
            },
        };

        let search_price = match side {
            BookSide::Bid => book.prices.binary_search_by(|p| price.cmp(p)),
            BookSide::Ask => book.prices.binary_search_by(|p| p.cmp(&price)),
        };

        match search_price {
            Ok(index) => {
                if new_size.0 == 0 {
                    book.prices.remove(index);
                    book.sizes.remove(index);
                } else {
                    book.sizes[index] = new_size;
                }
            }
            Err(index) => {
                if new_size.0 > 0 {
                    book.prices.insert(index, price);
                    book.sizes.insert(index, new_size);
                }
            }
        }
    }

    pub fn truncate_book_depth(&mut self, symbol_idx: SymbolId, depth: usize) {
        if let Some(bids) = self.bids.get_mut(symbol_idx) {
            bids.prices.truncate(depth);
            bids.sizes.truncate(depth);
        }

        if let Some(asks) = self.asks.get_mut(symbol_idx) {
            asks.prices.truncate(depth);
            asks.sizes.truncate(depth);
        }
    }

    pub fn best_bid(&self, symbol_key: &str) -> Option<(Price, Size)> {
        let token = self.id_resolver.get(symbol_key)?;

        let book = self.bids.get(*token)?;

        let best_price = book.prices.first().copied()?;
        let best_size = book.sizes.first().copied()?;

        Some((best_price, best_size))
    }

    pub fn best_ask(&self, symbol_key: &str) -> Option<(Price, Size)> {
        let token = self.id_resolver.get(symbol_key)?;

        let book = self.asks.get(*token)?;

        let best_price = book.prices.first().copied()?;
        let best_size = book.sizes.first().copied()?;

        Some((best_price, best_size))
    }
}

#[derive(Debug, Default)]
pub struct IndexerStats {
    pub ws_connects: u64,
    pub ws_reconnects: u64,
    pub frames_total: u64,
    pub frames_parsed: u64,
    pub parse_errors: u64,
    pub snapshots_seen: u64,
    pub ticks_seen: u64,
    pub last_trade_seen: u64,
    pub resolved_seen: u64,
    pub resolved_tokens_seen: u64,
    pub subscribe_sent: u64,
    pub unsubscribe_sent: u64,
    pub book_updates_applied: u64,
    pub side_parse_errors: u64,
    pub value_parse_errors: u64,
    pub socket_read_errors: u64,
    pub socket_write_errors: u64,
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
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

impl AsRef<str> for BookState {
    fn as_ref(&self) -> &str {
        match self {
            BookState::AwaitingSnapshot => "BookState::AwaitingSnapshot",
            BookState::Synced => "BookState::Synced",
            BookState::Corrupted => "BookState::Corrupted",
        }
    }
}

#[derive(Debug, Clone)]
struct SymbolCursor {
    symbol_id: SymbolId,
    exchange: Arc<String>,
    instrument_id: Arc<String>,
    step_size: u64,
    state: BookState,
    last_seq_id: i64,
    last_ts: i64,
}

#[derive(Debug)]
pub struct OrderBooksIndexerWorker {
    command_receiver: mpsc::Receiver<OrderBookCommand>,
    restart_signal_tx: broadcast::Sender<RestartSignal>,
    ingestion_tx: mpsc::Sender<IngestionEvent>,
    audit_tx: mpsc::Sender<AuditEvent>,
    shutdown_token: CancellationToken,
    book_indexer: OrderBookIndexer,
    symbol_cursors: HashMap<String, SymbolCursor>,
}

impl OrderBooksIndexerWorker {
    pub fn new(
        command_receiver: mpsc::Receiver<OrderBookCommand>,
        restart_signal_tx: broadcast::Sender<RestartSignal>,
        ingestion_tx: mpsc::Sender<IngestionEvent>,
        audit_tx: mpsc::Sender<AuditEvent>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            command_receiver,
            restart_signal_tx,
            ingestion_tx,
            audit_tx,
            shutdown_token,
            book_indexer: OrderBookIndexer::new(),
            symbol_cursors: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        tracing::info!("Starting orderbook indexer worker");
        loop {
            if self.shutdown_token.is_cancelled() {
                log_info!(
                    LogEventCategory::System,
                    "shutdown",
                    "orderbook_indexer_worker",
                    "orderbook_indexer_worker shutdown"
                );
                break;
            }

            tokio::select! {
                cmd = self.command_receiver.recv() => {
                    match cmd {
                        Some(command) => self.process(command),
                        None => break,
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    log_info!(LogEventCategory::System, "shutdown", "ecs_orderbook", "ECS orderbook shutdown");
                    break;
                }
                else => break
            }
        }
    }

    fn process(&mut self, command: OrderBookCommand) {
        match command {
            OrderBookCommand::Update(event) => self.handle_event(event),
            OrderBookCommand::TakeSnapShot => self.emit_snapshots(),
        }
    }

    fn handle_event(&mut self, event: NormalizedBookEvent) {
        let symbol_key = OrderBookIndexer::symbol_key(&event.exchange, &event.symbol);
        let symbol_id = if let Some(existing_id) = self.book_indexer.id_resolver.get(&symbol_key) {
            *existing_id
        } else {
            self.book_indexer
                .new_book(event.exchange.clone(), event.symbol.clone())
        };

        let cursor = self
            .symbol_cursors
            .entry(symbol_key)
            .or_insert_with(|| SymbolCursor {
                symbol_id,
                exchange: Arc::new(event.exchange.clone()),
                instrument_id: Arc::new(event.symbol.clone()),
                step_size: event.size_scale,
                state: BookState::AwaitingSnapshot,
                last_seq_id: 0,
                last_ts: 0,
            })
            .clone();

        if event.is_snapshot {
            self.book_indexer.clear_book(symbol_id);
            self.apply_side_updates(symbol_id, &event.bids, BookSide::Bid);
            self.apply_side_updates(symbol_id, &event.asks, BookSide::Ask);
            if let Some(local_depth) = Self::local_depth_for_exchange(&event.exchange) {
                self.book_indexer
                    .truncate_book_depth(symbol_id, local_depth);
            }

            if let Some(cursor) = self.symbol_cursors.get_mut(&OrderBookIndexer::symbol_key(
                &event.exchange,
                &event.symbol,
            )) {
                cursor.state = BookState::Synced;
                cursor.last_seq_id = event.seq_id;
                cursor.last_ts = event.timestamp;
                cursor.step_size = event.size_scale;
            }
            return;
        }

        if cursor.state != BookState::Synced {
            return;
        }

        if event.prev_seq_id != 0 && event.prev_seq_id != cursor.last_seq_id {
            if let Some(c) = self.symbol_cursors.get_mut(&OrderBookIndexer::symbol_key(
                &event.exchange,
                &event.symbol,
            )) {
                c.state = BookState::Corrupted;
            }

            self.mark_corrupted(&event);

            if let Some(c) = self.symbol_cursors.get_mut(&OrderBookIndexer::symbol_key(
                &event.exchange,
                &event.symbol,
            )) {
                c.state = BookState::AwaitingSnapshot;
            }
            return;
        }

        self.apply_side_updates(symbol_id, &event.bids, BookSide::Bid);
        self.apply_side_updates(symbol_id, &event.asks, BookSide::Ask);
        if let Some(local_depth) = Self::local_depth_for_exchange(&event.exchange) {
            self.book_indexer
                .truncate_book_depth(symbol_id, local_depth);
        }
        self.emit_ticks(&cursor, &event);

        if let Some(c) = self.symbol_cursors.get_mut(&OrderBookIndexer::symbol_key(
            &event.exchange,
            &event.symbol,
        )) {
            c.last_seq_id = event.seq_id;
            c.last_ts = event.timestamp;
            c.state = BookState::Synced;
            c.step_size = event.size_scale;
        }
    }

    fn local_depth_for_exchange(exchange: &str) -> Option<usize> {
        if exchange.eq_ignore_ascii_case("binance") {
            return Some(BINANCE_LOCAL_BOOK_DEPTH);
        }

        None
    }

    fn publish_depth_for_exchange(exchange: &str) -> usize {
        if exchange.eq_ignore_ascii_case("binance") {
            return BINANCE_PUBLISH_DEPTH;
        }

        DEFAULT_PUBLISH_DEPTH
    }

    fn apply_side_updates(&mut self, symbol_id: SymbolId, levels: &[BookLevel], side: BookSide) {
        for level in levels {
            self.book_indexer
                .apply_book_update(symbol_id, level.price, level.size, side);
        }
    }

    fn mark_corrupted(&mut self, event: &NormalizedBookEvent) {
        let audit_event = AuditEventBuilder::default()
            .topic("SequenceIdGap")
            .level(AuditLevel::Error)
            .message(format!(
                "Detected sequence gap for {}:{}",
                event.exchange, event.symbol
            ))
            .build()
            .map_err(|e| e.to_string());

        match audit_event {
            Ok(audit) => {
                let _ = self.audit_tx.try_send(audit);
            }
            Err(e) => {
                let payload =
                    ErrorPayload::new("audit_error", &e, ErrorSeverity::Recoverable, true);
                log_error!(LogEventCategory::Audit, "audit", "ecs_orderbook", payload);
            }
        }

        let _ = self.restart_signal_tx.send(RestartSignal {
            exchange: Arc::new(event.exchange.clone()),
            instrument_id: Arc::new(event.symbol.clone()),
        });
    }

    fn emit_ticks(&mut self, cursor: &SymbolCursor, event: &NormalizedBookEvent) {
        let Some(receipt_time) = chrono::Utc::now().timestamp_nanos_opt() else {
            return;
        };

        let exchange_ts = event.timestamp * 1_000_000;
        let bid = Arc::new("bid".to_string());
        let ask = Arc::new("ask".to_string());
        let should_filter = event.exchange.eq_ignore_ascii_case("binance");
        let publish_depth = Self::publish_depth_for_exchange(&event.exchange);

        let bid_filter: Option<Vec<Price>> = if should_filter {
            self.book_indexer.bids.get(cursor.symbol_id).map(|book| {
                book.prices
                    .iter()
                    .take(publish_depth)
                    .copied()
                    .collect::<Vec<_>>()
            })
        } else {
            None
        };
        let ask_filter: Option<Vec<Price>> = if should_filter {
            self.book_indexer.asks.get(cursor.symbol_id).map(|book| {
                book.prices
                    .iter()
                    .take(publish_depth)
                    .copied()
                    .collect::<Vec<_>>()
            })
        } else {
            None
        };

        for level in &event.bids {
            if should_filter
                && !bid_filter
                    .as_ref()
                    .is_some_and(|prices| prices.iter().any(|price| *price == level.price))
            {
                continue;
            }
            let _ = self.ingestion_tx.try_send(IngestionEvent::Tick(BookTbT {
                exchange: cursor.exchange.clone(),
                instrument_id: cursor.instrument_id.clone(),
                receipt_time,
                timestamp: exchange_ts,
                side: bid.clone(),
                price: Self::decimal_from_price(level.price, event.price_scale),
                size: Self::decimal_from_size(level.size, event.size_scale),
            }));
        }

        for level in &event.asks {
            if should_filter
                && !ask_filter
                    .as_ref()
                    .is_some_and(|prices| prices.iter().any(|price| *price == level.price))
            {
                continue;
            }
            let _ = self.ingestion_tx.try_send(IngestionEvent::Tick(BookTbT {
                exchange: cursor.exchange.clone(),
                instrument_id: cursor.instrument_id.clone(),
                receipt_time,
                timestamp: exchange_ts,
                side: ask.clone(),
                price: Self::decimal_from_price(level.price, event.price_scale),
                size: Self::decimal_from_size(level.size, event.size_scale),
            }));
        }
    }

    fn emit_snapshots(&mut self) {
        let Some(receipt_time) = chrono::Utc::now().timestamp_nanos_opt() else {
            return;
        };

        let cursors: Vec<SymbolCursor> = self.symbol_cursors.values().cloned().collect();
        for cursor in cursors {
            let depth = Self::publish_depth_for_exchange(cursor.exchange.as_ref());
            if cursor.state != BookState::Synced || cursor.last_ts == 0 {
                continue;
            }

            let Some(_meta) = self.book_indexer.symbols.get(cursor.symbol_id) else {
                continue;
            };
            let Some(bids) = self.book_indexer.bids.get(cursor.symbol_id) else {
                continue;
            };
            let Some(asks) = self.book_indexer.asks.get(cursor.symbol_id) else {
                continue;
            };

            let mut bid_prices = Vec::with_capacity(depth);
            let mut bid_sizes = Vec::with_capacity(depth);
            for (price, size) in bids.prices.iter().zip(bids.sizes.iter()).take(depth) {
                bid_prices.push(Self::decimal_from_price(*price, DEFAULT_PRICE_SCALE));
                bid_sizes.push(Self::decimal_from_size(*size, DEFAULT_QTY_SCALE));
            }

            let mut ask_prices = Vec::with_capacity(depth);
            let mut ask_sizes = Vec::with_capacity(depth);
            for (price, size) in asks.prices.iter().zip(asks.sizes.iter()).take(depth) {
                ask_prices.push(Self::decimal_from_price(*price, DEFAULT_PRICE_SCALE));
                ask_sizes.push(Self::decimal_from_size(*size, DEFAULT_QTY_SCALE));
            }

            let snapshot = BookSnapShot {
                exchange: cursor.exchange.clone(),
                instrument_id: cursor.instrument_id.clone(),
                receipt_time,
                timestamp: cursor.last_ts.saturating_mul(1_000_000),
                bids: vec![bid_prices, bid_sizes],
                asks: vec![ask_prices, ask_sizes],
            };

            let _ = self
                .ingestion_tx
                .try_send(IngestionEvent::Snapshot(snapshot));
        }
    }

    fn decimal_from_price(price: Price, scale: u32) -> Decimal {
        if scale == 0 {
            return Decimal::ZERO;
        }
        Decimal::from(price.0) / Decimal::from(scale)
    }

    fn decimal_from_size(size: Size, scale: u64) -> Decimal {
        if scale == 0 {
            return Decimal::ZERO;
        }
        Decimal::from(size.0) / Decimal::from(scale)
    }
}

#[derive(Debug)]
pub struct ExchangeConnector<A: ExchangeConnectorAdapter> {
    adapter: A,
    instruments: Vec<String>,
    data_sender: mpsc::Sender<OrderBookCommand>,
    restart_signal: broadcast::Receiver<RestartSignal>,
    shutdown_token: CancellationToken,
    active_subs: Arc<RwLock<HashSet<String>>>,
}

impl<A: ExchangeConnectorAdapter> ExchangeConnector<A> {
    pub fn new(
        adapter: A,
        instruments: Vec<String>,
        data_sender: mpsc::Sender<OrderBookCommand>,
        restart_signal: broadcast::Receiver<RestartSignal>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            adapter,
            instruments,
            data_sender,
            restart_signal,
            shutdown_token,
            active_subs: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) {
        tracing::info!(
            exchange = self.adapter.get_source_name(),
            "Exchange connected"
        );
        let mut backoff = 1u64;
        loop {
            if self.shutdown_token.is_cancelled() {
                log_info!(
                    LogEventCategory::System,
                    "shutdown",
                    "exchange_connector",
                    "Exchange connector shutdown"
                );
                break;
            }
            let request = match self.adapter.get_url() {
                Ok(request) => request,
                Err(e) => {
                    tracing::error!(error = ?e, "Failed to build websocket request");
                    continue;
                }
            };
            let mut _is_reconnected = false;

            match connect_async(request).await {
                Ok((websocket_, _)) => {
                    backoff = 1u64;
                    let (mut ws_write, mut ws_read) = websocket_.split();
                    let init_instruments = self.instruments.clone();
                    let msg = self.adapter.create_subscription(&init_instruments, false);
                    {
                        let mut writer = self.active_subs.write().await;
                        for inst in &init_instruments {
                            tracing::info!(
                                exchange = self.adapter.get_source_name(),
                                instrument = inst,
                                "Subscribed to"
                            );
                            writer.insert(inst.clone());
                        }
                    }
                    if let Err(e) = ws_write.send(msg).await {
                        tracing::error!("Error to send subscribe message {:?}", e);
                    } else {
                        self.push_bootstrap_events(&init_instruments).await;
                        let conn_start = tokio::time::Instant::now();

                        let is_shutdown_requested =
                            self.handle_connection(&mut ws_write, &mut ws_read).await;

                        if is_shutdown_requested {
                            log_info!(
                                LogEventCategory::System,
                                "shutdown",
                                "book_data_connector",
                                "BookDataConnector shutdowned"
                            );
                            break;
                        }

                        if conn_start.elapsed().as_secs() > TASK_DURATION_THRESHOLD {
                            backoff = 1;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = ?e, "Failed to connect");
                }
            }
            tracing::warn!(%backoff, "Connection lost or failed. Retrying");

            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(backoff)) => {
                    backoff = (backoff * 2).min(60);
                }
                _ = self.shutdown_token.cancelled() => {
                    tracing::info!("Shutdown received during backoff sleep");
                    break;
                }
            }
        }
    }

    async fn handle_connection(
        &mut self,
        ws_writer: &mut WsWriter,
        ws_reader: &mut WsReader,
    ) -> bool {
        let ping_duration = self
            .adapter
            .ping_interval()
            .unwrap_or(tokio::time::Duration::from_secs(30));
        let mut ping_timer = tokio::time::interval(ping_duration);
        ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                signal = self.restart_signal.recv() => {
                    let signal = match signal {
                        Ok(s) => s,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return true,
                    };

                    if !signal
                        .exchange
                        .eq_ignore_ascii_case(&self.adapter.get_source_name())
                    {
                        continue;
                    }

                    let check = {
                        let reader = self.active_subs.read().await;
                        reader.contains(signal.instrument_id.as_ref())
                    };

                    if check {
                        tracing::info!("Corrputed Signal received {:?}", signal);
                        let symbols = vec![signal.instrument_id.as_ref().to_owned()];

                        let un_sub = self.adapter.create_subscription(&symbols, true);
                        let _ = ws_writer.send(un_sub).await;

                        let re_sub = self.adapter.create_subscription(&symbols, false);
                        let _ = ws_writer.send(re_sub).await;
                        self.push_bootstrap_events(&symbols).await;
                    } else {
                        tracing::info!("Corrputed Signal failed received {:?}", signal);
                    }
                }

                msg = ws_reader.next() => {
                    match msg {
                        Some(Ok(Message::Ping(p))) => {
                            let _ = ws_writer.send(Message::Pong(p)).await;
                        }
                        Some(Ok(Message::Close(_))) => return false,
                        Some(Err(_)) | None => return false,

                        Some(Ok(msg)) => {
                            let bytes = match &msg {
                                Message::Text(txt) => txt.as_bytes(),
                                Message::Binary(bin) => bin,
                                _ => return false,
                            };

                            match self.adapter.parse_market_event(bytes) {
                                Ok(Some(event)) => {
                                    let _ = self.data_sender.try_send(OrderBookCommand::Update(event));
                                },
                                Ok(None) => {}
                                Err(err) => {
                                 tracing::debug!(error = ?err, "unable to parse market event");
                                }
                            }
                        }
                    }
                }
                _ = ping_timer.tick() => {
                    self.adapter.create_ping(ws_writer);
                }
                _ = self.shutdown_token.cancelled() => {
                    return true;
                }
            }
        }
    }

    async fn push_bootstrap_events(&self, instruments: &[String]) {
        match self.adapter.bootstrap_events(instruments).await {
            Ok(events) => {
                for event in events {
                    let _ = self.data_sender.try_send(OrderBookCommand::Update(event));
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    exchange = self.adapter.get_source_name(),
                    "Failed to bootstrap orderbook snapshot"
                );
            }
        }
    }
}

impl TableSchema for BookSnapShot {
    fn table_name(&self) -> &str {
        "exchanges_live_price"
    }
    fn get_columns(&self) -> Vec<(&str, ColumnValue<'_>)> {
        vec![
            ("symbol", ColumnValue::Symbol(&self.instrument_id)),
            ("receipt_time", ColumnValue::Timestamp(self.receipt_time)),
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

#[derive(Debug)]
pub struct TaskSupervisor {
    tracker: TaskTracker,
    pub global_shutdown: CancellationToken,
}

impl TaskSupervisor {
    pub fn new() -> Self {
        Self {
            tracker: TaskTracker::new(),
            global_shutdown: CancellationToken::new(),
        }
    }

    /// Spawns a restartable service (e.g., Book Connector Data Feed).
    /// The `task_factory` must capture any necessary config/tokens internally.
    pub fn spawn_service<F, Fut>(&self, name: impl Into<String>, task_factory: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let tracker = self.tracker.clone();
        let global_shutdown = self.global_shutdown.clone();
        let name = name.into();

        tracker.spawn(async move {
            tracing::info!("[{}] Supervisor started", name);

            loop {
                if global_shutdown.is_cancelled() {
                    break;
                }
                let fut = task_factory();
                let mut handle = tokio::spawn(fut);

                tokio::select! {
                    res = &mut handle => {
                        match res {
                            Ok(()) => {
                                if global_shutdown.is_cancelled() {
                                    break;
                                }
                                tracing::error!("[{}] Service exited unexpectedly. Restarting...", name);
                            }
                            Err(e) => {
                                if e.is_panic() {
                                    tracing::error!("[{}] CRITICAL: Service PANICKED! Restarting...", name);
                                } else {
                                    tracing::error!("[{}] Service cancelled by runtime.", name);
                                }
                            }
                        }
                    }

                    _ = global_shutdown.cancelled() => {
                        tracing::info!("[{}] Shutdown received. Aborting service...", name);
                        handle.abort();
                        break;
                    }
                }

                tokio::select! {
                    _ = sleep(Duration::from_secs(BACKOFF_PERIOD)) => {},
                    _ = global_shutdown.cancelled() => {
                        tracing::info!("[{}] Shutdown signal received while backing off.", name);
                        break;
                    }
                }
            }
            tracing::info!("[{}] Supervisor stopped", name);
        });
    }

    /// Spawns a one-off worker (e.g., Local L2 Orderbook).
    /// Does NOT restart on failure.
    pub fn spawn_worker<F, Fut, E>(&self, name: impl Into<String>, task_factory: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Debug + Send + 'static,
    {
        let tracker = self.tracker.clone();
        let shutdown = self.global_shutdown.clone();
        let name = name.into();

        tracker.spawn(async move {
            tracing::info!("[{}] Worker started", name);
            if let Err(e) = task_factory(shutdown).await {
                tracing::error!("[{}] Worker failed: {:?}", name, e);
            }
            tracing::info!("[{}] Worker stopped", name);
        });
    }

    pub async fn shutdown(self) {
        tracing::info!("Global shutdown initiated...");
        self.global_shutdown.cancel();

        self.tracker.close();
        self.tracker.wait().await;
        tracing::info!("Graceful shutdown complete.");
    }
}

#[cfg(test)]
mod tests {
    use super::{BookSide, OrderBookIndexer, Price, Size};

    #[test]
    fn new_book_must_not_collide_across_exchanges() {
        let mut indexer = OrderBookIndexer::new();

        let bybit = indexer.new_book("bybit".to_string(), "BTCUSDT".to_string());
        let okx = indexer.new_book("okx".to_string(), "BTCUSDT".to_string());

        assert_ne!(
            bybit, okx,
            "same symbol on different exchanges must be distinct"
        );
    }

    #[test]
    fn best_levels_should_be_top_of_book() {
        let mut indexer = OrderBookIndexer::new();
        let symbol_id = indexer.new_book("bybit".to_string(), "SOLUSDT".to_string());

        indexer.appy_book_update(symbol_id, Price(100), Size(1), BookSide::Bid);
        indexer.appy_book_update(symbol_id, Price(101), Size(2), BookSide::Bid);
        indexer.appy_book_update(symbol_id, Price(103), Size(1), BookSide::Ask);
        indexer.appy_book_update(symbol_id, Price(102), Size(2), BookSide::Ask);

        let key = OrderBookIndexer::symbol_key("bybit", "SOLUSDT");
        assert_eq!(indexer.best_bid(&key), Some((Price(101), Size(2))));
        assert_eq!(indexer.best_ask(&key), Some((Price(102), Size(2))));
    }
}
