use crate::{
    constants::{BACKOFF_PERIOD, TASK_DURATION_THRESHOLD},
    log_error, log_info,
    telementry::{
        AuditEvent, AuditEventBuilder, AuditLevel, ErrorPayload, ErrorSeverity, IngestionEvent,
        LogEventCategory, SystemEvent,
    },
    traits::{
        BoxStream, Cache, ColumnValue, ExchangeAdapter, ExchangeConnectorAdapter, TableSchema,
        WsReader, WsStream, WsWriter,
    },
    utils::InMemoryStore,
};
use crc32fast::Hasher;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    fmt::{Debug, Display},
    io::Write,
    io::{self, ErrorKind},
    sync::Arc,
};
use tokio::{
    net::TcpStream,
    sync::{RwLock, mpsc},
    time::{Duration, sleep},
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    client_async_tls_with_config,
    tungstenite::{Error as WsError, Message, Utf8Bytes, client::IntoClientRequest, http::Uri},
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

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

impl AsRef<str> for OrderBookCommand {
    fn as_ref(&self) -> &str {
        match self {
            OrderBookCommand::Update(_) => "Update",
            OrderBookCommand::TakeSnapShot => "Snapshot",
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
    pub fn best_bid(&self) -> Option<&Decimal> {
        self.bids.keys().next_back()
    }

    pub fn best_ask(&self) -> Option<&Decimal> {
        self.asks.keys().next()
    }

    pub fn best_bid_size(&self) -> Option<&Decimal> {
        self.bids.values().next_back()
    }

    pub fn best_ask_size(&self) -> Option<&Decimal> {
        self.asks.values().next()
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

#[derive(Debug)]
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

impl AsRef<str> for BookState {
    fn as_ref(&self) -> &str {
        match self {
            BookState::AwaitingSnapshot => "BookState::AwaitingSnapshot",
            BookState::Synced => "BookState::Synced",
            BookState::Corrupted => "BookState::Corrupted",
        }
    }
}

#[derive(Debug)]
pub struct LocalL2OrderBook<A: ExchangeAdapter> {
    adapter: A,
    command_receiver: mpsc::Receiver<OrderBookCommand>,
    restart_signal_tx: mpsc::Sender<Arc<String>>,
    ingestion_tx: mpsc::Sender<IngestionEvent>,
    audit_tx: mpsc::Sender<AuditEvent>,
    shutdown_token: CancellationToken,
    book_core: OrderBookL2,
    state: BookState,
    cache: Arc<InMemoryStore<String, Arc<String>>>,
}
impl<E> LocalL2OrderBook<E>
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
            cache: InMemoryStore::new(),
        }
    }

    pub async fn run(mut self) {
        {
            let span = tracing::info_span!("actor_startup", service = "orderbook");
            let _guard = span.enter();

            tracing::info!("Warming up cache");
            self.cache
                .set("bid".to_string(), Arc::new("bid".to_string()))
                .await;
            self.cache
                .set("ask".to_string(), Arc::new("ask".to_string()))
                .await;
            self.cache
                .set("buy".to_string(), Arc::new("buy".to_string()))
                .await;
            self.cache
                .set("sell".to_string(), Arc::new("sell".to_string()))
                .await;
            tracing::info!("Cache warm. Starting loop.");
        }

        loop {
            if self.shutdown_token.is_cancelled() {
                log_info!(
                    LogEventCategory::System,
                    "shutdown",
                    "local_l2_orderbook",
                    "Local L2 orderbook shutdown"
                );
                break;
            }
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
                    log_info!(LogEventCategory::System, "shutdown", "local_l2_orderbook", "Local L2 orderbook shutdown");
                    return ;
                }
                else => break
            }
        }
    }

    #[tracing::instrument(
        level = "info",
        name = "handle_command",
        skip(self, command),
        fields(
            cmd_type = %command.as_ref(),
        )
    )]
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

                                let system_audit = SystemEvent::L2LOBState {
                                    state: state.as_ref(),
                                };

                                log_info!(
                                    LogEventCategory::System,
                                    "book_state",
                                    "orderbook",
                                    system_audit
                                );

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
                                            log_info!(
                                                LogEventCategory::Audit,
                                                "audit",
                                                "orderbook",
                                                event.clone()
                                            );
                                            let _audit_seqence_failure =
                                                self.audit_tx.try_send(event);
                                        }
                                        Err(e) => {
                                            let error_payload = ErrorPayload::new(
                                                "audit",
                                                e.as_str(),
                                                ErrorSeverity::Critical,
                                                false,
                                            );
                                            log_error!(
                                                LogEventCategory::Audit,
                                                "audit",
                                                "orderbook",
                                                error_payload
                                            )
                                        }
                                    }
                                    state = BookState::Corrupted;
                                } else {
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
                                                    log_info!(
                                                        LogEventCategory::Audit,
                                                        "audit",
                                                        "orderbook",
                                                        event.clone()
                                                    );
                                                    let _audit_seqence_failure =
                                                        self.audit_tx.try_send(event);
                                                }
                                                Err(e) => {
                                                    let error_payload = ErrorPayload::new(
                                                        "audit",
                                                        e.as_str(),
                                                        ErrorSeverity::Critical,
                                                        false,
                                                    );
                                                    log_info!(
                                                        LogEventCategory::Audit,
                                                        "audit",
                                                        "orderbook",
                                                        error_payload
                                                    )
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

                                    for delta in delta_changes {
                                        let cache_side = match self.cache.try_get(delta.side) {
                                            Some(v) => v,
                                            None => {
                                                // Allocate in case of cache miss
                                                self.cache
                                                    .try_set(
                                                        delta.side.to_string(),
                                                        Arc::new(delta.new_size.to_string()),
                                                    )
                                                    .unwrap_or_default()
                                            }
                                        };

                                        let tick = BookTbT {
                                            exchange: exchange.clone(),
                                            instrument_id: instrument_id.clone(),
                                            side: cache_side.clone(),
                                            price: delta.price,
                                            size: delta.new_size,
                                            timestamp,
                                            receipt_time: ts_now,
                                        };
                                        let _ =
                                            self.ingestion_tx.try_send(IngestionEvent::Tick(tick));
                                    }
                                }

                                state
                            }
                            BookState::Corrupted => {
                                let timestamp = self.book_core.ts.clone() * 1_000_000;

                                {
                                    let system_event = SystemEvent::L2LOBTop {
                                        timestamp,
                                        best_bid_price: self
                                            .book_core
                                            .best_bid()
                                            .unwrap_or(&Decimal::ZERO)
                                            .clone(),
                                        best_bid_size: self
                                            .book_core
                                            .best_bid_size()
                                            .unwrap_or(&Decimal::ZERO)
                                            .clone(),
                                        best_ask_price: self
                                            .book_core
                                            .best_ask()
                                            .unwrap_or(&Decimal::ZERO)
                                            .clone(),
                                        best_ask_size: self
                                            .book_core
                                            .best_ask_size()
                                            .unwrap_or(&Decimal::ZERO)
                                            .clone(),
                                    };
                                    log_info!(
                                        LogEventCategory::System,
                                        "tick",
                                        "local_lob",
                                        system_event
                                    );
                                }

                                let system_audit = SystemEvent::L2LOBState {
                                    state: self.state.as_ref(),
                                };

                                log_info!(
                                    LogEventCategory::System,
                                    "book_state",
                                    "orderbook",
                                    system_audit
                                );

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

#[derive(Debug)]
pub struct ExchangeConnector<A: ExchangeConnectorAdapter> {
    adapter: A,
    data_sender_map: HashMap<u128, mpsc::Sender<OrderBookCommand>>,
    restart_signal: mpsc::Receiver<Arc<String>>,
    shutdown_token: CancellationToken,
    active_subs: Arc<RwLock<HashSet<String>>>,
}

impl<A: ExchangeConnectorAdapter> ExchangeConnector<A> {
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
            let url = self.adapter.get_url();
            let mut _is_reconnected = false;

            match connect_ws(&url).await {
                Ok(websocket_) => {
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
                    {
                        let mut writer = self.active_subs.write().await;
                        for inst in init_instruments.into_iter() {
                            tracing::info!(
                                exchange = self.adapter.get_source_name(),
                                instrument = inst,
                                "Subscribed to"
                            );
                            writer.insert(inst);
                        }
                    }
                    if let Err(e) = ws_write.send(msg).await {
                        tracing::error!("Error to send subscribe message {:?}", e);
                    } else {
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
                Some(signal) = self.restart_signal.recv() => {
                    let check = {
                        let reader = self.active_subs.read().await;
                        reader.contains(signal.as_ref())
                    };

                    if check {
                        tracing::info!("Corrputed Signal received {:?}", signal);
                        let symbols = [signal.as_ref().to_owned()];

                        let un_sub = self.adapter.create_subscription(&symbols, true);
                        let _ = ws_writer.send(un_sub).await;

                        let re_sub = self.adapter.create_subscription(&symbols, false);
                        let _ = ws_writer.send(re_sub).await;
                    } else {
                        tracing::info!("Corrputed Signal failed received {:?}", signal);
                    }
                }

                msg = ws_reader.next() => {
                    match msg {
                        Some(Ok(Message::Text(txt))) => {
                            if let Ok(value) = self.adapter.route_message(&txt) {
                                if let Some(tx) = self.data_sender_map.get(&value) {
                                    let _ = tx.try_send(OrderBookCommand::Update(txt.clone()));
                                }
                            } else {
                                continue
                            }
                        }
                        Some(Ok(Message::Ping(p))) => {
                            let _ = ws_writer.send(Message::Pong(p)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            return false;
                        }
                        Some(Err(_e)) => {
                            return false;
                        }
                        Some(_) => {}
                        None => {
                            return false;
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
}

fn parse_socks_proxy(proxy_url: &str) -> Result<(String, u16), io::Error> {
    let uri: Uri = proxy_url
        .parse()
        .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;
    let scheme = uri.scheme_str().unwrap_or_default();
    if scheme != "socks5" && scheme != "socks5h" {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "ALL_PROXY must be socks5:// or socks5h://",
        ));
    }
    let host = uri
        .host()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "ALL_PROXY missing host"))?;
    let port = uri
        .port_u16()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "ALL_PROXY missing port"))?;
    Ok((host.to_string(), port))
}

fn target_host_port(uri: &Uri) -> Result<(String, u16), io::Error> {
    let host = uri
        .host()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "WebSocket URL missing host"))?;
    let port = uri.port_u16().unwrap_or_else(|| match uri.scheme_str() {
        Some("wss") | Some("https") => 443,
        Some("ws") | Some("http") => 80,
        _ => 443,
    });
    Ok((host.to_string(), port))
}

async fn connect_ws(url: &str) -> Result<WsStream, WsError> {
    let request = url.into_client_request()?;
    let uri = request.uri().clone();
    let (host, port) = target_host_port(&uri)?;

    let base_stream: BoxStream = if let Ok(proxy_url) = env::var("ALL_PROXY") {
        match parse_socks_proxy(&proxy_url) {
            Ok((proxy_host, proxy_port)) => {
                let stream =
                    Socks5Stream::connect((proxy_host.as_str(), proxy_port), (host.as_str(), port))
                        .await
                        .map_err(|e| WsError::Io(io::Error::new(ErrorKind::Other, e)))?;
                Box::new(stream)
            }
            Err(err) => {
                tracing::warn!(error = ?err, "Invalid ALL_PROXY, connecting directly");
                Box::new(TcpStream::connect((host.as_str(), port)).await?)
            }
        }
    } else {
        Box::new(TcpStream::connect((host.as_str(), port)).await?)
    };

    let (ws_stream, _) = client_async_tls_with_config(request, base_stream, None, None).await?;
    Ok(ws_stream)
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
