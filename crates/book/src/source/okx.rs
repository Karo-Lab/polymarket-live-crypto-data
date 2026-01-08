use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    str::FromStr,
};
use crc32fast::Hasher;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc},
    time::{Duration, interval, sleep},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Utf8Bytes},
};
use tokio_util::sync::CancellationToken;

use crate::{ingestion::orderbook::BookSnapShot, meta::pipline_logger::AuditEvent};

#[derive(Debug, Deserialize)]
pub struct OkxOrderBookMesssage<'a> {
    arg: Arg<'a>,
    pub action: Cow<'a, str>,
    data: Vec<BookData<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Arg<'a> {
    channel: Cow<'a, str>,
    inst_id: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookData<'a> {
    asks: Vec<OrderLevel<'a>>,
    bids: Vec<OrderLevel<'a>>,
    ts: Cow<'a, str>,
    checksum: i32,
    seq_id: i64,
    prev_seq_id: i64,
}

#[derive(Debug, Deserialize)]
pub struct OrderLevel<'a> {
    price: Cow<'a, str>,
    size: Cow<'a, str>,
    liquid_price: Cow<'a, str>,
    count: Cow<'a, str>,
}

#[derive(Debug)]
pub enum OrderBookCommand {
    Update(Utf8Bytes),
    TakeSnapShot
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OkxOrderBook {
    pub ts: String,
    pub instrument_id: String,
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
}

impl OkxOrderBook {
    pub fn new(inst_id: String) -> Self {
        Self {
            ts: String::new(),
            instrument_id: inst_id,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn updates(&mut self, update_data: &BookData) {
        for level in update_data.asks.iter() {
            let price = Decimal::from_str(level.price.as_ref()).unwrap_or_default();
            let size = Decimal::from_str(level.size.as_ref()).unwrap_or_default();
            if level.size.eq("0") {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, size);
            }
        }

        for level in update_data.bids.iter() {
            let price = Decimal::from_str(level.price.as_ref()).unwrap_or_default();
            let size = Decimal::from_str(level.size.as_ref()).unwrap_or_default();
            if level.size.eq("0") {
                self.bids.remove(&price);
            } else {
                self.bids.insert(price, size);
            }
        }
    }

    // Okx require local orderbook to match the checksum of the top 25 level of bids/asks
    // If fail, reset the orderbook
    pub fn calculate_local_checksum(&self) -> i32 {
        let mut parts = Vec::new();

        let mut bids_iter = self.bids.iter().rev();
        let mut asks_iter = self.asks.iter();

        for _ in 0..25 {
            let bid = bids_iter.next();
            let ask = asks_iter.next();

            if let Some((price, size)) = bid {
                parts.push(self.format_decimal(price));
                parts.push(self.format_decimal(size));
            }
            if let Some((price, size)) = ask {
                parts.push(self.format_decimal(price));
                parts.push(self.format_decimal(size));
            }
        }

        let checksum_str = parts.join(":");

        let mut hasher = Hasher::new();
        hasher.update(checksum_str.as_bytes());

        hasher.finalize() as i32
    }

    fn format_decimal(&self, d: &rust_decimal::Decimal) -> String {
        d.normalize().to_string()
    }
    
    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<OrderBookCommand>,
        restart_signal_tx: mpsc::Sender<String>,
        ingestion_tx: mpsc::Sender<BookSnapShot>,
        audit_tx: mpsc::Sender<AuditEvent>,
        shutdown_token: CancellationToken,
    ) {
        let mut local_previous_seq_id = 0;
        let depth = 25;
        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    match cmd {
                        Some(OrderBookCommand::Update(txt)) => {
                            match serde_json::from_slice::<OkxOrderBookMesssage>(txt.as_bytes()) {
                                Ok(v) => {
                                    match v.action.as_ref() {
                                        "update" => {
                                            
                                            for data in v.data {
                                                self.ts = data.ts.to_string();
                                                
                                                if data.prev_seq_id != local_previous_seq_id {
                                                    let _ = restart_signal_tx.send(self.instrument_id.clone()).await;
                                                    
                                                    let _ = audit_tx.try_send(AuditEvent::SeqIdGap {
                                                        symbol: self.instrument_id.clone(),
                                                        expected: data.prev_seq_id,
                                                        actual: local_previous_seq_id,                                                    
                                                    });
                                                }
                                                self.updates(&data);
                                                local_previous_seq_id = data.seq_id;
                                                
                                                if data.seq_id % 100 == 0 {
                                                    let local_ob_cs = self.calculate_local_checksum();
                                                    if data.checksum !=  local_ob_cs {
                                                        let _ = restart_signal_tx.send(self.instrument_id.clone()).await;
                                                        let _ = audit_tx.try_send(AuditEvent::CheckSumFailure { 
                                                            symbol: self.instrument_id.clone(), 
                                                            expected: data.checksum, 
                                                            actual: local_ob_cs,
                                                        });
                                                    }   
                                                }
                                            }
        
                                        },
                                        "snapshot" => {
                                            // Flush all 
                                            self.bids.clear();
                                            self.asks.clear();
        
                                            self.instrument_id = v.arg.inst_id.to_string();
                                            for data in v.data {
                                                self.ts = data.ts.to_string();
                                                local_previous_seq_id = data.seq_id;
                                                self.updates(&data);
                                            }
                                        }
                                        _ => continue
                                    }
                                },
                                _ => continue
                            }
                        }
                        Some(OrderBookCommand::TakeSnapShot) => {
                            if let Some(ts_now) = chrono::Utc::now().timestamp_nanos_opt() {
                                let mut bid_prices = Vec::with_capacity(depth);
                                let mut bid_sizes = Vec::with_capacity(depth);
                                
                                let mut ask_prices = Vec::with_capacity(depth);
                                let mut ask_sizes = Vec::with_capacity(depth);
                                        
                                let instrument_id_cln = self.instrument_id.clone();
                                
                                for (
                                        (bid_price_, bid_size_), 
                                        (ask_price_, ask_size_)
                                ) in self.bids.iter().rev().take(depth).zip(self.asks.iter().take(depth)) {
                                    bid_prices.push(*bid_price_);
                                    bid_sizes.push(*bid_size_);
                                    
                                    ask_prices.push(*ask_price_);
                                    ask_sizes.push(*ask_size_);
                                }
                            
                                if let Ok(value) = self.ts.parse::<i64>() {
                                    let exchange_ts = value * 1_000_000;
                                    let snapshot = BookSnapShot { 
                                        instrument_id: instrument_id_cln,
                                        exchange_ts,
                                        timestamp: ts_now,
                                        bids: vec![bid_prices, bid_sizes],
                                        asks: vec![ask_prices, ask_sizes]
                                    };
                                                                    
                                    let _ = ingestion_tx.try_send(snapshot);
                                }
                            }
                        }
                        None => break
                    }
                    
                }
                _ = shutdown_token.cancelled() => {
                    return;
                }
                else => break
            }
        }
    }
}

pub struct OkxOrderBookDataStream {
    channel_ob_map: HashMap<String, mpsc::Sender<OrderBookCommand>>,
    hex_topic_map: HashMap<u128, String>,
}

impl OkxOrderBookDataStream {
    pub fn new(
        channel_ob_map: HashMap<String, mpsc::Sender<OrderBookCommand>>,
        hex_topic_map: HashMap<u128, String>,
    ) -> Self {
        Self {
            channel_ob_map,
            hex_topic_map,
        }
    }

    fn build_subscribe_request(&self, channels: &[String], is_unsub: bool) -> String {
        let id: u8 = rand::random();

        let args: Vec<_> = channels
            .iter()
            .map(|sym| {
                serde_json::json!({
                    "channel": "books",
                    "instId": sym.as_str(),
                })
            })
            .collect();

        let op = if is_unsub { "unsubscribe" } else { "subscribe" };

        serde_json::json!({
            "op": op,
            "args": args,
            "id": id
        })
        .to_string()
    }

    fn init_subscribe_request(&self) -> String {
        let id: u8 = rand::random();

        let args: Vec<_> = self
            .channel_ob_map
            .keys()
            .map(|sym| {
                serde_json::json!({
                    "channel": "books",
                    "instId": sym.as_str(),
                })
            })
            .collect();

        serde_json::json!({
            "op": "subscribe",
            "args": args,
            "id": id
        })
        .to_string()
    }

    pub async fn start_stream(
        &self,
        mut restart_signal_rx: mpsc::Receiver<String>,
        shutdown_token: CancellationToken,
    ) {
        let url = "wss://ws.okx.com:8443/ws/v5/public";

        let mut backoff = 1u64;
        let mut is_reconnected = false;

        let mut heartbeat_interval = interval(Duration::from_secs(10));
        heartbeat_interval.tick().await;

        let init_sub_msg = self.init_subscribe_request();

        let mut failure_channels = Vec::with_capacity(4);
                
        loop {
            match connect_async(url).await {
                Ok((ws, _)) => {
                    let (mut sink, mut read) = ws.split();
                    backoff = 1;

                    if is_reconnected {
                        tracing::info!("Reconnected Okx");
                    } else {
                        tracing::info!("Connected Okx");
                    }

                    let _ = sink
                        .send(Message::Text(Utf8Bytes::from(init_sub_msg.clone())))
                        .await;

                    loop {
                        tokio::select! {
                            Some(signal) = restart_signal_rx.recv() => {
                                failure_channels.push(signal);

                                let un_sub = self.build_subscribe_request(&failure_channels, true);
                                let _ = sink
                                        .send(Message::Text(Utf8Bytes::from(un_sub.clone())))
                                        .await;
                                let _ = sleep(Duration::from_millis(50)).await;
                                let resub = self.build_subscribe_request(&failure_channels, false);
                                let _ = sink
                                    .send(Message::Text(Utf8Bytes::from(resub.clone())))
                                    .await;

                            }
                            Some(msg) = read.next() => {
                                match msg {
                                    Ok(Message::Text(txt)) => {
                                        if txt == "ping" {
                                            if let Err(e) = sink.send(Message::Text("pong".into())).await {
                                                tracing::warn!("Unable to send pong to Okx - {e}");
                                                continue;
                                            }
                                        }

                                        let pattern = b"\"instId\":\"";
                                        let bytes = txt.as_bytes();
                                        let mut chunk = [0u8; 16];
                                        
                                        // Find the start index of the value of 'instId:' in the bytes return Okx websocket message
                                        // of channel 'books'
                                        if let Some(index) = bytes.windows(pattern.len()).position(|window| window == pattern) {
                                            let start = index + pattern.len();
                                            if let Some(end) = bytes[start..].iter().position(|&b| b == b'"') {
                                                let inst_id = &bytes[start..start + end];

                                                chunk[..13].copy_from_slice(inst_id);

                                                let current_val = u128::from_le_bytes(chunk);
                                                for (hex,topic) in self.hex_topic_map.iter() {
                                                    if current_val ^ *hex == 0 {
                                                        if let Some(tx) = self.channel_ob_map.get(topic) {
                                                            // Utf8Bytes utilize Bytes from bytes crate underthe hood
                                                            // Cloning is cheap since Bytes act like Arc<T>
                                                            if let Err(e) = tx.send(OrderBookCommand::Update(txt.clone())).await {
                                                                tracing::warn!("Error forwarding {:?}",e);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Ok(Message::Ping(p)) => {
                                        let _ = sink.send(Message::Pong(p)).await;
                                    }
                                    Ok(Message::Close(_)) => {
                                        tracing::info!("Connection closed Okx");
                                        is_reconnected = true;
                                        break;
                                    }
                                    Err(e) => {
                                        tracing::warn!("WS error Okx {:?}",e);
                                        is_reconnected = true;
                                        break;
                                    }
                                    _ => {}
                                }
                            }

                            _ = heartbeat_interval.tick() => {
                                if let Err(e) = sink.send(Message::Text("PING".into())).await {
                                    tracing::warn!("Error sending PING {:?}", e)
                                }
                            }
                            _ = shutdown_token.cancelled() => {
                                let _ = sink.send(Message::Close(None)).await;
                                return;
                            }
                        }
                    }
                }

                Err(e) => {
                    tracing::warn!("Connect error to Okx - {:?}", e);
                }
            }

            tracing::info!("Reconnecting to Okx in {}s", backoff);

            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(60);
        }
    }
}
