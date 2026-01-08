use core::{error::ConnectorError, models::{HashWriter, OrderBookL2}, traits::{ExchangeAdapter, ExchangeConnectorAdapter}};
use std::{borrow::Cow, str::FromStr};

use crc32fast::Hasher;
use rust_decimal::Decimal;
use serde::{Deserialize};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use std::io::Write;

#[derive(Debug, Deserialize)]
pub struct OkxOrderBookMesssage<'a> {
    pub arg: Arg<'a>,
    #[serde(borrow)]
    pub action: Cow<'a, str>,
    #[serde(borrow)]
    pub data: Vec<OkxBookData<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Arg<'a> {
    #[serde(borrow)]
    pub channel: Cow<'a, str>,
    #[serde(borrow)]
    pub inst_id: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OkxBookData<'a> {
    #[serde(borrow)]
    pub asks: Vec<OkxOrderLevel<'a>>,
    #[serde(borrow)]
    pub bids: Vec<OkxOrderLevel<'a>>,
    #[serde(borrow)]
    pub ts: Cow<'a, str>,
    pub checksum: i32,
    pub seq_id: u64,
    pub prev_seq_id: u64,
}

#[derive(Debug, Deserialize)]
pub struct OkxOrderLevel<'a> {
    #[serde(borrow)]
    pub price: Cow<'a, str>,
    #[serde(borrow)]
    pub size: Cow<'a, str>,
    #[serde(borrow)]
    pub liquid_price: Cow<'a, str>,
    #[serde(borrow)]
    pub count: Cow<'a, str>,
}

pub struct OkxAdapter;

impl ExchangeAdapter for OkxAdapter {
    type InputBookData<'a> = OkxOrderBookMesssage<'a>;
    const EXCHANGE_NAME : Cow<'_,str> = Cow::Borrowed("okx");
    
    fn is_snapshot<'a>(&self, msg: &Self::InputBookData<'_>) -> bool {
        msg.action.as_ref() == "snapshot"
    }
    fn get_seq_id<'a>(&self, msg: &Self::InputBookData<'_>) -> u64 {
        if let Some(d) = msg.data.first() {
            return d.seq_id;
        } else {
            return 0;
        }
    }
    fn get_prev_seq_id<'a>(&self, msg: &Self::InputBookData<'_>) -> u64 {
        if let Some(d) = msg.data.first() {
            return d.prev_seq_id;
        } else {
            return 0;
        }
    }
    fn apply<'a>(&self,core: &mut OrderBookL2, msg: &Self::InputBookData<'_>) {
        for delta in msg.data.iter() {
            for level in delta.asks.iter() {
                let price = Decimal::from_str(&level.price).unwrap_or_default();
                let size = Decimal::from_str(&level.size).unwrap_or_default();
                
                if level.size.eq("0") {
                    core.asks.remove(&price);
                } else {
                    core.asks.insert(price, size);
                }
            }
            
            for level in delta.bids.iter() {
                let price = Decimal::from_str(&level.price).unwrap_or_default();
                let size = Decimal::from_str(&level.size).unwrap_or_default();
                
                if level.size.eq("0") {
                    core.bids.remove(&price);
                } else {
                    core.bids.insert(price, size);
                }
            }
        }
    }
    
    fn verify_integrity<'a>(&self, core: &OrderBookL2, msg: &Self::InputBookData<'_>) -> bool {
        let mut cs = 0;
        for data in msg.data.iter() {
            cs = data.checksum
        }
        
        let mut hasher = Hasher::new();
        let mut writer = HashWriter(&mut hasher);
        
        let mut bids_iter = core.bids.iter().rev();
        let mut asks_iter = core.asks.iter();
        
        let mut is_first_item = true;
        
        for _ in 0..25 {
            let bid = bids_iter.next();
            let ask = asks_iter.next();
            
            if let Some((price,size)) = bid {
                if !is_first_item {
                    let _ = writer.write_all(b":");
                }
                
                let _ = write!(writer, "{}", price.normalize());
                let _ = writer.write_all(b":");
                let _ = write!(writer, "{}", size.normalize());
                
                is_first_item = false
            }
            
            if let Some((price, size)) = ask {
                if !is_first_item {
                    let _ = writer.write_all(b":");
                }
                
                let _ = write!(writer, "{}", price.normalize());
                let _ = writer.write_all(b":");
                let _ = write!(writer, "{}", size.normalize());
                
                is_first_item = false
            }
        }
        hasher.finalize() == cs as u32
    }
}

pub struct OkxConnectorAdapter;

impl ExchangeConnectorAdapter for OkxConnectorAdapter {    
    fn get_url(&self) -> String {
        "wss://ws.okx.com:8443/ws/v5/public".to_string()
    }
    
    fn create_subscription(&self, instruments: &[String], is_unsub: bool) -> Message {
        let id: u8 = rand::random();

        let args: Vec<_> = instruments
            .iter()
            .map(|sym| {
                serde_json::json!({
                    "channel": "books",
                    "instId": sym.as_str(),
                })
            })
            .collect();
        let op = if is_unsub { "unsubscribe" } else { "subscribe" };

        let json_string = serde_json::json!({
            "op": op,
            "args": args,
            "id": id
        })
        .to_string();
        
        Message::Text(Utf8Bytes::from(json_string))
    }
    
    fn route_message(&self, msg: &impl AsRef<[u8]>) -> Result<u128, ConnectorError> {
        let bytes = msg.as_ref();
        let pattern = b"\"instId\":\"";
        let mut chunk = [0u8;16];
        let mut result = 0u128;
        
        if let Some(index) = bytes.windows(pattern.len()).position(|matching| matching == pattern) {
            let start = index + pattern.len();
            
            if let Some(end) = bytes[start..].iter().position(|&b| b == b'"') {
                let inst_id = &bytes[start..start + end];
                
                chunk[..pattern.len().min(16)].copy_from_slice(inst_id);
                
                result = u128::from_le_bytes(chunk);
            } 
        }
        Ok(result)
    }
}