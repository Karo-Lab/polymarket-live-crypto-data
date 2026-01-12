use exchanges_common::{
    models::{LevelDelta},
    traits::{ExchangeAdapter, ExchangeConnectorAdapter},
};
use std::{borrow::Cow, str::FromStr};

use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};

#[derive(Debug, Deserialize)]
pub struct BybitBookMessage<'a> {
    #[serde(borrow)]
    pub topic: Cow<'a, str>,
    #[serde(borrow)]
    #[serde(rename = "type")]
    pub event_type: Cow<'a, str>,
    pub ts: i64,
    #[serde(borrow)]
    pub data: BybitBookMessageData<'a>,
    pub cts: i64,
}

#[derive(Debug, Deserialize)]
pub struct BybitBookMessageData<'a> {
    #[serde(borrow)]
    pub s: Cow<'a, str>,
    #[serde(borrow)]
    pub b: Vec<Vec<Cow<'a, str>>>,
    #[serde(borrow)]
    pub a: Vec<Vec<Cow<'a, str>>>,
    pub u: i64,
    pub seq: i64,
}

pub struct BybitAdapter;

impl ExchangeAdapter for BybitAdapter {
    type InputBookData<'a> = BybitBookMessage<'a>;
    const EXCHANGE_NAME: Cow<'_, str> = Cow::Borrowed("bybit");

    fn get_instrument<'a>(&self, msg: &'a Self::InputBookData<'_>) -> &'a str {
        &msg.data.s
    }

    fn is_snapshot<'a>(&self, msg: &Self::InputBookData<'_>) -> bool {
        msg.event_type.as_ref() == "snapshot"
    }

    fn get_timestamp<'a>(&self, msg: &'a Self::InputBookData<'_>) -> i64 {
        msg.ts
    }

    fn get_seq_id<'a>(&self, msg: &Self::InputBookData<'_>) -> i64 {
        msg.data.u
    }
    fn get_prev_seq_id<'a>(&self, msg: &Self::InputBookData<'_>) -> i64 {
        msg.data.u - 1i64
    }
    fn apply<'a, 'b>(
        &self,
        core: &'b mut exchanges_common::models::OrderBookL2,
        msg: &Self::InputBookData<'_>,
    ) -> Vec<LevelDelta<'b>> {
        let mut changes = Vec::new();

        for level in msg.data.b.iter() {
            let price = Decimal::from_str(&level.first().unwrap_or(&Cow::Borrowed("0")))
                .unwrap_or_default();
            let new_size =
                Decimal::from_str(&level.last().unwrap_or(&Cow::Borrowed("0"))).unwrap_or_default();

            let old_size = if new_size.is_zero() {
                core.bids.remove(&price).unwrap_or_default()
            } else {
                core.bids.insert(price, new_size).unwrap_or_default()
            };

            if old_size != new_size {
                changes.push(LevelDelta {
                    side: "bid",
                    price,
                    old_size,
                    new_size,
                    diff: new_size - old_size,
                });
            }
        }

        for level in msg.data.a.iter() {
            let price = Decimal::from_str(&level.first().unwrap_or(&Cow::Borrowed("0")))
                .unwrap_or_default();
            let new_size =
                Decimal::from_str(&level.last().unwrap_or(&Cow::Borrowed("0"))).unwrap_or_default();

            let old_size = if new_size.is_zero() {
                core.asks.remove(&price).unwrap_or_default()
            } else {
                core.asks.insert(price, new_size).unwrap_or_default()
            };

            if old_size != new_size {
                changes.push(LevelDelta {
                    side: "ask",
                    price,
                    old_size,
                    new_size,
                    diff: new_size - old_size,
                });
            }
        }
        changes
    }
    
    fn get_bids<'a>(&self, msg: &'a Self::InputBookData<'_>) -> &'a Vec<Vec<Cow<'a, str>>> {
        &msg.data.b
    }
    fn get_asks<'a>(&self, msg: &'a Self::InputBookData<'_>) -> &'a Vec<Vec<Cow<'a, str>>> {
        &msg.data.a
    }
}

pub struct BybitConnectorAdapter;

impl ExchangeConnectorAdapter for BybitConnectorAdapter {
    fn get_source_name(&self) -> String {
        "Bybit".to_string()
    }
    fn get_url(&self) -> String {
        "wss://stream.bybit.com/v5/public/linear".to_string()
    }
    fn create_subscription(
        &self,
        instruments: &[String],
        is_unsub: bool,
    ) -> tokio_tungstenite::tungstenite::Message {
        let id: u8 = rand::random();

        let args: Vec<_> = instruments
            .iter()
            .map(|sym| format!("orderbook.50.{}", sym))
            .collect();

        let op = if is_unsub { "unsubscribe" } else { "subscribe" };

        let json_string = serde_json::json!({
            "op": op,
            "args": args,
            "req_id": id
        })
        .to_string();

        Message::Text(Utf8Bytes::from(json_string))
    }

    fn route_message(
        &self,
        msg: &impl AsRef<[u8]>,
    ) -> Result<u128, exchanges_common::error::ConnectorError> {
        let bytes = msg.as_ref();
        let pattern = b"\"s\":\"";

        let mut chunk = [0u8; 16];
        let mut result = 0u128;

        if let Some(index) = bytes
            .windows(pattern.len())
            .position(|matching| matching == pattern)
        {
            let start = index + pattern.len();

            if let Some(end) = bytes[start..].iter().position(|&b| b == b'"') {
                let symbol = &bytes[start..start + end];

                chunk[..symbol.len().min(16)].copy_from_slice(symbol);

                result = u128::from_le_bytes(chunk);
            }
        }

        Ok(result)
    }
}
