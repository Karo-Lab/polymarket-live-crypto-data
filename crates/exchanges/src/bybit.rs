use exchanges_common::{
    error::ConnectorError,
    models::{BookLevel, DEFAULT_PRICE_SCALE, DEFAULT_QTY_SCALE, NormalizedBookEvent, Price, Size},
    traits::ExchangeConnectorAdapter,
};
use std::{borrow::Cow, str::FromStr};

use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes, client::IntoClientRequest};

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

#[derive(Debug)]
pub struct BybitConnectorAdapter;

impl ExchangeConnectorAdapter for BybitConnectorAdapter {
    fn get_source_name(&self) -> String {
        "Bybit".to_string()
    }
    fn get_url(&self) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, ConnectorError> {
        "wss://stream.bybit.com/v5/public/linear"
            .into_client_request()
            .map_err(|e| ConnectorError::WebSocket(e.to_string()))
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

    fn parse_market_event(
        &self,
        msg: &[u8],
    ) -> Result<Option<NormalizedBookEvent>, ConnectorError> {
        let parsed = match serde_json::from_slice::<BybitBookMessage<'_>>(msg) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        if parsed.topic.is_empty() || parsed.data.s.is_empty() {
            return Ok(None);
        }

        let mut bids = Vec::with_capacity(parsed.data.b.len());
        for level in &parsed.data.b {
            let Some(book_level) = parse_level(level) else {
                continue;
            };
            bids.push(book_level);
        }

        let mut asks = Vec::with_capacity(parsed.data.a.len());
        for level in &parsed.data.a {
            let Some(book_level) = parse_level(level) else {
                continue;
            };
            asks.push(book_level);
        }

        Ok(Some(NormalizedBookEvent {
            exchange: "bybit".to_string(),
            symbol: parsed.data.s.into_owned(),
            timestamp: parsed.ts,
            seq_id: parsed.data.u,
            prev_seq_id: parsed.data.u.saturating_sub(1),
            is_snapshot: parsed.event_type.as_ref() == "snapshot",
            price_scale: DEFAULT_PRICE_SCALE,
            size_scale: DEFAULT_QTY_SCALE,
            bids,
            asks,
        }))
    }
}

fn parse_level(level: &[Cow<'_, str>]) -> Option<BookLevel> {
    let price_raw = level.first()?;
    let size_raw = level.last()?;

    let price = Decimal::from_str(price_raw).ok()?;
    let size = Decimal::from_str(size_raw).ok()?;

    let scaled_price = (price * Decimal::from(DEFAULT_PRICE_SCALE)).round();
    let scaled_size = (size * Decimal::from(DEFAULT_QTY_SCALE)).round();

    Some(BookLevel {
        price: Price(scaled_price.to_u32()?),
        size: Size(scaled_size.to_u64()?),
    })
}
