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
    pub seq_id: i64,
    pub prev_seq_id: i64,
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

#[derive(Debug)]
pub struct OkxConnectorAdapter;

impl ExchangeConnectorAdapter for OkxConnectorAdapter {
    fn get_source_name(&self) -> String {
        "Okx".to_string()
    }

    fn get_url(&self) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, ConnectorError> {
        "wss://ws.okx.com:8443/ws/v5/public"
            .into_client_request()
            .map_err(|e| ConnectorError::WebSocket(e.to_string()))
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

    fn parse_market_event(
        &self,
        msg: &[u8],
    ) -> Result<Option<NormalizedBookEvent>, ConnectorError> {
        let parsed = match serde_json::from_slice::<OkxOrderBookMesssage<'_>>(msg) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        if parsed.arg.inst_id.is_empty() || parsed.data.is_empty() {
            return Ok(None);
        }

        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for delta in &parsed.data {
            bids.reserve(delta.bids.len());
            asks.reserve(delta.asks.len());

            for level in &delta.bids {
                let Some(book_level) = parse_level(level) else {
                    continue;
                };
                bids.push(book_level);
            }
            for level in &delta.asks {
                let Some(book_level) = parse_level(level) else {
                    continue;
                };
                asks.push(book_level);
            }
        }

        let seq = parsed.data.first().map_or(0, |v| v.seq_id);
        let prev_seq = parsed.data.first().map_or(0, |v| v.prev_seq_id);
        let ts = parsed
            .data
            .first()
            .and_then(|v| v.ts.parse::<i64>().ok())
            .unwrap_or_default();

        Ok(Some(NormalizedBookEvent {
            exchange: "okx".to_string(),
            symbol: parsed.arg.inst_id.into_owned(),
            timestamp: ts,
            seq_id: seq,
            prev_seq_id: prev_seq,
            is_snapshot: parsed.action.as_ref() == "snapshot",
            price_scale: DEFAULT_PRICE_SCALE,
            size_scale: DEFAULT_QTY_SCALE,
            bids,
            asks,
        }))
    }
}

fn parse_level(level: &OkxOrderLevel<'_>) -> Option<BookLevel> {
    let price = Decimal::from_str(&level.price).ok()?;
    let size = Decimal::from_str(&level.size).ok()?;

    let scaled_price = (price * Decimal::from(DEFAULT_PRICE_SCALE)).round();
    let scaled_size = (size * Decimal::from(DEFAULT_QTY_SCALE)).round();

    Some(BookLevel {
        price: Price(scaled_price.to_u32()?),
        size: Size(scaled_size.to_u64()?),
    })
}
