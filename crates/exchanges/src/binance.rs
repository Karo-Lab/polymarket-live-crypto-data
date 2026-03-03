use exchanges_common::{
    error::ConnectorError,
    models::{BookLevel, DEFAULT_PRICE_SCALE, DEFAULT_QTY_SCALE, NormalizedBookEvent, Price, Size},
    sbe::{
        ReadBuf,
        binance::{
            depth_diff_stream_event_codec::{
                DepthDiffStreamEventDecoder, SBE_SCHEMA_ID,
                SBE_TEMPLATE_ID as DEPTH_DIFF_TEMPLATE_ID,
            },
            depth_snapshot_stream_event_codec::{
                DepthSnapshotStreamEventDecoder, SBE_TEMPLATE_ID as DEPTH_SNAPSHOT_TEMPLATE_ID,
            },
            message_header_codec::{ENCODED_LENGTH as SBE_HEADER_LEN, MessageHeaderDecoder},
        },
    },
    traits::ExchangeConnectorAdapter,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Deserialize;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::tungstenite::{
    Message, Utf8Bytes, client::IntoClientRequest, http::HeaderValue,
};

const BINANCE_LOCAL_BOOK_DEPTH: usize = 200;

#[derive(Debug, Clone, Copy)]
enum BinanceSyncState {
    AwaitingBridge { snapshot_last_update_id: i64 },
    Synced { last_update_id: i64 },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestDepthSnapshot {
    last_update_id: i64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct BinanceConnectorAdapter {
    rest_client: reqwest::Client,
    sync_state: Arc<Mutex<HashMap<String, BinanceSyncState>>>,
}

impl Default for BinanceConnectorAdapter {
    fn default() -> Self {
        Self {
            rest_client: reqwest::Client::new(),
            sync_state: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl ExchangeConnectorAdapter for BinanceConnectorAdapter {
    fn get_source_name(&self) -> String {
        "Binance".to_string()
    }
    fn get_url(&self) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, ConnectorError> {
        dotenv::dotenv().ok();
        let url = "wss://stream-sbe.binance.com:9443/stream".to_string();
        let mut request = url
            .into_client_request()
            .map_err(|e| ConnectorError::WebSocket(e.to_string()))?;

        let binance_ed25519_key = std::env::var("BINANCE_ED25519_API_KEY").map_err(|e| {
            ConnectorError::Unknown(format!("missing BINANCE_ED25519_API_KEY env var: {e}"))
        })?;

        let header = HeaderValue::from_str(&binance_ed25519_key).map_err(|e| {
            ConnectorError::Unknown(format!("invalid BINANCE_ED25519_API_KEY header value: {e}"))
        })?;
        request.headers_mut().insert("X-MBX-APIKEY", header);

        Ok(request)
    }
    fn create_subscription(
        &self,
        instruments: &[String],
        is_unsub: bool,
    ) -> tokio_tungstenite::tungstenite::Message {
        let id: u8 = rand::random();

        let params: Vec<_> = instruments
            .iter()
            .map(|sym| format!("{}@depth", sym.to_ascii_lowercase()))
            .collect();

        let method = if is_unsub { "UNSUBSCRIBE" } else { "SUBSCRIBE" };

        let json_string = serde_json::json!({
            "method": method,
            "params": params,
            "id": id
        })
        .to_string();

        println!("PAYLOAD - {:?}", json_string);

        Message::Text(Utf8Bytes::from(json_string))
    }

    async fn bootstrap_events(
        &self,
        instruments: &[String],
    ) -> Result<Vec<NormalizedBookEvent>, ConnectorError> {
        let mut snapshots = Vec::with_capacity(instruments.len());

        for instrument in instruments {
            let symbol = instrument.to_ascii_uppercase();
            let url = format!(
                "https://api.binance.com/api/v3/depth?symbol={symbol}&limit={BINANCE_LOCAL_BOOK_DEPTH}"
            );

            let response = self.rest_client.get(url).send().await.map_err(|err| {
                ConnectorError::Unknown(format!("binance depth request failed: {err}"))
            })?;

            if !response.status().is_success() {
                return Err(ConnectorError::Unknown(format!(
                    "binance depth request failed with status {}",
                    response.status()
                )));
            }

            let payload = response.json::<RestDepthSnapshot>().await.map_err(|err| {
                ConnectorError::Unknown(format!("invalid binance depth response: {err}"))
            })?;

            let bids = parse_rest_levels(&payload.bids)?;
            let asks = parse_rest_levels(&payload.asks)?;

            set_snapshot_state(&self.sync_state, &symbol, payload.last_update_id)?;

            snapshots.push(NormalizedBookEvent {
                exchange: "binance".to_string(),
                symbol,
                timestamp: now_millis(),
                seq_id: payload.last_update_id,
                prev_seq_id: payload.last_update_id,
                is_snapshot: true,
                price_scale: DEFAULT_PRICE_SCALE,
                size_scale: DEFAULT_QTY_SCALE,
                bids,
                asks,
            });
        }

        Ok(snapshots)
    }

    fn parse_market_event(
        &self,
        msg: &[u8],
    ) -> Result<
        Option<exchanges_common::models::NormalizedBookEvent>,
        exchanges_common::error::ConnectorError,
    > {
        if msg.len() < SBE_HEADER_LEN {
            return Ok(None);
        }

        if matches!(msg.first(), Some(b'{') | Some(b'[')) {
            return Ok(None);
        }

        let read_buf = ReadBuf::new(msg);
        let header_decoder = MessageHeaderDecoder::default().wrap(read_buf, 0);

        if header_decoder.schema_id() != SBE_SCHEMA_ID {
            return Ok(None);
        }

        match header_decoder.template_id() {
            DEPTH_DIFF_TEMPLATE_ID => parse_diff_event(header_decoder, &self.sync_state),
            DEPTH_SNAPSHOT_TEMPLATE_ID => Ok(Some(parse_snapshot_event(
                header_decoder,
                &self.sync_state,
            )?)),
            _ => Ok(None),
        }
    }
}

fn parse_diff_event(
    header_decoder: MessageHeaderDecoder<ReadBuf<'_>>,
    sync_state: &Arc<Mutex<HashMap<String, BinanceSyncState>>>,
) -> Result<Option<NormalizedBookEvent>, ConnectorError> {
    let ticker_decoder = DepthDiffStreamEventDecoder::default().header(header_decoder);

    let event_time = ticker_decoder.event_time();
    let first_book_update_id = ticker_decoder.first_book_update_id();
    let last_book_update_id = ticker_decoder.last_book_update_id();
    let price_exp = ticker_decoder.price_exponent();
    let qty_exp = ticker_decoder.qty_exponent();

    let mut bids_decoder = ticker_decoder.bids_decoder();
    let mut bids: Vec<BookLevel> = Vec::with_capacity(bids_decoder.count() as usize);

    while let Ok(Some(_index)) = bids_decoder.advance() {
        let price = bids_decoder.price();
        let qty = bids_decoder.qty();

        let Some(book_level) = parse_level(price, qty, price_exp, qty_exp) else {
            continue;
        };
        bids.push(book_level);
    }

    let ticker_decoder = bids_decoder.parent().map_err(|err| {
        ConnectorError::Unknown(format!("missing parent after bids decode: {err:?}"))
    })?;
    let mut asks_decoder = ticker_decoder.asks_decoder();
    let mut asks = Vec::with_capacity(asks_decoder.count() as usize);
    while let Ok(Some(_index)) = asks_decoder.advance() {
        let price = asks_decoder.price();
        let qty = asks_decoder.qty();

        let Some(book_level) = parse_level(price, qty, price_exp, qty_exp) else {
            continue;
        };

        asks.push(book_level);
    }

    let mut ticker_decoder = asks_decoder.parent().map_err(|err| {
        ConnectorError::Unknown(format!("missing parent after asks decode: {err:?}"))
    })?;
    let symbol = {
        let (offset, length) = ticker_decoder.symbol_decoder();
        let symbol_bytes = ticker_decoder.symbol_slice((offset, length));
        std::str::from_utf8(symbol_bytes)
            .unwrap_or("UNKNOWN")
            .to_ascii_uppercase()
    };

    let Some((seq_id, prev_seq_id)) = resolve_diff_sequence(
        sync_state,
        &symbol,
        first_book_update_id,
        last_book_update_id,
    )?
    else {
        return Ok(None);
    };

    Ok(Some(NormalizedBookEvent {
        exchange: "binance".to_string(),
        symbol,
        timestamp: event_time,
        seq_id,
        prev_seq_id,
        is_snapshot: false,
        price_scale: DEFAULT_PRICE_SCALE,
        size_scale: DEFAULT_QTY_SCALE,
        bids,
        asks,
    }))
}

fn parse_snapshot_event(
    header_decoder: MessageHeaderDecoder<ReadBuf<'_>>,
    sync_state: &Arc<Mutex<HashMap<String, BinanceSyncState>>>,
) -> Result<NormalizedBookEvent, ConnectorError> {
    let ticker_decoder = DepthSnapshotStreamEventDecoder::default().header(header_decoder);

    let event_time = ticker_decoder.event_time();
    let book_update_id = ticker_decoder.book_update_id();
    let price_exp = ticker_decoder.price_exponent();
    let qty_exp = ticker_decoder.qty_exponent();

    let mut bids_decoder = ticker_decoder.bids_decoder();
    let mut bids: Vec<BookLevel> = Vec::with_capacity(bids_decoder.count() as usize);

    while let Ok(Some(_index)) = bids_decoder.advance() {
        let price = bids_decoder.price();
        let qty = bids_decoder.qty();

        let Some(book_level) = parse_level(price, qty, price_exp, qty_exp) else {
            continue;
        };
        bids.push(book_level);
    }

    let ticker_decoder = bids_decoder.parent().map_err(|err| {
        ConnectorError::Unknown(format!("missing parent after bids decode: {err:?}"))
    })?;
    let mut asks_decoder = ticker_decoder.asks_decoder();
    let mut asks = Vec::with_capacity(asks_decoder.count() as usize);
    while let Ok(Some(_index)) = asks_decoder.advance() {
        let price = asks_decoder.price();
        let qty = asks_decoder.qty();

        let Some(book_level) = parse_level(price, qty, price_exp, qty_exp) else {
            continue;
        };

        asks.push(book_level);
    }

    let mut ticker_decoder = asks_decoder.parent().map_err(|err| {
        ConnectorError::Unknown(format!("missing parent after asks decode: {err:?}"))
    })?;
    let symbol = {
        let (offset, length) = ticker_decoder.symbol_decoder();
        let symbol_bytes = ticker_decoder.symbol_slice((offset, length));
        std::str::from_utf8(symbol_bytes)
            .unwrap_or("UNKNOWN")
            .to_ascii_uppercase()
    };

    set_snapshot_state(sync_state, &symbol, book_update_id)?;

    Ok(NormalizedBookEvent {
        exchange: "binance".to_string(),
        symbol,
        timestamp: event_time,
        seq_id: book_update_id,
        prev_seq_id: book_update_id,
        is_snapshot: true,
        price_scale: DEFAULT_PRICE_SCALE,
        size_scale: DEFAULT_QTY_SCALE,
        bids,
        asks,
    })
}

fn resolve_diff_sequence(
    sync_state: &Arc<Mutex<HashMap<String, BinanceSyncState>>>,
    symbol: &str,
    first_book_update_id: i64,
    last_book_update_id: i64,
) -> Result<Option<(i64, i64)>, ConnectorError> {
    let mut guard = sync_state
        .lock()
        .map_err(|_| ConnectorError::Unknown("binance sync state lock poisoned".to_string()))?;

    let Some(state) = guard.get(symbol).copied() else {
        return Ok(None);
    };

    let sequence = match state {
        BinanceSyncState::AwaitingBridge {
            snapshot_last_update_id,
        } => {
            if last_book_update_id <= snapshot_last_update_id {
                return Ok(None);
            }

            let next_expected = snapshot_last_update_id.saturating_add(1);
            if first_book_update_id <= next_expected && next_expected <= last_book_update_id {
                guard.insert(
                    symbol.to_string(),
                    BinanceSyncState::Synced {
                        last_update_id: last_book_update_id,
                    },
                );
                Some((last_book_update_id, snapshot_last_update_id))
            } else {
                None
            }
        }
        BinanceSyncState::Synced { last_update_id } => {
            if last_book_update_id <= last_update_id {
                return Ok(None);
            }

            let next_expected = last_update_id.saturating_add(1);
            let prev_seq_id = if first_book_update_id <= next_expected {
                last_update_id
            } else {
                first_book_update_id.saturating_sub(1)
            };

            guard.insert(
                symbol.to_string(),
                BinanceSyncState::Synced {
                    last_update_id: last_book_update_id,
                },
            );
            Some((last_book_update_id, prev_seq_id))
        }
    };

    Ok(sequence)
}

fn set_snapshot_state(
    sync_state: &Arc<Mutex<HashMap<String, BinanceSyncState>>>,
    symbol: &str,
    snapshot_last_update_id: i64,
) -> Result<(), ConnectorError> {
    let mut guard = sync_state
        .lock()
        .map_err(|_| ConnectorError::Unknown("binance sync state lock poisoned".to_string()))?;
    guard.insert(
        symbol.to_string(),
        BinanceSyncState::AwaitingBridge {
            snapshot_last_update_id,
        },
    );
    Ok(())
}

fn parse_rest_levels(levels: &[(String, String)]) -> Result<Vec<BookLevel>, ConnectorError> {
    let mut out = Vec::with_capacity(levels.len());

    for (price_raw, qty_raw) in levels {
        let price = Decimal::from_str(price_raw).map_err(|err| {
            ConnectorError::ParseError(format!("invalid binance price {price_raw}: {err}"))
        })?;
        let qty = Decimal::from_str(qty_raw).map_err(|err| {
            ConnectorError::ParseError(format!("invalid binance qty {qty_raw}: {err}"))
        })?;

        let scaled_price = (price * Decimal::from(DEFAULT_PRICE_SCALE)).round();
        let scaled_size = (qty * Decimal::from(DEFAULT_QTY_SCALE)).round();

        let Some(price) = scaled_price.to_u32() else {
            return Err(ConnectorError::ParseError(format!(
                "scaled price overflow for {price_raw}"
            )));
        };
        let Some(size) = scaled_size.to_u64() else {
            return Err(ConnectorError::ParseError(format!(
                "scaled qty overflow for {qty_raw}"
            )));
        };

        out.push(BookLevel {
            price: Price(price),
            size: Size(size),
        });
    }

    Ok(out)
}

fn now_millis() -> i64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    i64::try_from(duration.as_millis()).unwrap_or(0)
}

fn parse_level(price_raw: i64, qty_raw: i64, price_exp: i8, qty_exp: i8) -> Option<BookLevel> {
    let price_decimal = decimal_from_mantissa(price_raw, price_exp)?;
    let qty_decimal = decimal_from_mantissa(qty_raw, qty_exp)?;

    let scaled_price = (price_decimal * Decimal::from(DEFAULT_PRICE_SCALE)).round();
    let scaled_size = (qty_decimal * Decimal::from(DEFAULT_QTY_SCALE)).round();

    Some(BookLevel {
        price: Price(scaled_price.to_u32()?),
        size: Size(scaled_size.to_u64()?),
    })
}

fn decimal_from_mantissa(raw: i64, exponent: i8) -> Option<Decimal> {
    if raw.is_negative() {
        return None;
    }

    if exponent.is_negative() {
        return Some(Decimal::from_i128_with_scale(
            i128::from(raw),
            exponent.unsigned_abs() as u32,
        ));
    }

    let factor = 10i64.checked_pow(exponent as u32)?;
    Decimal::from(raw).checked_mul(Decimal::from(factor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchanges_common::traits::ExchangeConnectorAdapter;

    fn push_u16_le(buf: &mut Vec<u8>, value: u16) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    fn push_i64_le(buf: &mut Vec<u8>, value: i64) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    fn build_diff_event_frame() -> Vec<u8> {
        let mut buf = Vec::new();

        // SBE message header
        push_u16_le(&mut buf, 26); // blockLength
        push_u16_le(&mut buf, 10003); // templateId (DepthDiffStreamEvent)
        push_u16_le(&mut buf, 1); // schemaId
        push_u16_le(&mut buf, 0); // version

        // Fixed fields
        push_i64_le(&mut buf, 1_777_777_777_000_000); // eventTime
        push_i64_le(&mut buf, 1001); // firstBookUpdateId
        push_i64_le(&mut buf, 1002); // lastBookUpdateId
        buf.push((-4i8) as u8); // priceExponent
        buf.push((-6i8) as u8); // qtyExponent

        // bids group (1 row)
        push_u16_le(&mut buf, 16); // blockLength
        push_u16_le(&mut buf, 1); // numInGroup
        push_i64_le(&mut buf, 123_456); // price mantissa
        push_i64_le(&mut buf, 987_654); // qty mantissa

        // asks group (1 row)
        push_u16_le(&mut buf, 16); // blockLength
        push_u16_le(&mut buf, 1); // numInGroup
        push_i64_le(&mut buf, 123_556); // price mantissa
        push_i64_le(&mut buf, 888_888); // qty mantissa

        // symbol varString8
        let symbol = b"BTCUSDT";
        buf.push(symbol.len() as u8);
        buf.extend_from_slice(symbol);

        buf
    }

    fn build_snapshot_event_frame() -> Vec<u8> {
        let mut buf = Vec::new();

        // SBE message header
        push_u16_le(&mut buf, 18); // blockLength
        push_u16_le(&mut buf, 10002); // templateId (DepthSnapshotStreamEvent)
        push_u16_le(&mut buf, 1); // schemaId
        push_u16_le(&mut buf, 0); // version

        // Fixed fields
        push_i64_le(&mut buf, 1_777_777_777_000_000); // eventTime
        push_i64_le(&mut buf, 5555); // bookUpdateId
        buf.push((-4i8) as u8); // priceExponent
        buf.push((-6i8) as u8); // qtyExponent

        // bids group (1 row)
        push_u16_le(&mut buf, 16); // blockLength
        push_u16_le(&mut buf, 1); // numInGroup
        push_i64_le(&mut buf, 12_345); // price mantissa
        push_i64_le(&mut buf, 50_000); // qty mantissa

        // asks group (1 row)
        push_u16_le(&mut buf, 16); // blockLength
        push_u16_le(&mut buf, 1); // numInGroup
        push_i64_le(&mut buf, 12_355); // price mantissa
        push_i64_le(&mut buf, 60_000); // qty mantissa

        // symbol varString8
        let symbol = b"ETHUSDT";
        buf.push(symbol.len() as u8);
        buf.extend_from_slice(symbol);

        buf
    }

    #[test]
    fn parse_diff_event_decodes_symbol_and_scales_levels() {
        let adapter = BinanceConnectorAdapter::default();
        set_snapshot_state(&adapter.sync_state, "BTCUSDT", 1000).expect("seed sync state");
        let frame = build_diff_event_frame();

        let event = adapter
            .parse_market_event(&frame)
            .expect("parse should not error")
            .expect("diff frame should decode");

        assert_eq!(event.exchange, "binance");
        assert_eq!(event.symbol, "BTCUSDT");
        assert!(!event.is_snapshot);
        assert_eq!(event.price_scale, DEFAULT_PRICE_SCALE);
        assert_eq!(event.size_scale, DEFAULT_QTY_SCALE);
        assert_eq!(event.seq_id, 1002);
        assert_eq!(event.prev_seq_id, 1000);
        assert_eq!(event.bids.len(), 1);
        assert_eq!(event.asks.len(), 1);
        assert_eq!(event.bids[0].price.0, 123_456);
        assert_eq!(event.bids[0].size.0, 987_654);
        assert_eq!(event.asks[0].price.0, 123_556);
        assert_eq!(event.asks[0].size.0, 888_888);
    }

    #[test]
    fn parse_diff_event_is_ignored_without_snapshot_state() {
        let adapter = BinanceConnectorAdapter::default();
        let frame = build_diff_event_frame();

        let event = adapter
            .parse_market_event(&frame)
            .expect("parse should not error");

        assert!(event.is_none());
    }

    #[test]
    fn parse_snapshot_event_template_is_supported() {
        let adapter = BinanceConnectorAdapter::default();
        let frame = build_snapshot_event_frame();

        let event = adapter
            .parse_market_event(&frame)
            .expect("parse should not error")
            .expect("snapshot frame should decode");

        assert_eq!(event.symbol, "ETHUSDT");
        assert!(event.is_snapshot);
        assert_eq!(event.seq_id, 5555);
        assert_eq!(event.prev_seq_id, 5555);
        assert_eq!(event.bids.len(), 1);
        assert_eq!(event.asks.len(), 1);
    }

    #[test]
    fn parse_rest_levels_scales_values() {
        let levels = vec![("12.3456".to_string(), "0.500000".to_string())];
        let parsed = parse_rest_levels(&levels).expect("rest levels should parse");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].price.0, 123_456);
        assert_eq!(parsed[0].size.0, 500_000);
    }
}
