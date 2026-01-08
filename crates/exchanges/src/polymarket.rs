use core::traits::{ExchangeAdapter, ExchangeConnectorAdapter};
use std::{borrow::Cow, str::FromStr};

use rand::rand_core::le;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type")]
pub enum PolyCLOBMessage<'a> {
    #[serde(rename = "book")]
    Snapshot(#[serde(borrow)] PolyOrderBookL2Snapshot<'a>),
    #[serde(rename = "price_change")]
    L2Tick(#[serde(borrow)] PolyOrderBookL2Tick<'a>),
    #[serde(rename = "last_trade_price")]
    LastTrade(#[serde(borrow)] PolyLastTradePrice<'a>)
}

#[derive(Debug, Deserialize)]
pub(crate) struct PolyOrderBookL2Snapshot<'a> {
    #[serde(borrow)]
    pub market: Cow<'a, str>,
    #[serde(borrow)]
    pub asset_id: Cow<'a, str>,
    #[serde(borrow)]
    pub timestamp: Cow<'a, str>,
    #[serde(borrow)]
    pub hash: Cow<'a, str>,
    #[serde(borrow)]
    pub bids: Vec<PolyOrderBookSummary<'a>>,
    #[serde(borrow)]
    pub asks: Vec<PolyOrderBookSummary<'a>>,
    #[serde(borrow)]
    pub event_type: Cow<'a, str>,
    #[serde(borrow)]
    pub last_trade_price: Cow<'a, str>
}

#[derive(Debug, Deserialize)]
pub(crate) struct PolyOrderBookSummary<'a> {
    #[serde(borrow)]
    pub price: Cow<'a, str>,
    #[serde(borrow)]
    pub size: Cow<'a, str>
}

#[derive(Debug, Deserialize)]
pub(crate) struct PolyOrderBookL2Tick<'a> {
    #[serde(borrow)]
    pub market: Cow<'a, str>,
    #[serde(borrow)]
    pub price_changes: Vec<PolyOrderBookTick<'a>>,
    #[serde(borrow)]
    pub timestamp: Cow<'a, str>,
    #[serde(borrow)]
    pub event_type: Cow<'a, str>
}

#[derive(Debug, Deserialize)]
pub(crate) struct PolyOrderBookTick<'a> {
    #[serde(borrow)]
    pub asset_id: Cow<'a, str>,
    #[serde(borrow)]
    pub price: Cow<'a, str>,
    #[serde(borrow)]
    pub size: Cow<'a, str>,
    #[serde(borrow)]
    pub side: Cow<'a, str>,
    #[serde(borrow)]
    pub hash: Cow<'a, str>,
    #[serde(borrow)]
    pub best_bid: Cow<'a, str>,
    #[serde(borrow)]
    pub best_ask: Cow<'a, str>
}

#[derive(Debug, Deserialize)]
pub(crate) struct PolyLastTradePrice<'a> {
    #[serde(borrow)]
    pub market: Cow<'a, str>,
    #[serde(borrow)]
    pub asset_id: Cow<'a, str>,
    #[serde(borrow)]
    pub price: Cow<'a, str>,
    #[serde(borrow)]
    pub size: Cow<'a, str>,
    #[serde(borrow)]
    pub fee_rate_bps: Cow<'a, str>,
    #[serde(borrow)]
    pub side: Cow<'a, str>,
    #[serde(borrow)]
    pub timestamp: Cow<'a, str>,
    #[serde(borrow)]
    pub event_type: Cow<'a, str>,
    #[serde(borrow)]
    pub transaction_hash: Cow<'a, str>
}

pub struct PolyCLOBAdapter;

impl ExchangeAdapter for PolyCLOBAdapter {
    type InputBookData<'a> = PolyCLOBMessage<'a>;
    const EXCHANGE_NAME : Cow<'_,str> = Cow::Borrowed("polymarket"); 
    
    fn is_snapshot<'a>(&self, msg: &Self::InputBookData<'_>) -> bool {
        matches!(msg, PolyCLOBMessage::Snapshot(_))
    }
    fn apply<'a>(&self,core: &mut core::models::OrderBookL2, msg: &Self::InputBookData<'_>) {
        match msg {
            PolyCLOBMessage::Snapshot(snapshot) => {
                if core.instrument_id.ne(snapshot.asset_id.as_ref()) {
                    return;
                } 
                for level in snapshot.bids.iter() {
                    let price = Decimal::from_str(&level.price).unwrap_or_default();
                    let size = Decimal::from_str(&level.size).unwrap_or_default();
                    
                    if size.is_zero() {
                        core.bids.remove(&price);
                    } else {
                        core.bids.insert(price, size);
                    }
                }
                for level in snapshot.asks.iter() {
                    let price = Decimal::from_str(&level.price).unwrap_or_default();
                    let size = Decimal::from_str(&level.size).unwrap_or_default();
                    
                    if size.is_zero() {
                        core.asks.remove(&price);
                    } else {
                        core.asks.insert(price, size);
                    }
                }
            }
            PolyCLOBMessage::L2Tick(tick) => {
                for level in tick.price_changes.iter() {
                    if core.instrument_id.ne(level.asset_id.as_ref()) {
                        continue;
                    }
                    match level.side.as_ref() {
                        "BUY" => {
                            let price = Decimal::from_str(&level.price).unwrap_or_default();
                            let size = Decimal::from_str(&level.size).unwrap_or_default();
                            if size.is_zero() {
                                core.bids.remove(&price);
                            } else {
                                core.bids.insert(price, size);
                            }
                        },
                        "SELL" => {
                            let price = Decimal::from_str(&level.price).unwrap_or_default();
                            let size = Decimal::from_str(&level.size).unwrap_or_default();
                            if size.is_zero() {
                                core.asks.remove(&price);
                            } else {
                                core.asks.insert(price, size);
                            }
                        },
                        &_ => {}
                    }
                }
            }
            _ => {return;}
        }
    }
}

pub struct PolyCLOBConnectorAdapter;

impl ExchangeConnectorAdapter for PolyCLOBConnectorAdapter {
    fn get_url(&self) -> String {
        "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()
    }
    
    fn create_subscription(&self, instruments: &[String], is_unsub: bool) -> Message {
        let id: u8 = rand::random();
        
        let op = if is_unsub {"unsubscribe"} else {"subscribe"};
        
        let json_string = serde_json::json!({
            "operation": op,
            "assets_ids": instruments
        }).to_string();
        
        Message::Text(Utf8Bytes::from(json_string))
    }
    fn route_message(&self, msg: &impl AsRef<[u8]>) -> Result<u128, core::error::ConnectorError> {
        unimplemented!()
    }
}