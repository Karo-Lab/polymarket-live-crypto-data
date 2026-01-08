use std::borrow::{Borrow, Cow};

use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use serde::{Deserialize, Serialize};
use crate::{error::ConnectorError, models::OrderBookL2};
use tokio::{net::TcpStream};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::{Message, Utf8Bytes}};

pub(crate) type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
pub(crate) type WsReader = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub trait ExchangeConnectorAdapter {
    fn get_url(&self) -> String;
    fn create_subscription(&self, instruments: &[String], is_unsub: bool) -> Message;
    fn route_message(&self, msg: &impl AsRef<[u8]>) -> Result<u128, ConnectorError>;
    fn ping_interval(&self) -> Option<tokio::time::Duration> {
        None
    }
   fn create_ping(&self, _ws_writer: &mut WsWriter) -> Option<Message> {
       None
   }
}

pub trait ExchangeAdapter {
    type InputBookData<'a>: Send + Sync + Deserialize<'a>;
    const EXCHANGE_NAME : Cow<'_,str>;

    fn is_snapshot<'a>(&self, msg: &Self::InputBookData<'_>) -> bool;

    fn get_seq_id<'a>(&self, msg: &Self::InputBookData<'_>) -> u64 {
        0
    }
    
    fn get_prev_seq_id<'a>(&self, msg: &Self::InputBookData<'_>) -> u64 {
        0
    }
        
    fn apply<'a>(&self,core: &mut OrderBookL2, msg: &Self::InputBookData<'_>);

    fn verify_integrity<'a>(&self, _core: &OrderBookL2, _msg: &Self::InputBookData<'_>) -> bool {
        true
    }
}

 