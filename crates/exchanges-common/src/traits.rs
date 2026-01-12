use std::{borrow::{Cow}, fmt::{Debug}};

use futures::stream::{SplitSink, SplitStream};
use serde::{Deserialize};
use crate::{error::{ConnectorError, IngestionError}, models::{LevelDelta, OrderBookL2}};
use tokio::{net::TcpStream};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::{Message}};

pub(crate) type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
pub(crate) type WsReader = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub trait ExchangeConnectorAdapter {
    fn get_source_name(&self) -> String;
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
    
    fn get_instrument<'a>(&self, msg: &'a Self::InputBookData<'_>) -> &'a str;
    
    fn get_timestamp<'a>(&self, msg: &'a Self::InputBookData<'_>) -> i64;
    
    fn is_snapshot<'a>(&self, msg: &Self::InputBookData<'_>) -> bool;

    fn get_seq_id<'a>(&self, msg: &Self::InputBookData<'_>) -> i64 {
        0
    }
    
    fn get_prev_seq_id<'a>(&self, msg: &Self::InputBookData<'_>) -> i64 {
        0
    }
    
    fn get_bids<'a>(&self, msg: &'a Self::InputBookData<'_>) -> &'a Vec<Vec<Cow<'a, str>>>;
    
    fn get_asks<'a>(&self, msg: &'a Self::InputBookData<'_>) -> &'a Vec<Vec<Cow<'a, str>>>;
        
    fn apply<'a, 'b>(&self,core: &'b mut OrderBookL2, msg: &Self::InputBookData<'_>) -> Vec<LevelDelta<'b>>;

    fn verify_integrity<'a>(&self, _core: &OrderBookL2, _msg: &Self::InputBookData<'_>) -> bool {
        true
    }
}

#[derive(Debug)]
pub enum ColumnValue<'a> {
    Integer(i64),
    Double(f64),
    Varchar(&'a str),
    Timestamp(i64),
    DoubleArray(Vec<f64>),
    Array2dDouble(Vec<Vec<f64>>),
    Symbol(&'a str),
}

pub trait TableSchema: Send + Sync {
    fn table_name(&self) -> &str;
    fn get_columns(&self) -> Vec<(&str, ColumnValue<'_>)>;
}

pub trait IngestionBackend: Send + Sync {
    async fn push(&mut self, table: &str, columns: Vec<(&str, ColumnValue)>) -> Result<(), IngestionError>;
    async fn flush(&mut self) -> Result<(), IngestionError>;
}


