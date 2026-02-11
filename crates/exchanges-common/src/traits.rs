use std::{borrow::Cow, fmt::Debug, hash::Hash};

use crate::{
    error::{ConnectorError, IngestionError},
    models::{LevelDelta, OrderBookL2},
};
use futures::stream::{SplitSink, SplitStream};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

pub trait AsyncStream: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncStream for T {}

pub(crate) type BoxStream = Box<dyn AsyncStream + Send + Unpin>;
pub(crate) type WsStream = WebSocketStream<MaybeTlsStream<BoxStream>>;
pub(crate) type WsWriter = SplitSink<WsStream, Message>;
pub(crate) type WsReader = SplitStream<WsStream>;

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
    const EXCHANGE_NAME: Cow<'_, str>;

    fn get_instrument<'a>(&self, msg: &'a Self::InputBookData<'_>) -> &'a str;

    fn get_timestamp<'a>(&self, msg: &'a Self::InputBookData<'_>) -> i64;

    fn is_snapshot<'a>(&self, msg: &Self::InputBookData<'a>) -> bool;

    fn get_seq_id<'a>(&self, _msg: &Self::InputBookData<'a>) -> i64 {
        0
    }

    fn get_prev_seq_id<'a>(&self, _msg: &Self::InputBookData<'a>) -> i64 {
        0
    }

    fn get_bids<'a>(&self, msg: &'a Self::InputBookData<'_>) -> &'a Vec<Vec<Cow<'a, str>>>;

    fn get_asks<'a>(&self, msg: &'a Self::InputBookData<'_>) -> &'a Vec<Vec<Cow<'a, str>>>;

    fn apply<'a, 'b>(
        &self,
        core: &'b mut OrderBookL2,
        msg: &Self::InputBookData<'a>,
    ) -> Vec<LevelDelta<'b>>;

    fn verify_integrity<'a>(&self, _core: &OrderBookL2, _msg: &Self::InputBookData<'a>) -> bool {
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
    fn push(
        &mut self,
        table: &str,
        columns: Vec<(&str, ColumnValue)>,
    ) -> impl std::future::Future<Output = Result<(), IngestionError>> + Send + Sync;
    fn flush(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), IngestionError>> + Send + Sync;
}

pub trait Cache<K, V>: Send + Sync {
    fn get<Q>(&self, k: &Q) -> impl std::future::Future<Output = Option<V>> + Send + Sync
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + Sync + ?Sized;
    fn set(&self, k: K, v: V) -> impl std::future::Future<Output = Option<V>> + Send + Sync;
    fn invalidate<Q>(&self, k: &Q) -> impl std::future::Future<Output = ()> + Send + Sync
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + Sync + ?Sized;

    fn try_get<Q>(&self, k: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + Sync + ?Sized;
    fn try_set(&self, k: K, v: V) -> Option<V>;
}
