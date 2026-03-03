use std::{fmt::Debug, hash::Hash};

use crate::{
    error::{ConnectorError, IngestionError},
    models::NormalizedBookEvent,
};
use futures::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, http::Request},
};

pub trait AsyncStream: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncStream for T {}

pub(crate) type BoxStream = Box<dyn AsyncStream + Send + Unpin>;
pub(crate) type WsStream = WebSocketStream<MaybeTlsStream<BoxStream>>;
pub(crate) type WsWriter = SplitSink<WsStream, Message>;
pub(crate) type WsReader = SplitStream<WsStream>;

pub trait ExchangeConnectorAdapter {
    fn get_source_name(&self) -> String;
    fn get_url(&self) -> Result<Request<()>, ConnectorError>;
    fn create_subscription(&self, instruments: &[String], is_unsub: bool) -> Message;
    fn bootstrap_events(
        &self,
        _instruments: &[String],
    ) -> impl std::future::Future<Output = Result<Vec<NormalizedBookEvent>, ConnectorError>> + Send
    {
        async { Ok(Vec::new()) }
    }
    fn parse_market_event(
        &self,
        _msg: &[u8],
    ) -> Result<Option<NormalizedBookEvent>, ConnectorError> {
        Ok(None)
    }
    fn ping_interval(&self) -> Option<tokio::time::Duration> {
        None
    }
    fn create_ping(&self, _ws_writer: &mut WsWriter) -> Option<Message> {
        None
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
