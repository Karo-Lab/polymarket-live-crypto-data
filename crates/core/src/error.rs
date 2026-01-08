use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("websocket error: {0}")]
    WebSocket(String),
    #[error("failed to parse symbol {0}")]
    ParseError(String),
    #[error("unknown error: {0}")]
    Unknown(String)
}