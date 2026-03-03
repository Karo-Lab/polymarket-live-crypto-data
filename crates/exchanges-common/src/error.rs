use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("websocket error: {0}")]
    WebSocket(String),
    #[error("failed to parse symbol {0}")]
    ParseError(String),
    #[error("unknown error: {0}")]
    Unknown(String),
}

#[derive(Debug, Error)]
pub enum IngestionError {
    #[error("Error pushing data: {0}")]
    IngestionFailed(String),
    #[error("Database error: {0}")]
    DbError(String),
    #[error("Type error: {0}")]
    TypeError(String),
}

#[derive(Debug)]
pub enum TaskError {
    Panic,
}
