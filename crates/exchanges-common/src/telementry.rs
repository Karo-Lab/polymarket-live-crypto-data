use chrono::{DateTime, Utc};
use derive_builder::Builder;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use std::{borrow::Cow, fmt::Debug};

use crate::{
    models::{BookSnapShot, BookTbT},
    traits::{ColumnValue, TableSchema},
};

#[derive(Debug, Serialize)]
pub enum SystemEvent<'a> {
    L2LOBTop {
        timestamp: i64,
        best_bid_price: Decimal,
        best_bid_size: Decimal,
        best_ask_price: Decimal,
        best_ask_size: Decimal,
    },
    L2LOBState {
        #[serde(borrow)]
        state: &'a str,
    },
}

#[derive(Debug)]
pub enum IngestionEvent {
    Snapshot(BookSnapShot),
    Tick(BookTbT),
}

impl TableSchema for IngestionEvent {
    fn table_name(&self) -> &str {
        match self {
            IngestionEvent::Snapshot(s) => s.table_name(),
            IngestionEvent::Tick(t) => t.table_name(),
        }
    }
    fn get_columns(&self) -> Vec<(&str, ColumnValue<'_>)> {
        match self {
            IngestionEvent::Snapshot(s) => s.get_columns(),
            IngestionEvent::Tick(t) => t.get_columns(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum AuditLevel {
    Info,
    Warning,
    Error,
    Critical,
}

impl AsRef<str> for AuditLevel {
    fn as_ref(&self) -> &str {
        match self {
            AuditLevel::Critical => "critical",
            AuditLevel::Error => "error",
            AuditLevel::Warning => "warning",
            AuditLevel::Info => "info",
        }
    }
}

#[derive(Debug, Clone, Builder, Serialize)]
#[builder(pattern = "owned")]
#[builder(setter(into))]
pub struct AuditEvent {
    #[builder(default = "Utc::now()")]
    pub timestamp: DateTime<Utc>,
    pub topic: String,
    pub level: AuditLevel,
    pub message: String,

    #[builder(default, setter(strip_option))]
    pub details: Option<Value>,
}

impl std::fmt::Display for AuditEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ts = self.timestamp.to_rfc3339();
        write!(
            f,
            "Timestamp: {} - topic: {} - level: {} - message: {}",
            ts,
            self.topic,
            self.level.as_ref(),
            self.message
        )
    }
}

impl AuditEvent {
    pub fn new(
        topic: impl Into<String>,
        msg: impl Into<String>,
        details: Option<Value>,
        level: AuditLevel,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            topic: topic.into(),
            level,
            message: msg.into(),
            details,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogEventCategory {
    Ingestion,
    System,
    Audit,
}

impl AsRef<str> for LogEventCategory {
    fn as_ref(&self) -> &str {
        match self {
            LogEventCategory::Audit => "LogEventCategory::Audit",
            LogEventCategory::Ingestion => "LogEventCategory::Ingestion",
            LogEventCategory::System => "LogEventCategory::System",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StandardEvent<'a, T>
where
    T: Serialize,
{
    pub version: &'a str,
    pub service: Cow<'a, str>,
    pub category: LogEventCategory,
    pub event_name: Cow<'a, str>,
    pub data: T,
}

impl<'a, T> StandardEvent<'a, T>
where
    T: Serialize,
{
    pub fn new(
        category: LogEventCategory,
        name: impl Into<Cow<'a, str>>,
        service_name: impl Into<Cow<'a, str>>,
        data: T,
    ) -> Self {
        Self {
            version: "1.0",
            service: service_name.into(),
            category,
            event_name: name.into(),
            data,
        }
    }
}

#[derive(Debug, Serialize)]
pub enum ErrorSeverity {
    Critical,
    Recoverable,
    Fatal,
}

#[derive(Debug, Serialize)]
pub struct ErrorPayload<'a> {
    error_code: &'a str,
    messsage: &'a str,
    severity: ErrorSeverity,
    is_retryable: bool,
}

impl<'a> ErrorPayload<'a> {
    #[must_use]
    pub fn new(code: &'a str, err: &'a str, severity: ErrorSeverity, retryable: bool) -> Self {
        Self {
            error_code: code,
            messsage: err,
            severity,
            is_retryable: retryable,
        }
    }
}

#[macro_export]
macro_rules! log_info {
    ($category:expr, $name:expr, $service:expr, $data:expr) => {
        if tracing::enabled!(tracing::Level::INFO) {
            let event = $crate::telementry::StandardEvent::new($category, $name, $service, $data);

            tracing::info!(event = ?event, "log_info");
        }
    };
}

#[macro_export]
macro_rules! log_warn {
    ($category:expr, $name:expr, $service:expr, $data:expr) => {
        if tracing::enabled!(tracing::Level::WARN) {
            let event = $crate::telementry::StandardEvent::new($category, $name, $service, $data);

            tracing::warn!(event = ?event, "log_warning");
        }
    };
}

#[macro_export]
macro_rules! log_error {
    ($category:expr, $name:expr, $service:expr, $payload:expr) => {
        if tracing::enabled!(tracing::Level::ERROR) {
            let event = $crate::telementry::StandardEvent::new($category, $name, $service, $payload);

            tracing::error!(event = ?event, "log_error");
        }
    };
}
