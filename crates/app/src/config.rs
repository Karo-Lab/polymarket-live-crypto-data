use std::{env, path::Path};
use tracing_appender::non_blocking;
use tracing_subscriber::{
    EnvFilter, fmt::{self, layer}, layer::SubscriberExt, registry, util::SubscriberInitExt
};

pub struct QuestDBConfig {
    pub db_url: String,
    pub ilp_url: String,
}

impl QuestDBConfig {
    pub fn load() -> Self {
        dotenv::dotenv().ok();
        Self {
            db_url: Self::load_qdb_config(),
            ilp_url: Self::load_qdb_ilp_config(),
        }
    }
    fn load_qdb_config() -> String {
        let host = env::var("QUEST_DB_HOST").expect("QUEST DB HOST configuration not found");
        let port = env::var("QUEST_DB_PORT").expect("QUEST DB PORT configuration not found");

        format!(
            "http::addr={}:{};username=admin;password=quest;",
            host, port
        )
    }
    fn load_qdb_ilp_config() -> String {
        let host = env::var("QUEST_DB_HOST").expect("QUEST DB HOST configuration not found");
        let port = env::var("QUEST_DB_ILP").expect("QUEST DB ILP configuration not found");

        format!(
            "http::addr={}:{};username=admin;password=quest;",
            host, port
        )
    }
}

pub struct PostgresDBConfig {
    pub db_url: String,
}

impl PostgresDBConfig {
    pub fn load() -> Self {
        dotenv::dotenv().ok();
        Self {
            db_url: Self::load_postgres_config(),
        }
    }

    fn load_postgres_config() -> String {
        let host = env::var("POSTGRES_DB_HOST").expect("POSTGRESQL HOST configuration not found");
        let port = env::var("POSTGRES_DB_PORT").expect("POSTGRESQL PORT configuration not found");
        let user = env::var("POSTGRES_DB_USER").expect("POSTGRESQL USER configuration not found");
        let password =
            env::var("POSTGRES_DB_PASSWORD").expect("POSTGRESQL PASSWORD configuration not found");
        let db_name =
            env::var("POSTGRES_DB_NAME").expect("POSTGRESQL DB_NAME configuration not found");

        format!(
            "postgresql://{}:{}@{}:{}/{}",
            user, password, host, port, db_name
        )
    }
}

pub struct SystemLogging;

impl SystemLogging {
    pub fn init_logging(
        log_path: impl AsRef<Path>,
        log_file_name: impl AsRef<Path>,
    ) -> (non_blocking::WorkerGuard, non_blocking::WorkerGuard) {
        // init console logger
        let (console_writer, console_guard) = non_blocking(std::io::stdout());

        let console_layer = fmt::layer()
            .with_writer(console_writer)
            .compact()
            .with_file(true)
            .with_line_number(true)
            .with_target(false)
            .with_ansi(true)
            .with_timer(fmt::time::UtcTime::rfc_3339());

        let file_appender = tracing_appender::rolling::daily(log_path, log_file_name);

        // init file log storing
        let (file_writer, file_guard) = non_blocking(file_appender);
        
        let file_layer = fmt::layer()
            .with_writer(file_writer)
            .json()
            .with_ansi(false)
            .with_timer(fmt::time::UtcTime::rfc_3339());

        registry()
            .with(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
            .with(console_layer)
            .with(file_layer)
            .init();

        (console_guard, file_guard)
    }
}
