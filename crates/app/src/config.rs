use std::env;
use tracing_appender::non_blocking;
use tracing_subscriber::{fmt, EnvFilter};

pub struct QuestDBConfig {
    pub db_url: String,
    pub ilp_url: String,
}

impl QuestDBConfig {
    pub fn load() -> Self {
        dotenv::dotenv().ok();
        Self { 
            db_url: Self::load_qdb_config(),
            ilp_url: Self::load_qdb_ilp_config()
        }
    }
    fn load_qdb_config() -> String {
        let host = env::var("QUEST_DB_HOST").expect("QUEST DB HOST configuration not found");
        let port = env::var("QUEST_DB_PORT").expect("QUEST DB PORT configuration not found");

        format!("http::addr={}:{};username=admin;password=quest;", host, port)
    }
    fn load_qdb_ilp_config() -> String {
        let host = env::var("QUEST_DB_HOST").expect("QUEST DB HOST configuration not found");
        let port = env::var("QUEST_DB_ILP").expect("QUEST DB ILP configuration not found");
        
        format!("http::addr={}:{};username=admin;password=quest;", host, port)
    }
}

pub struct PostgresDBConfig {
    pub db_url: String
}

impl PostgresDBConfig {
    pub fn load() -> Self {
        dotenv::dotenv().ok();
        Self {
            db_url: Self::load_postgres_config()
        }
    }
    
    fn load_postgres_config() -> String {
        let host = env::var("POSTGRES_DB_HOST").expect("POSTGRESQL HOST configuration not found");
        let port = env::var("POSTGRES_DB_PORT").expect("POSTGRESQL PORT configuration not found");
        let user = env::var("POSTGRES_DB_USER").expect("POSTGRESQL USER configuration not found");
        let password = env::var("POSTGRES_DB_PASSWORD").expect("POSTGRESQL PASSWORD configuration not found");
        let db_name = env::var("POSTGRES_DB_NAME").expect("POSTGRESQL DB_NAME configuration not found");
     
        format!("postgresql://{}:{}@{}:{}/{}", user,password,host,port,db_name)
    }
}


pub struct SystemLogging;

impl SystemLogging {
    pub fn init_logging() -> non_blocking::WorkerGuard {
        let (non_blocking, guard) = non_blocking(std::io::stdout());
        
        fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
            .with_timer(fmt::time::UtcTime::rfc_3339())
            .with_file(true)
            .with_ansi(true)
            .with_target(false)
            .with_line_number(true)
            .with_writer(non_blocking)
            .compact()
            .init();
        
        guard
    }
}