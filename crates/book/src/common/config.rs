use std::env;

use dotenv::dotenv;

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
        let db_name = env::var("POSTGRES_DB_PASSWORD").expect("POSTGRESQL DB_NAME configuration not found");
     
        format!("postgresql://{}:{}@{}:{}/{}", user,password,host,port,db_name)
    }
}
