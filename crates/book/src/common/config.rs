use std::env;

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
