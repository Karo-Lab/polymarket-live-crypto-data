use deadpool_postgres::{Config, ManagerConfig, Pool, PoolConfig, Runtime, tokio_postgres::NoTls};

use crate::common::config::PostgresDBConfig;

pub fn create_pool() -> Pool {
    let env = PostgresDBConfig::load();
    
    let mut cfg = Config::new();
    
    cfg.url = Some(env.db_url);
    
    cfg.pool = Some(PoolConfig::new(20));
    
    cfg.manager = Some(ManagerConfig {
        recycling_method: deadpool_postgres::RecyclingMethod::Fast
    });
    
    cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("Failed to create Postgres connection pool")
}