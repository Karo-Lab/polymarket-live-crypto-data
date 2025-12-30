CREATE TABLE IF NOT EXISTS table_registry (
    registry_id SERIAL PRIMARY KEY,
    table_name VARCHAR(120) UNIQUE,
    data_layer VARCHAR(20) CHECK (data_layer IN ('BRONZE', 'SILVER','GOLD')),
    exchange VARCHAR(50),
    resolution VARCHAR(10),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS archives (
    archive_id SERIAL PRIMARY KEY, 
    registry_id INT,               
    partition_date TEXT,
    row_count BIGINT,
    file_path TEXT,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_registry 
        FOREIGN KEY(registry_id) 
        REFERENCES table_registry(registry_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_archives_registry_id ON archives(registry_id);

INSERT INTO table_registry (table_name, data_layer,exchange,resolution,description)
VALUES ('okx_live_price','BRONZE', 'okx','tick','Live L2 okx orderbook data')