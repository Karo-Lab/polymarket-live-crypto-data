-- Unified Postgres metadata init for ingestion + archiver.
-- This project intentionally keeps a single up migration while schema is still early-stage.

CREATE TABLE IF NOT EXISTS table_registry (
    registry_id SERIAL PRIMARY KEY,
    table_name VARCHAR(120) NOT NULL UNIQUE,
    data_layer VARCHAR(20) NOT NULL DEFAULT 'BRONZE' CHECK (data_layer IN ('BRONZE', 'SILVER', 'GOLD')),
    database_type VARCHAR(20) NOT NULL DEFAULT 'questdb',
    exchange VARCHAR(50),
    resolution VARCHAR(20),
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS archives (
    archive_id BIGSERIAL PRIMARY KEY,
    registry_id INT NOT NULL,
    partition_date DATE NOT NULL,
    row_count BIGINT,
    file_path TEXT NOT NULL,
    archived_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_archives_registry
        FOREIGN KEY (registry_id)
        REFERENCES table_registry(registry_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_archives_registry_partition_file
        UNIQUE (registry_id, partition_date, file_path)
);

CREATE INDEX IF NOT EXISTS idx_archives_registry_partition_date
    ON archives(registry_id, partition_date);

CREATE TABLE IF NOT EXISTS pipeline_audit (
    ts TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    topic TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT,
    details JSONB
);

CREATE INDEX IF NOT EXISTS idx_pipeline_audit_ts_topic
    ON pipeline_audit(ts, topic);

-- Manual seed list: update this block whenever new QuestDB tables are introduced.
INSERT INTO table_registry (table_name, data_layer, database_type, exchange, resolution, description)
VALUES
    ('poly_live_price', 'BRONZE', 'questdb', 'polymarket', 'snapshot', 'Polymarket live orderbook snapshots'),
    ('poly_live_tbt', 'BRONZE', 'questdb', 'polymarket', 'tbt', 'Polymarket trade-by-trade stream'),
    ('exchanges_live_price', 'BRONZE', 'questdb', 'multi_exchange', 'snapshot', 'Centralized exchange live orderbook snapshots'),
    ('exchanges_live_tbt', 'BRONZE', 'questdb', 'multi_exchange', 'tbt', 'Centralized exchange trade-by-trade stream')
ON CONFLICT (table_name) DO NOTHING;
