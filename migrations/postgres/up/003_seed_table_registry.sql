CREATE TABLE IF NOT EXISTS pipeline_audit (
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    topic TEXT NOT NULL,
    level TEXT NOT NULL,
    message VARCHAR,
    details JSONB
);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp_topic ON pipeline_audit(timestamp, topic);
DROP TABLE IF EXISTS pipline_audits;
DROP TABLE IF EXISTS pipline_audit;