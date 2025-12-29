CREATE TABLE IF NOT EXISTS pipline_audits (
    ts TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    expected_val BIGINT,
    actual_val BIGINT,
    details JSONB
);
CREATE INDEX IF NOT EXISTS idx_audit_symbol_ts ON pipline_audits(symbol, ts);