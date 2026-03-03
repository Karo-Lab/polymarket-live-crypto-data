-- Down migration for Postgres metadata init.
-- Drop dependent tables first to satisfy FK constraints.

DROP TABLE IF EXISTS archives;
DROP TABLE IF EXISTS pipeline_audit;
DROP TABLE IF EXISTS table_registry;
