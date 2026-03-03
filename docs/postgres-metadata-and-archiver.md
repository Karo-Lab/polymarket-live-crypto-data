# Postgres Metadata and Archiver Workflow

## Purpose

Postgres stores operational metadata for QuestDB table archiving.
The archiver (`scripts/cron/archiver.py`) reads `table_registry` to decide which tables to process and writes exported file records into `archives`.

## Migration Strategy (Current)

This project currently uses a **single Postgres init migration**:

- `migrations/postgres/up/001_create_meta_data_up.sql`

Rationale:

- project is early-stage
- schema churn is still expected
- simpler reset/re-init during local development

## Tables

- `table_registry`: list of known data tables and metadata used by archiver
- `archives`: one row per archived output file and partition date
- `pipeline_audit`: generic operational audit log table

## Manual Seed Workflow (Small Scale)

When a new QuestDB table is created, add it to the seed block in:

- `migrations/postgres/up/001_create_meta_data_up.sql`

Use this pattern:

```sql
INSERT INTO table_registry (table_name, data_layer, database_type, exchange, resolution, description)
VALUES ('new_table_name', 'BRONZE', 'questdb', 'source_name', 'snapshot', 'What this table stores')
ON CONFLICT (table_name) DO NOTHING;
```

Notes:

- Keep `data_layer='BRONZE'` for tables that should be archived by current archiver logic.
- Keep inserts idempotent with `ON CONFLICT (table_name) DO NOTHING`.

## Operational Checks

Verify seeded registry rows:

```sql
SELECT registry_id, table_name, data_layer, database_type, is_active
FROM table_registry
ORDER BY table_name;
```

Verify archive tracking:

```sql
SELECT registry_id, partition_date, file_path, row_count, archived_at
FROM archives
ORDER BY archived_at DESC
LIMIT 50;
```
