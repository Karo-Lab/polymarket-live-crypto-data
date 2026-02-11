from datetime import datetime, timedelta
from typing import List, Set, Tuple

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from psycopg import OperationalError, connect

from common.config import postgres_db_cfg, quest_db_cfg
from common.logger import logger, setup_logger

setup_logger("questdb_archiver")


def get_bronze_tables(pg_conn) -> List[dict]:
    """Fetch all BRONZE table metadata from the registry."""
    query = "SELECT registry_id, table_name, exchange FROM table_registry WHERE data_layer = 'BRONZE';"
    with pg_conn.cursor() as cur:
        cur.execute(query)
        return [
            {"registry_id": row[0], "table_name": row[1], "exchange": row[2]}
            for row in cur.fetchall()
        ]


def get_archived_dates(pg_conn, registry_id: int) -> Set[str]:
    """Fetch set of already archived dates for a specific table."""
    window_start = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")
    query = "SELECT partition_date FROM archives WHERE registry_id = %s AND partition_date >= %s"
    try:
        with pg_conn.cursor() as cur:
            cur.execute(query, (registry_id, window_start))
            return {row[0] for row in cur.fetchall()}
    except Exception as e:
        logger.error(f"Table registry id {registry_id} error - {e}")
        return set()


def get_questdb_partitions(qdb_conn, table_name: str) -> Set[str]:
    """Query QuestDB to find existing completed daily partitions."""
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    query = f"SELECT distinct timestamp::DATE FROM {table_name} WHERE timestamp < '{today_str}'::DATE"
    try:
        with qdb_conn.cursor() as cur:
            cur.execute(query)
            return {
                row[0].strftime("%Y-%m-%d")
                if hasattr(row[0], "strftime")
                else str(row[0])
                for row in cur.fetchall()
            }
    except Exception as e:
        logger.warning(f"Table {table_name} query failed (might not exist yet): {e}")
        return set()


def execute_copy_command(qdb_conn, table_name: str, date_str: str) -> Tuple[str, int]:
    """
    Helper to run the actual COPY SQL and capture Row Count.
    Returns: (filename, row_count)
    """
    filename = f"{table_name}_{date_str}.parquet"
    export_path = f"/archive_staging/{filename}"

    count_query = f"SELECT count() FROM {table_name} WHERE timestamp IN '{date_str}'"
    row_count = 0
    with qdb_conn.cursor() as cur:
        cur.execute(count_query)
        row_count = cur.fetchone()[0]

    stmt = f"""
        COPY (SELECT * FROM {table_name} WHERE timestamp IN '{date_str}')
        TO '{export_path}'
        WITH FORMAT PARQUET COMPRESSION_CODEC ZSTD;
    """
    logger.info(f"Exporting {table_name} for {date_str} (Rows: {row_count})...")

    with qdb_conn.cursor() as cur:
        cur.execute(stmt)

    return filename, row_count


def archive_partition(
    qdb_conn, date_str: str, table_name: str
) -> Tuple[bool, List[Tuple[str, int]]]:
    """
    Triggers QuestDB COPY command.
    Returns: Success_Bool, List of (Filename, RowCount) tuples.
    """
    generated_data = []  

    try:
        main_file, main_count = execute_copy_command(qdb_conn, table_name, date_str)
        generated_data.append((main_file, main_count))

        if table_name.endswith("_tbt"):
            snap_table_name = table_name.replace("_tbt", "_price")
            logger.info(f"Detected TBT table. Archiving Snapshot: {snap_table_name}")

            try:
                snap_file, snap_count = execute_copy_command(
                    qdb_conn, snap_table_name, date_str
                )
                generated_data.append((snap_file, snap_count))
            except Exception as e:
                logger.error(
                    f"Failed to archive associated snapshot table {snap_table_name}: {e}"
                )

        return True, generated_data

    except Exception as e:
        if "exists" in str(e):
            logger.warning(f"Export for {table_name} likely already exists. Skipping.")
            return True, generated_data

        logger.error(f"Critical Archive Failure for {table_name}: {e}")
        return False, []


def mark_as_archived(
    pg_conn, date_str: str, file_data: List[Tuple[str, int]], registry_id: int
):
    """
    Update Postgres Metadata with Row Count.
    file_data is a list of (filename, row_count) tuples.
    """
    query = """
        INSERT INTO archives (registry_id, partition_date, file_path, row_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    with pg_conn.cursor() as cur:
        for fname, count in file_data:
            cur.execute(query, (registry_id, date_str, fname, count))


def run_smart_backfill():
    logger.info("Starting Bronze tables Archival Check")

    try:
        with (
            connect(postgres_db_cfg.url, autocommit=True) as pg_conn,
            connect(quest_db_cfg.pwp, autocommit=True) as qdb_conn,
        ):
            bronze_tables = get_bronze_tables(pg_conn)

            for table_info in bronze_tables:
                r_id = table_info["registry_id"]
                t_name = table_info["table_name"]

                available_dates = get_questdb_partitions(qdb_conn, t_name)
                archived_dates = get_archived_dates(pg_conn, r_id)
                missing_dates = sorted(list(available_dates - archived_dates))

                for date_str in missing_dates:
                    success, file_data = archive_partition(qdb_conn, date_str, t_name)

                    if success and file_data:
                        mark_as_archived(pg_conn, date_str, file_data, r_id)

                        log_msg = ", ".join([f"{f} ({c} rows)" for f, c in file_data])
                        logger.info(f"Archived {t_name} [{date_str}]: {log_msg}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    hour = 0
    minute = 5

    run_smart_backfill()

    scheduler.add_job(
        run_smart_backfill,
        trigger=CronTrigger(hour=hour, minute=minute, timezone="UTC"),
        id="archiver_service",
    )

    logger.info(f"Scheduler active. Next run at {hour}:{minute:02d} UTC.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopping archiver...")
