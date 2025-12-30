from datetime import datetime, timedelta
from typing import List, Set
from psycopg import OperationalError, connect
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from common.config import postgres_db_cfg, quest_db_cfg
from common.logger import setup_logger, logger

setup_logger("questdb_archiver")


def get_bronze_tables(pg_conn) -> List[dict]:
    """Fetch all BRONZE table metadata from the registry."""
    query = "SELECT registry_id,table_name, exchange FROM table_registry WHERE data_layer = 'BRONZE';"
    with pg_conn.cursor() as cur:
        cur.execute(query)
        return [{"registry_id": row[0],"table_name": row[1], "exchange": row[2]} for row in cur.fetchall()]

def get_archived_dates(pg_conn, registry_id: int) -> Set[str]:
    """Fetch set of already archived dates for a specific table. Fetched only the last 14 days since bronze table only have 7 days ttl data"""
    window_start = (datetime.utcnow() - timedelta(days=14)).strftime('%Y-%m-%d')
    logger.info(f"Look back at {window_start}")
    query = "SELECT partition_date FROM archives WHERE registry_id = %s AND partition_date >= %s"
    try:
        with pg_conn.cursor() as cur:
            cur.execute(query, (registry_id,window_start))
            return {row[0] for row in cur.fetchall()}
    except Exception as e:
        logger.error(f"Table registry id {registry_id} error - {e}")
        return set()

def get_questdb_partitions(qdb_conn, table_name: str) -> Set[str]:
    """Query QuestDB to find existing completed daily partitions."""
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    query = f"SELECT distinct timestamp::DATE FROM {table_name} WHERE timestamp < '{today_str}'::DATE"
    try:
        with qdb_conn.cursor() as cur:
            cur.execute(query)
            return {row[0].strftime('%Y-%m-%d') for row in cur.fetchall()}
    except Exception as e:
        logger.error(f"Table {table_name} might not exist in QuestDB yet: {e}")
        return set()

def archive_partition(qdb_conn, date_str: str, table_name: str):
    """Trigger QuestDB COPY TO command using absolute paths."""
    filename = f"{table_name}_{date_str}.parquet"
    
    stmt = f"""
        COPY (SELECT * FROM {table_name} WHERE timestamp IN '{date_str}')
        TO '{filename}'
        WITH FORMAT PARQUET COMPRESSION_CODEC ZSTD;
    """
    try:
        with qdb_conn.cursor() as cur:
            cur.execute(stmt)
        return True, filename
    except Exception as e:
        if "exists" in str(e):
            logger.warning(f"File {filename} already exists in storage. Treating as success.")
            return True, filename
        logger.error(f"Failed to copy {table_name} for {date_str}: {e}")
        return False, None

def mark_as_archived(pg_conn, date_str: str, filename: str, registry_id: int):
    """Update the Postgres Metadata Catalog."""
    query = """
        INSERT INTO archives (registry_id, partition_date, file_path)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    with pg_conn.cursor() as cur:
        cur.execute(query, (registry_id, date_str, filename))

def run_smart_backfill():
    logger.info("Starting Bronze tables Archival Check")
    
    try:
        with connect(postgres_db_cfg.url, autocommit=True) as pg_conn, \
             connect(quest_db_cfg.pwp, autocommit=True) as qdb_conn:
            
            bronze_tables = get_bronze_tables(pg_conn)
            
            if not bronze_tables:
                logger.warning("No BRONZE tables found in registry.")
                return

            for table_info in bronze_tables:
                r_id = table_info['registry_id']
                t_name = table_info['table_name']
                exch = table_info['exchange']
                
                logger.info(f"Processing table: {t_name} [{exch}]")
                
                # Get all availiable timestamp in YYYY-MM-DD format in questDB of bronze source
                available_dates = get_questdb_partitions(qdb_conn, t_name)
                
                # Get all availiable timestamp in YYYY-MM-DD format in postgres of bronze source
                archived_dates = get_archived_dates(pg_conn, r_id)
                
                # Check if any missing date that had not been archived
                missing_dates = sorted(list(available_dates - archived_dates))
                
                if not missing_dates:
                    logger.info(f"Table {t_name} is up to date.")
                    continue

                for date_str in missing_dates:
                    success, filename = archive_partition(qdb_conn, date_str, t_name)
                    if success:
                        mark_as_archived(pg_conn, date_str, filename, r_id)
                        logger.info(f"Successfully archived {t_name} for {date_str}")

    except OperationalError as e:
        logger.error(f"Connection Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in backfill: {e}")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    hour = 0
    minutes = 5
    scheduler.add_job(
        run_smart_backfill, 
        trigger=CronTrigger(hour=hour, minute=minutes, timezone='UTC'),
        id='archiver_service'
    )
    
    # Run once at startup
    run_smart_backfill()

    logger.info(f"Scheduler active. Waiting for {hour}:{minutes} UTC...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopping archiver...")