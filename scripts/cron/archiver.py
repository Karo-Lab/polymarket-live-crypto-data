from datetime import datetime, timedelta
from psycopg import OperationalError, connect

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from common.config import quest_db_cfg
from common.logger import setup_logger, logger

setup_logger("questdb_archiver")

def run_archival_job():
    """
    The actual logic that runs once per day.
    """
    # yesterday = datetime.now() - timedelta(days=1)
    # 
    yesterday = datetime.now()
    target_date_str = yesterday.strftime('%Y-%m-%d')
    
    full_path = f"okx_live_price_{target_date_str}.parquet"

    logger.info(f"Triggering export for: {target_date_str}")
    logger.info(f"Instructing QuestDB to write to: {full_path}")

    stmt = f"""
        COPY (SELECT * FROM okx_live_price WHERE timestamp IN '{target_date_str}')
        TO '{full_path}'
        WITH FORMAT PARQUET COMPRESSION_CODEC ZSTD;
    """

    conn = None
    try:
        conn = connect(quest_db_cfg.pwp, autocommit=True)
        with conn.cursor() as cursor:
            cursor.execute(stmt)
            logger.info(f"SUCCESS: Archived {target_date_str} to {full_path}")

    except OperationalError as e:
        if "exists" in str(e):
            logger.warning(f"SKIPPING: File {full_path} already exists. No action taken.")
        else:
            logger.error(f"DB ERROR: {e}")
            
    except Exception as e:
        logger.error(f"CRITICAL ERROR: {e}")
    finally:
        if conn:
            conn.close()

def main():
    logger.info("Initializing Archiver Service...")
    
    scheduler = BlockingScheduler()
    
    hour = 0
    minute = 5
    
    scheduler.add_job(
        run_archival_job, 
        trigger=CronTrigger(hour=hour, minute=minute, timezone='UTC'),
        id='daily_export',
        replace_existing=True
    )
    

    logger.info(f"Scheduler started. Waiting for next run at {hour}:{minute} UTC...")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    main()