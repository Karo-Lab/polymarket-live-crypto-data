import argparse
from psycopg import connect, OperationalError
from os import listdir, path
from sys import exit
from common.config import quest_db_cfg, postgres_db_cfg 
from common.logger import logger, setup_logger

setup_logger("db_migration")

def run_db_migration(db_name: str, db_conn_string: str, direction: str):
    """
    Executes migration for a specific database.
    structure: migrations/{db_name}/{direction}
    """
    migrations_path = path.join("migrations", db_name, direction)
    
    if not path.exists(migrations_path):
        logger.warning(f"[{db_name}] Directory {migrations_path} does not exist. Skipping.")
        return

    files = [f for f in listdir(migrations_path) if f.endswith(".sql")]
    
    sql_scripts = sorted(files)

    if direction == "down":
        sql_scripts.reverse()
    
    if not sql_scripts:
        logger.info(f"[{db_name}] No .sql files found in {migrations_path}.")
        return

    logger.info(f"[{db_name}] Starting {direction} migration ({len(sql_scripts)} files)...")

    try: 
        with connect(db_conn_string, autocommit=True) as conn:
            with conn.cursor() as cursor:
                for script_name in sql_scripts:
                    logger.info(f"[{db_name}] Executing: {script_name}")
                    
                    full_path = path.join(migrations_path, script_name)
                    with open(full_path, "r") as f:
                        cursor.execute(f.read())
                
                logger.info(f"[{db_name}] {direction} migrations success.")
    except OperationalError as e:
        logger.error(f"[{db_name}] {direction} sql error at {script_name} - {e}")
    except Exception as e:
        logger.error(f"[{db_name}] {direction} migrations failed at {script_name} - {e}")
        exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database Migration Tool")
    parser.add_argument("--direction", choices=["up", "down"], default="up", help="Migration direction")
    parser.add_argument("--target", choices=["all", "questdb", "postgres"], default="all", help="Target specific DB")

    args = parser.parse_args()

    databases = {
        "questdb": quest_db_cfg.pwp,
        "postgres": postgres_db_cfg.url
    }

    targets = [args.target] if args.target != "all" else ["questdb", "postgres"]

    for target_db in targets:
        if target_db in databases:
            run_db_migration(target_db, databases[target_db], args.direction)
        else:
            logger.error(f"Config for {target_db} not found.")