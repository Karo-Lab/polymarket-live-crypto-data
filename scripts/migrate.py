from psycopg import connect
from os import listdir, path
from common.config import quest_db_cfg
from common.logger import logger, setup_logger
from sys import exit, argv
setup_logger("quest_db_init_schema")

def run_migration(direction="up"):
    migrations_path = path.join("migrations", direction)
    
    if not path.exists(migrations_path):
        logger.error(f"Migration directory {migrations_path} does not exist")
        return
    
    # Get all migrations scripts in order
    sql_scripts = sorted([f for f in listdir(migrations_path) if f.endswith(".sql")])
    
    try: 
        with connect(quest_db_cfg.pwp, autocommit=True) as conn:
            with conn.cursor() as cursor:
                for script_name in sql_scripts:
                    logger.info(f"Running {direction} migrations: {script_name}")
                    
                    with open(path.join(migrations_path, script_name), "r") as f:
                        cursor.execute(f.read())
                
                logger.info(f"{direction} migrations success.")
    except Exception as e:
        logger.error(f"{direction} migrations failed - {e}")
        exit(1)


if __name__ == "__main__":
    direction = argv[1] if len(argv) > 1 else "up"
    run_migration(direction)