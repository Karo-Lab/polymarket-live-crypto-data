from loguru import logger
from pathlib import Path
import sys

# Get the directory where logger.py actually lives

logger.remove()

def setup_logger(service_name: str):
    BASE_DIR = Path(__file__).resolve().parent.parent
    
    LOG_DIR = BASE_DIR / "logs" / service_name
    try:
        LOG_DIR.mkdir(exist_ok=True, parents=True)
    except Exception as e:
        print(f"Warning: Could not create {LOG_DIR}, falling back to /tmp/logs. Error: {e}")
        LOG_DIR = Path("/tmp/logs")
        LOG_DIR.mkdir(exist_ok=True)


    logger.add(
        sys.stdout,
        level="INFO",
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>",
    )
    
    logger.add(
        LOG_DIR / "trading.log",
        rotation="10 MB",          # rotate every 10 MB
        retention="14 days",       # keep 14 days
        compression="zip",         # compress old logs
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{line} | {message}",
        backtrace=True,            # show full trace on errors
        diagnose=True,             # show variables in tracebacks
    )
    
    return logger

__all__ = ["logger", "setup_logger"]
