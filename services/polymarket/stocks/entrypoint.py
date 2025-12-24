from datetime import datetime, timezone
from typing import List
from common.logger import logger, setup_logger
from asyncio import run
from time import time
from shared.ingestion import BasePolymarketCollector

setup_logger("live-crypto-daily-collector")

class LiveStocksDailyCollector(BasePolymarketCollector):
    def __init__(self, base_filename: str, topics: List[str], rotation_interval: int, data_dir: str) -> None:
        super().__init__(
            base_filename, 
            topics, 
            rotation_interval, 
            data_dir
        )
    
    def get_floored_epoch(self,offset=0):
        now = int(time())
        return (now - (now % self.rotation_interval)) + (
            offset * self.rotation_interval
        )
    
    def build_slug(self, slug_base: str, epoch):
        dt_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
        # et_tz = pytz_tz("US/Eastern")
        # dt_et = dt_utc.astimezone(et_tz)
        
        formatted_time = dt_utc.strftime("%B-%d-%Y").lower()
        
        return f"{slug_base}-{formatted_time}"

async def main():
    topics = [
        "nik-up-or-down-on",    # Nikkei
        "spx-up-or-down-on",    # SPX500
        "cl-up-or-down-on",     # Crude Oil
        "gc-up-or-down-on",     # Gold
        "nflx-up-or-down-on",   # Netflix
        "ukx-up-or-down-on",    # Ukx
        "hsi-up-or-down-on",    # Hong kong index
        "si-up-or-down-on",     # Silver
        "rut-up-or-down-on",    # Russel
        "dax-up-or-down-on",    # German index
        "dji-up-or-down-on",    # Dowjone index
        "tsla-up-or-down-on",   # Tesla 
        "pltr-up-or-down-on",   # Palantir 
        "amzn-up-or-down-on",   # Amazon
        "nvda-up-or-down-on",   # Nvidia
        "msft-up-or-down-on",   # Microsoft
        "ndx-up-or-down-on",    # Nasdaq
        "aapl-up-or-down-on",   # Apple
        "meta-up-or-down-on",   # Meta
        "googl-up-or-down-on",  # Google
        "open-up-or-down-on",   #  
        "nya-up-or-down-on"     # NYSE
    ]
    live = LiveStocksDailyCollector(
        base_filename="polymarket-stocks-data",
        topics=topics,
        rotation_interval=86400, # Daily interval
        data_dir="data/stocks"
    )
    
    logger.info("Booting polymarket Daily stocks live crypto price collector")
    await live.run()

if __name__ == "__main__":
    try:
        run(main())
    except KeyboardInterrupt:
        logger.info("Polymarket 15m live crypto price collector shutdown")