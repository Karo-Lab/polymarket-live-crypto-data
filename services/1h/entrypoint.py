from datetime import datetime, timezone
from typing import List
from common.logger import logger
from asyncio import run
from time import time
from pytz import timezone as pytz_tz
from shared.core import BasePolymarketCollector

class LiveCrypto1hCollector(BasePolymarketCollector):
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
        et_tz = pytz_tz("US/Eastern")
        dt_et = dt_utc.astimezone(et_tz)
        
        formatted_time = dt_et.strftime("%B-%d-%I%p-et").lower().replace("-0", "-")
        
        return f"{slug_base}-{formatted_time}"

async def main():
    topics = [
        "bitcoin-up-or-down",
        "ethereum-up-or-down",
        "solana-up-or-down",
        "xrp-up-or-down",
    ]
    live = LiveCrypto1hCollector(
        base_filename="polymarket-crypto-data",
        topics=topics,
        rotation_interval=3600,
        data_dir="data/1h"
    )
    
    logger.info("Booting polymarket 15m live crypto price collector")
    await live.run()

if __name__ == "__main__":
    try:
        run(main())
    except KeyboardInterrupt:
        logger.info("Polymarket 15m live crypto price collector shutdown")