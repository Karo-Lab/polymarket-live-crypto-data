from typing import List
from common.logger import logger, setup_logger
from asyncio import run
from time import time
from shared.core import BasePolymarketCollector

setup_logger("live-crypto-4h-collector")

class LiveCrypto4hCollector(BasePolymarketCollector):
    def __init__(self, base_filename: str, topics: List[str], rotation_interval: int, data_dir: str) -> None:
        super().__init__(
            base_filename, 
            topics, 
            rotation_interval, 
            data_dir
        )
    
    def get_floored_epoch(self,offset=0):
        utc_offset = 7 * 3600  
        now = int(time())
        local_now = now + utc_offset
        floored_local = (local_now - (local_now % self.rotation_interval))
        return (floored_local - utc_offset) + (offset * self.rotation_interval)
        
    def build_slug(self, slug_base: str, epoch):
        return f"{slug_base}-{epoch}"

async def main():
    topics = [
        "btc-updown-4h",
        "eth-updown-4h",
        "sol-updown-4h",
        "xrp-updown-4h",
    ]
    live = LiveCrypto4hCollector(
        base_filename="polymarket-crypto-data",
        topics=topics,
        rotation_interval=14400, #4h interval
        data_dir="data/4hm"
    )
    
    logger.info("Booting polymarket 15m live crypto price collector")
    await live.run()

if __name__ == "__main__":
    try:
        run(main())
    except KeyboardInterrupt:
        logger.info("Polymarket 15m live crypto price collector shutdown")