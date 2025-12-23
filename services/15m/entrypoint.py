from typing import List
from common.logger import logger
from asyncio import run
from time import time
from shared.core import BasePolymarketCollector

class LiveCrypto15mCollector(BasePolymarketCollector):
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
        return f"{slug_base}-{epoch}"

async def main():
    topics = [
        "btc-updown-15m",
        "eth-updown-15m",
        "sol-updown-15m",
        "xrp-updown-15m",
    ]
    live = LiveCrypto15mCollector(
        base_filename="polymarket-crypto-data",
        topics=topics,
        rotation_interval=900,
        data_dir="data/15m"
    )
    
    logger.info("Booting polymarket 15m live crypto price collector")
    await live.run()

if __name__ == "__main__":
    try:
        run(main())
    except KeyboardInterrupt:
        logger.info("Polymarket 15m live crypto price collector shutdown")