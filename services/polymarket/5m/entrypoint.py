from asyncio import run
from time import time
from typing import List

from common.logger import logger, setup_logger
from shared.ingestion import BasePolymarketCollector

setup_logger("live-crypto-5m-collector")


class LiveCrypto5mCollector(BasePolymarketCollector):
    def __init__(
        self,
        base_filename: str,
        topics: List[str],
        rotation_interval: int,
        data_dir: str,
    ) -> None:
        super().__init__(
            base_filename,
            topics,
            rotation_interval,
            data_dir,
        )

    def get_floored_epoch(self, offset=0):
        now = int(time())
        return (now - (now % self.rotation_interval)) + (
            offset * self.rotation_interval
        )

    def build_slug(self, slug_base: str, epoch):
        return f"{slug_base}-{epoch}"


async def main():
    topics = [
        "btc-updown-5m",
    ]
    live = LiveCrypto5mCollector(
        base_filename="polymarket-crypto-data",
        topics=topics,
        rotation_interval=300,
        data_dir="data/5m",
    )

    logger.info("Booting polymarket 5m live crypto price collector")
    await live.run()


if __name__ == "__main__":
    try:
        run(main())
    except KeyboardInterrupt:
        logger.info("Polymarket 5m live crypto price collector shutdown")
