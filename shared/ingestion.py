import time
from abc import ABC, abstractmethod
from asyncio import (
    FIRST_COMPLETED,
    Queue,
    TimeoutError,
    create_task,
    get_event_loop,
    sleep,
    wait,
    wait_for,
)
from csv import writer
from datetime import datetime, timezone
from json import dumps, loads
from os import makedirs, path
from typing import Any, Dict, List

from aiohttp import ClientSession
from pandas import DataFrame
from questdb.ingress import Sender
from typing_extensions import deprecated
from websockets import ClientConnection, ConnectionClosed, connect

from common.config import config, quest_db_cfg
from common.logger import logger
from shared.utils import to_numpy_book


class BasePolymarketCollector(ABC):
    def __init__(
        self, base_filename: str, topics: List[str], rotation_interval: int, data_dir
    ) -> None:
        self.base_filename = base_filename
        self.topics = topics
        self.rotation_interval = rotation_interval
        self.buffer = Queue(maxsize=50000)
        self.ingest_snapshot_queue = Queue(maxsize=50000)
        self.ingest_tbt_queue = Queue(maxsize=50000)
        self.data_dir = data_dir

        makedirs(self.data_dir, exist_ok=True)
        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"{self.base_filename}_{current_date}.csv"
        self.csv_file_path = path.join(self.data_dir, filename)

    @abstractmethod
    def get_floored_epoch(self, offset=0) -> Any:
        """Calculates the start of a 15m window. Offset=1 gets the next window."""
        pass

    async def ingest_quest_db(
        self,
        queue: Queue,
        schema: List[str],
        event: str,
        table_name: str,
        batch_size=1000,
        flush_interval=0.5,
    ):
        batch_rows = []
        last_flush = get_event_loop().time()

        with Sender.from_conf(quest_db_cfg.url) as sender:
            try:
                while True:
                    try:
                        row = await wait_for(
                            queue.get(), timeout=0.1
                        )

                        if row is None:  # Shutdown signal
                            break
                        if row["event"] != event:
                            continue

                        batch_rows.append(row["data"])
                        queue.task_done()

                    except TimeoutError:
                        pass

                    current_time = get_event_loop().time()

                    if batch_rows and (
                        len(batch_rows) >= batch_size
                        or (current_time - last_flush) >= flush_interval
                    ):                        
                        if batch_rows:
                            df_to_send = DataFrame(batch_rows, columns=schema)
                            sender.dataframe(
                                df_to_send, table_name=table_name, at="timestamp"
                            )
                            sender.flush()

                            batch_rows = []
                            last_flush = current_time
            except Exception as e:
                logger.error(f"QuestDB Ingestion Error: {e}")
                raise
            finally:
                if batch_rows:
                    df_to_send = DataFrame(batch_rows, columns=schema)
                    sender.dataframe(df_to_send, table_name=table_name, at="timestamp")
                    sender.flush()

    @deprecated("Use questdb ingestion instead")
    async def csv_writer_task(self, batch_size=1000, flush_interval=0.5):
        """
        Batch write to csv, only write to csv if batch size reach or flush interval happen
        """
        logger.info("Writer started with daily rotation.")
        logger.info(f"Writer started for {self.base_filename}")
        with open(self.csv_file_path, "a", newline="") as f:
            writer_ = writer(f)

            if f.tell() == 0:
                writer_.writerow(
                    [
                        "local_ts",
                        "market_ts",
                        "event",
                        "asset_id",
                        "outcome",
                        "best_bid",
                        "bid_size",
                        "best_ask",
                        "ask_size",
                    ]
                )

            batch = []
            last_flush = get_event_loop().time()

            try:
                while True:
                    try:
                        row = await wait_for(self.buffer.get(), timeout=0.1)

                        if row is None:
                            if batch:
                                writer_.writerows(batch)
                            break

                        batch.append(row)
                        self.buffer.task_done()
                    except TimeoutError:
                        pass

                    current_time = get_event_loop().time()

                    if (
                        len(batch) >= batch_size
                        or (current_time - last_flush) >= flush_interval
                    ):
                        logger.info(
                            f"BATCH {len(batch)} - Flush interval {current_time - last_flush}"
                        )
                        if batch:
                            writer_.writerows(batch)
                            f.flush()
                            batch = []
                            last_flush = current_time
            finally:
                if batch:
                    writer_.writerows(batch)
                f.close()

    async def market_data_worker(
        self, epoch, token_map: Dict[Any, Any], token_ids: List[str]
    ):
        """Handles the WebSocket connection for a specific epoch window."""
        if not token_ids:
            return

        # Expire after the next interval start + 15s (buffer time for it to)
        expiry = epoch + self.rotation_interval + 15

        logger.info(
            f"[{datetime.now().strftime('%H:%M:%S')}] Starting stream for epoch {epoch}"
        )

        ws_url = f"{config.WS_URL}/ws/market"

        while time.time() < expiry:
            try:
                async with connect(ws_url) as ws:
                    subscribe_msg = {
                        "type": "market",
                        "initial_dump": False,
                        "assets_ids": token_ids,
                    }
                    await ws.send(dumps(subscribe_msg))
                    ping_task = create_task(self.send_heartbeat(ws))

                    try:
                        while time.time() < expiry:
                            message = await wait_for(ws.recv(), timeout=15.0)
                            if not message:
                                logger.warning("Empty message received, skipping...")
                                continue

                            if message == "PONG":
                                continue

                            data = loads(message)
                            match data.get("event_type"):
                                case "book":
                                    tid = data.get("asset_id")
                                    meta = token_map.get(tid)
                                    if not meta:
                                        continue

                                    bids = data.get("bids", [])
                                    asks = data.get("asks", [])


                                    # Data processing
                                    bids_matrix = to_numpy_book(bids)
                                    asks_matrix = to_numpy_book(asks)

                                    # Timestamp handling
                                    raw_ts = int(data.get("timestamp", 0))
                                    if raw_ts > 1e15:
                                        ts_dt = datetime.fromtimestamp(
                                            raw_ts / 1_000_000, tz=timezone.utc
                                        )
                                    elif raw_ts > 1e12:
                                        ts_dt = datetime.fromtimestamp(
                                            raw_ts / 1_000, tz=timezone.utc
                                        )
                                    else:
                                        ts_dt = datetime.fromtimestamp(
                                            raw_ts, tz=timezone.utc
                                        )

                                    quest_db_row = [
                                        ts_dt,
                                        datetime.now(timezone.utc),
                                        meta["topic"],
                                        tid,
                                        meta["side"],
                                        bids_matrix,
                                        asks_matrix,
                                    ]

                                    payload = {"event": "book", "data": quest_db_row}
                                    await self.ingest_snapshot_queue.put(payload)

                                case "price_change":
                                    raw_ts = int(data.get("timestamp", 0))
                                    if raw_ts > 1e15:
                                        ts_dt = datetime.fromtimestamp(
                                            raw_ts / 1_000_000, tz=timezone.utc
                                        )
                                    elif raw_ts > 1e12:
                                        ts_dt = datetime.fromtimestamp(
                                            raw_ts / 1_000, tz=timezone.utc
                                        )
                                    else:
                                        ts_dt = datetime.fromtimestamp(
                                            raw_ts, tz=timezone.utc
                                        )

                                    receipt_dt = datetime.now(timezone.utc)

                                    changes = data.get("price_changes", [])

                                    for change in changes:
                                        tid = change.get("asset_id")
                                        meta = token_map.get(tid)

                                        if not meta:
                                            continue

                                        quest_db_row = [
                                            ts_dt,
                                            receipt_dt,
                                            meta.get("topic", ""),
                                            tid,
                                            meta.get("side", ""),
                                            change.get("side"),
                                            float(change.get("price", 0)),
                                            float(change.get("size", 0)),
                                            float(change.get("best_bid", 0)),
                                            float(change.get("best_ask", 0)),
                                        ]

                                        payload = {
                                            "event": "price_change",
                                            "data": quest_db_row,
                                        }
                                        await self.ingest_tbt_queue.put(payload)
                                case _:
                                    continue

                    finally:
                        ping_task.cancel()

            except (ConnectionClosed, TimeoutError) as e:
                logger.warning(
                    f"Worker {epoch} connection lost: {e}. Reconnecting in 2s"
                )
                await sleep(2)
            except Exception as e:
                logger.error(f"Worker Error for {epoch}: {e}")

    async def send_heartbeat(self, ws: ClientConnection):
        """Sends 'PING' string every 10 seconds as required by Polymarket."""
        try:
            while True:
                await ws.send("PING")
                await sleep(10)
        except Exception:
            pass

    @abstractmethod
    def build_slug(self, slug_base: str, epoch) -> str:
        pass

    async def fetch_mapped_tokens(self, session: ClientSession, epoch):
        """
        Constructs the specific slug for the 15m interval and fetches IDs.
        Note: You may need to format the slug to match Polymarket's naming convention
        (e.g., 'btc-updown-15m-174065..')
        """

        token_map = {}
        token_clob_ids = []

        for slug_base in self.topics:
            slug = self.build_slug(slug_base, epoch)

            url = f"{config.GAMMA_ENDPOINT}/events/slug/{slug}"
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        logger.info(f"Fetched from {url}")
                        data = await resp.json()

                        if not data or "markets" not in data:
                            continue

                        market = data["markets"][0]

                        outcomes = loads(market.get("outcomes", '["Up","Down"]'))

                        ids = loads(market.get("clobTokenIds", "[]"))

                        if len(ids) >= 2:
                            token_map[ids[0]] = {"topic": slug, "side": outcomes[0]}
                            token_map[ids[1]] = {
                                "topic": slug,
                                "side": outcomes[1],
                            }

                            token_clob_ids.extend(ids)
                    else:
                        logger.error(f"Error fetching from {url}")

            except Exception as e:
                logger.error(f"Error fetching tokens for {slug}: {e}")

        return token_map, token_clob_ids

    async def manager(self):
        """Orchestrates Hot and Cold tasks."""
        async with ClientSession() as session:
            active_tasks = {}

            while True:
                now = time.time()
                current_epoch = self.get_floored_epoch(offset=0)
                next_epoch = self.get_floored_epoch(offset=1)
                if current_epoch not in active_tasks:
                    token_map, flat_ids = await self.fetch_mapped_tokens(
                        session, current_epoch
                    )
                    if flat_ids:
                        active_tasks[current_epoch] = create_task(
                            self.market_data_worker(current_epoch, token_map, flat_ids)
                        )

                # Pre-warm Cold task (Start before the current one ends)
                # This will fetch the next token ids and sub to it before the event start
                if (
                    now > (next_epoch - config.PRE_WARM_TIME)
                    and next_epoch not in active_tasks
                ):
                    logger.info(f"Pre-warming next window: {next_epoch}")
                    token_map, flat_ids = await self.fetch_mapped_tokens(
                        session, next_epoch
                    )
                    if flat_ids:
                        active_tasks[next_epoch] = create_task(
                            self.market_data_worker(next_epoch, token_map, flat_ids)
                        )

                # Cleanup completed tasks
                done_epochs = [e for e, t in active_tasks.items() if t.done()]
                for e in done_epochs:
                    del active_tasks[e]

                await sleep(10)

    async def run(self):
        """
        Main entrypoint
        """
        poly_l2_snapshot_columns = [
            "timestamp",
            "receipt_time",
            "slug",
            "token_id",
            "token_name",
            "bids",
            "asks",
        ]

        poly_tbt_changes_columns = [
            "timestamp",
            "receipt_time",
            "slug",
            "token_id",
            "token_name",
            "side",
            "price",
            "size",
            "best_bid",
            "best_ask",
        ]

        while True:
            try:
                snapshot_writer_task = create_task(
                    self.ingest_quest_db(
                        queue=self.ingest_snapshot_queue,
                        schema=poly_l2_snapshot_columns,
                        table_name="poly_live_price",
                        event="book",
                    )
                )
                tbt_writer_task = create_task(
                    self.ingest_quest_db(
                        queue=self.ingest_tbt_queue,
                        schema=poly_tbt_changes_columns,
                        table_name="poly_live_tbt",
                        event="price_change",
                    )
                )
                orchestrator_task = create_task(self.manager())

                _done, pending = await wait(
                    [snapshot_writer_task, tbt_writer_task, orchestrator_task],
                    return_when=FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()

                logger.error("Unexpected failure, restarting in 5s")
                await sleep(5)
            except Exception as e:
                logger.error(f"Unexpected failure caught {e}, restarting in 5s")
                await sleep(5)
