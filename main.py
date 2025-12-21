import asyncio
import csv
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp
import websockets
import os
from config import config
from logger import logger


DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True) 
CSV_FILE_BASE = os.path.join(DATA_DIR, "polymarket-crypto-data-15m")
DEFAULT_TOPICS = [
    "btc-updown-15m",
    "eth-updown-15m",
    "sol-updown-15m",
    "xrp-updown-15m",
]

def get_floored_epoch(offset=0):
    """Calculates the start of a 15m window. Offset=1 gets the next window."""
    now = int(time.time())
    return (now - (now % config.ROTATION_INTERVAL)) + (
        offset * config.ROTATION_INTERVAL
    )

async def csv_writer_task(buffer: asyncio.Queue,base_filename: str, batch_size=500, flush_interval=1.0):
    logger.info("Writer started with daily rotation.")
        
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = f"{base_filename}_{current_date}.csv"
    logger.info(f"Writer started for {filename}")
    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)
        
        if f.tell() == 0:
            writer.writerow(["local_ts", "market_ts", "event", "asset_id", "outcome", "best_bid", "bid_size", "best_ask", "ask_size"])
            
        batch = []
        last_flush = asyncio.get_event_loop().time()
        
        try:
            while True:
                try:
                    row = await asyncio.wait_for(buffer.get(), timeout=0.1)
    
                    if row is None: 
                        if batch: 
                            writer.writerows(batch)
                        break
                    
                    batch.append(row)
                    buffer.task_done()
                except asyncio.TimeoutError:
                    pass
                
                current_time = asyncio.get_event_loop().time()
                if len(batch) >= batch_size or (current_time - last_flush) >= flush_interval:
                    if batch:
                        writer.writerows(batch)
                        f.flush()
                        batch = []
                        last_flush = current_time
        finally:
            if batch:
                writer.writerows(batch)
            f.close()
            
            

async def fetch_mapped_tokens(session: aiohttp.ClientSession, epoch: int):
    """
    Constructs the specific slug for the 15m interval and fetches IDs.
    Note: You may need to format the slug to match Polymarket's naming convention
    (e.g., 'btc-updown-15m-174065..')
    """

    token_map = {}
    token_clob_ids = []

    for slug_base in DEFAULT_TOPICS:
        slug = f"{slug_base}-{epoch}"

        url = f"{config.GAMMA_ENDPOINT}/events/slug/{slug}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()

                    if not data or "markets" not in data:
                        continue

                    market = data["markets"][0]

                    outcomes = json.loads(market.get("outcomes", '["Up","Down"]'))

                    ids = json.loads(market.get("clobTokenIds", "[]"))

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


async def market_data_worker(
    epoch: int, token_map: Dict[Any, Any], token_ids: List[str], buffer: asyncio.Queue
):
    """Handles the WebSocket connection for a specific epoch window."""
    if not token_ids:
        return

    expiry = epoch + config.ROTATION_INTERVAL + 15

    logger.info(
        f"[{datetime.now().strftime('%H:%M:%S')}] Starting stream for epoch {epoch}"
    )

    ws_url = f"{config.WS_URL}/ws/market"
    
    while time.time() < expiry:
        try:
            async with websockets.connect(ws_url) as ws:
                subscribe_msg = {
                    "type": "market",
                    "initial_dump": False,
                    "assets_ids": token_ids,
                }
                await ws.send(json.dumps(subscribe_msg))
                ping_task = asyncio.create_task(send_heartbeat(ws))

                try:
                    while time.time() < expiry:
                        message = await asyncio.wait_for(ws.recv(), timeout=15.0)
                        if not message:
                            logger.warning("Empty message received, skipping...")
                            continue
                        
                        if message == "PONG":
                            continue
                        
                        data = json.loads(message)

                        tid = data.get("asset_id")
                        meta = token_map.get(tid)

                        if not meta:
                            continue
                        # Polymarket return top of book bid-ask at the last index of the returned array
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])

                        best_bid = bids[-1]["price"] if bids else None
                        best_ask = asks[-1]["price"] if asks else None

                        bid_vol = bids[-1]["size"] if bids else None
                        ask_vol = asks[-1]["size"] if asks else None

                        row = [
                            datetime.now(timezone.utc).isoformat(),
                            data.get("timestamp"),
                            meta["topic"],
                            tid,
                            meta["side"],
                            best_bid,
                            bid_vol,
                            best_ask,
                            ask_vol,
                        ]
                        logger.info(f"Row info: {row}")
                        await buffer.put(row)
                finally:
                    ping_task.cancel()

        except (websockets.ConnectionClosed, asyncio.TimeoutError) as e:
            logger.warning(f"Worker {epoch} connection lost: {e}. Reconnecting in 2s")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Worker Error for {epoch}: {e}")


async def send_heartbeat(ws: websockets.ClientConnection):
    """Sends 'PING' string every 10 seconds as required by Polymarket."""
    try:
        while True:
            await ws.send("PING")
            await asyncio.sleep(10)
    except Exception:
        pass


async def manager(buffer: asyncio.Queue):
    """Orchestrates Hot and Cold tasks."""    
    async with aiohttp.ClientSession() as session:
        active_tasks = {}

        while True:
            now = time.time()
            current_epoch = get_floored_epoch(offset=0)
            next_epoch = get_floored_epoch(offset=1)

            if current_epoch not in active_tasks:
                token_map, flat_ids = await fetch_mapped_tokens(session, current_epoch)
                if flat_ids:
                    active_tasks[current_epoch] = asyncio.create_task(
                        market_data_worker(current_epoch, token_map, flat_ids, buffer)
                    )

            # Pre-warm Cold task (Start before the current one ends)
            # This will fetch the next token ids and sub to it before the event start
            if (
                now > (next_epoch - config.PRE_WARM_TIME)
                and next_epoch not in active_tasks
            ):
                logger.info(f"Pre-warming next window: {next_epoch}")
                token_map, flat_ids = await fetch_mapped_tokens(session, next_epoch)
                if flat_ids:
                    active_tasks[next_epoch] = asyncio.create_task(
                        market_data_worker(next_epoch, token_map, flat_ids, buffer)
                    )

            # Cleanup completed tasks
            done_epochs = [e for e, t in active_tasks.items() if t.done()]
            for e in done_epochs:
                del active_tasks[e]

            await asyncio.sleep(10)

async def main():
    """
        Main entrypoint
    """
    buffer = asyncio.Queue(maxsize=50000)

    while True:
        try:
            writer_task = asyncio.create_task(csv_writer_task(buffer,CSV_FILE_BASE))
            orchestrator_task = asyncio.create_task(manager(buffer=buffer))
            
            done, pending = await asyncio.wait(
                [writer_task, orchestrator_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            for task in pending:
                task.cancel()
            
            logger.error("Unexpected failure, restarting in 5s")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected failure caught {e}, restarting in 5s")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        logger.info("Booting polymarket 15m live crypto price collector")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("polymarket 15m live crypto price collector shutdown")
