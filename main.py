"""
We subscribe to check when the latest block is updated, then we query it normally via the RPC & save it

Flow:
- Async save to JSON files (each are unique so its good)
- Then have every X blocks maybe, call an SQL method sync which takes all the saved JSON and loads it insert
- There is a LOT of disk IO with this approach. I just do not feel like making SQLite async right now
"""
# TODO: save Tx types to a table over time

import asyncio
import json
import multiprocessing
import os
import time
import uuid

import httpx

from SQL import Database
from util import (  # decode_txs,; decode_txs_async,; get_sender,; remove_useless_data,
    get_block_txs,
    get_latest_chain_height,
)

CPU_COUNT = multiprocessing.cpu_count()

GROUPING = 50  # 50-100 is good.

# TODO: Save Txs & events to postgres?
# maybe a redis cache as well so other people can subscribe to redis for events?

# https://docs.tendermint.com/v0.34/rpc/
RPC_IP = "15.204.143.232:26657"  # if this is blank, we update every 6 seconds
RPC_URL = f"ws://{RPC_IP}/websocket"
RPC_ARCHIVE = "https://rpc-archive.junonetwork.io:443"

WALLET_PREFIX = "juno1"
VALOPER_PREFIX = "junovaloper1"
WALLET_LENGTH = 43

# junod works as well, this is just a lightweight decoder of it.
COSMOS_BINARY_FILE = "juno-decode"  # https://github.com/Reecepbcups/juno-decoder
try:
    res = os.popen(f"{COSMOS_BINARY_FILE}").read()
    # print(res)
except Exception as e:
    print(f"Please install {COSMOS_BINARY_FILE} to your path or ~/go/bin/")
    exit(1)

current_dir = os.path.dirname(os.path.realpath(__file__))

ignore = [
    "ibc.core.client.v1.MsgUpdateClient",
    "ibc.core.channel.v1.MsgAcknowledgement",
]

# We will index for each user after we index everything entirely using get_sender & iterating all Txs.
# Then we can add this as a default in the future. See notes in SQL.py

errors = os.path.join(current_dir, "errors")
os.makedirs(errors, exist_ok=True)


def stage_block_return_values_format(height: int, block_data: dict):
    decoded_txs = block_data["result"]["block"]["data"]["txs"]

    msg_types: dict[str, int] = {}
    tx: dict
    txs: dict[int, dict] = {}
    for tx in decoded_txs:
        messages = tx.get("body", {}).get("messages", [])

        for msg in messages:
            msg_type = msg.get("@type")
            if msg_type in msg_types.keys():
                msg_types[msg_type] += 1
            else:
                msg_types[msg_type] = 1

        # if ignore is in the string of the tx, continue
        # Since we only ignore IBC msgs we can just skip the Tx for now.
        if any(x in str(tx) for x in ignore):
            continue

        unique_id = uuid.uuid4().int
        txs[unique_id] = tx

    # DEBUGGING
    if height % 10 == 0:
        print(f"Block {height}: {len(txs.keys())} txs")

    # We return the values we want to save in SQL per block
    return {
        "height": height,
        "msg_types": msg_types,
        "txs": txs,  # unique_id: json
    }


def run_decode_single_async(tx: str) -> dict:
    # TODO: replace this with proto in the future?
    # We run in a pool for better performance
    # check for max len tx (store code breaks this for CLI usage on linux)
    if len(tx) > 32766:
        # Store codes
        # print("TX too long. Skipping...")
        return {}

    res = os.popen(f"{COSMOS_BINARY_FILE} tx decode {tx} --output json").read()
    return json.loads(res)


async def download_block(pool, height: int) -> dict | None:
    # Skip already downloaded height data

    # Note sure if this is a limiting factor or not as we add?
    # May not be possible with async
    if db.get_block_txs(height) != None:
        print(f"Block {height} is already downloaded & saved in SQL")
        return None

    # Query block with client
    # TODO: Save a pool of clients and pass through instead of generating a new one each time?
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{RPC_ARCHIVE}/block?height={height}", timeout=60)
        if r.status_code != 200:
            print(f"Error: {r.status_code} @ height {height}")
            with open(os.path.join(errors, f"{height}.json"), "w") as f:
                f.write(r.text)
            return None

    # Gets block transactions, decodes them to JSON, and saves them to the block_data
    block_data: dict = r.json()
    block_txs = (
        block_data.get("result", {}).get("block", {}).get("data", {}).get("txs", [])
    )

    decoded_txs: list[dict] = pool.map(run_decode_single_async, block_txs)
    block_data["result"]["block"]["data"]["txs"] = decoded_txs

    return stage_block_return_values_format(height, block_data)


def test_get_data():
    # tables = db.get_all_tables()
    # print(tables)
    # schema = db.get_table_schema("messages")
    # print(schema)

    latest = db.get_latest_saved_block_height()
    print(latest)
    txs_in_block = db.get_block_txs(latest)
    print("txs_in_block", txs_in_block)
    tx = db.get_tx(txs_in_block[-1])
    print(tx)
    pass


async def main():
    if False:
        test_get_data()
        exit(1)

    # while loop, every 6 seconds query the RPC for latest and download. Try catch
    while True:
        last_downloaded = db.get_latest_saved_block_height()
        current_chain_height = get_latest_chain_height(RPC_ARCHIVE=RPC_ARCHIVE)
        block_diff = current_chain_height - last_downloaded
        print(
            f"Latest live height: {current_chain_height:,}. Last downloaded: {last_downloaded:,}. Behind by: {block_diff:,}"
        )

        start = 7000000  # original 6_700_000
        if start <= last_downloaded:
            start = last_downloaded

        # ensure end is a multiple of grouping
        end = current_chain_height - (current_chain_height % GROUPING)

        difference = int(end) - start
        print(f"Download Spread: {difference:,} blocks")

        # Runs through groups for downloading from the RPC
        with multiprocessing.Pool(CPU_COUNT) as pool:
            for i in range((end - start) // GROUPING + 1):
                tasks = {}
                start_time = time.time()
                for j in range(GROUPING):
                    # block index from the grouping its in
                    block = start + i * GROUPING + j
                    tasks[block] = asyncio.create_task(download_block(pool, block))

                print(f"Waiting to do # of blocks: {len(tasks)}")

                # This should never happen, just a precaution
                try:
                    values = await asyncio.gather(*tasks.values())
                    save_values_to_sql(values)
                except Exception as e:
                    print(e)
                    print("Error in tasks")
                    continue

                end_time = time.time()
                print(
                    f"Finished #{len(tasks)} of tasks in {end_time - start_time} seconds"
                )

                # print(f"early return on purpose for testing"); exit(1)

        print("Finished")
        time.sleep(6)


def save_values_to_sql(values: list[dict]):
    # values are from save_block_data_to_json
    # Schema: {
    #     "height": height,
    #     "msg_types": msg_types, # dict[str, int] = {}
    #     "txs": txs,  # unique_id: json
    # }

    for value in values:
        if value == None:  # if we already downloaded or there was an error
            continue

        height = value["height"]
        msg_types = dict(value["msg_types"])
        block_txs = dict(value["txs"])

        # if any of the above are none, skip
        if height == None or msg_types == None or block_txs == None:
            # write to log
            with open("error.log", "a") as f:
                f.write(
                    f"Error in height: {height} for blocks and data in save_values_to_sql"
                )
            continue

        sql_tx_ids: list[int] = []
        for _, tx_json in block_txs.items():
            # print(_, tx_json); exit(1)
            # our database Unique ID of a Tx
            unique_id = db.insert_tx(tx_json)
            sql_tx_ids.append(unique_id)

        db.insert_block(height, sql_tx_ids)
        for msg_type, count in msg_types.items():
            db.insert_type_count(msg_type, count, height)

        # print(f"save_values_to_sql. Exited early")
        # exit(1)

    # Saves to it after we go through all group of blocks
    db.commit()
    pass


# from websocket import create_connection
if __name__ == "__main__":
    db = Database(os.path.join(current_dir, "data.db"))
    # # db.drop_all()
    db.create_tables()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    loop.close()
