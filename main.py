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
import random
import time
import uuid
from dataclasses import dataclass

import httpx

from SQL import Database
from util import (  # decode_txs,; decode_txs_async,; get_sender,; remove_useless_data,
    get_block_txs,
    get_latest_chain_height,
)

CPU_COUNT = multiprocessing.cpu_count()

GROUPING = 100  # 50-100 is good.

RPC_ARCHIVE_LINKS: list[str] = [
    "https://rpc-archive.junonetwork.io:443",
    # "https://rpc.juno.strange.love:443", # not archive, using for testing through
    # "https://juno-rpc.reece.sh:443", # not archive, using for testing through
    # "https://juno-rpc.polkachu.com:443", # not archive, using for testing through
]

WALLET_PREFIX = "juno1"
VALOPER_PREFIX = "junovaloper1"
WALLET_LENGTH = 43

# junod works as well, this is just a lightweight decoder of it.
COSMOS_BINARY_FILE = "juno-decode"  # https://github.com/Reecepbcups/juno-decoder

current_dir = os.path.dirname(os.path.realpath(__file__))

# We will index for each user after we index everything entirely using get_sender & iterating all Txs.
# Then we can add this as a default in the future. See notes in SQL.py

errors_dir = os.path.join(current_dir, "errors")


'''
# Later we can decode
decoded_txs: list[dict] = pool.map(run_decode_single_async, block_txs)    

def stage_block_return_values_format(height: int, decoded_txs: list[dict] = []):
    # decoded_txs = block_data["result"]["block"]["data"]["txs"]
    msg_types: dict[str, int] = {}
    tx: dict
    txs: dict[int, dict] = {}
    for tx in decoded_txs:        
        try:
            messages: list = tx["body"]["messages"]
        except:
            messages = []

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
'''

@dataclass
class BlockData:
    height: int
    block_time: str
    encoded_txs: list[str]


async def download_block(client: httpx.AsyncClient, pool, height: int) -> BlockData | None:
    # Skip already downloaded height data

    # Note sure if this is a limiting factor or not as we add?
    # May not be possible with async
    if db.get_block(height) != None:
        print(f"Block {height} is already downloaded & saved in SQL")
        return None

    # Query block with client       
    RPC_ARCHIVE_URL = random.choice(RPC_ARCHIVE_LINKS)
    r = await client.get(f"{RPC_ARCHIVE_URL}/block?height={height}", timeout=30)
    if r.status_code != 200:
        os.makedirs(errors_dir, exist_ok=True)

        print(f"Error: {r.status_code} @ height {height}")
        with open(os.path.join(errors_dir, f"{height}.json"), "w") as f:
            f.write(r.text)
        return None

    block_time = ""
    encoded_block_txs: list = []
    try:
        v = r.json()["result"]["block"]

        block_time = v["header"]["time"] # 2023-04-13T17:46:31.898429796Z
        encoded_block_txs = v["data"]["txs"] # ["amino1"]
    except KeyError:
        return None
    
    # Removes store_codes
    encoded_block_txs = [x for x in encoded_block_txs if len(x) < 32000]

    # return stage_block_return_values_format(height, decoded_txs=decoded_txs)
    return BlockData(height, block_time, encoded_block_txs)


async def main():
    if False:
        # tables = db.get_all_tables()
        # print(tables)
        # schema = db.get_table_schema("messages")
        # print(schema)

        latest = db.get_latest_saved_block_height()
        print(latest)

        block = db.get_block(latest-1)        
        print("txs_in_block", block.tx_ids)
        print("time", block.time)
        # tx = db.get_tx_amino(block.tx_ids[-1])
        # print(tx)

        pass
        exit(1)

    # while loop, every 6 seconds query the RPC for latest and download. Try catch
    while True:
        last_downloaded: int = db.get_latest_saved_block().height
        current_chain_height = get_latest_chain_height(RPC_ARCHIVE=RPC_ARCHIVE_LINKS[0])
        block_diff = current_chain_height - last_downloaded
        print(
            f"Latest live height: {current_chain_height:,}. Last downloaded: {last_downloaded:,}. Behind by: {block_diff:,}"
        )

        # Doing later so I can test against multiple PRs
        # start = 7_000_000
        start = 7_500_000

        if start <= last_downloaded:
            start = last_downloaded

        # ensure end is a multiple of grouping
        end = current_chain_height - (current_chain_height % GROUPING)

        difference = int(end) - start
        print(f"Download Spread: {difference:,} blocks")

        # Runs through groups for downloading from the RPC
        async with httpx.AsyncClient() as httpx_client:
            # TODO Move to concurrent.futures.ThreadPoolExecutor?
            with multiprocessing.Pool(CPU_COUNT*2) as pool:
                for i in range((end - start) // GROUPING + 1):
                    tasks = {}
                    start_time = time.time()
                    for j in range(GROUPING):
                        # block index from the grouping its in
                        block = start + i * GROUPING + j
                        tasks[block] = asyncio.create_task(download_block(httpx_client, pool, block))                    

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
                        f"Finished #{len(tasks)} blocks in {end_time - start_time} seconds @ {start + i * GROUPING}"
                    )

                    # print(f"early return on purpose for testing"); exit(1)

        print("Finished")
        time.sleep(6)        
        exit(1)


def save_values_to_sql(values: list[BlockData]):    
    for bd in values:
        if bd == None:  # if we already downloaded or there was an error
            continue

        height = bd.height
        block_time = bd.block_time
        amino_txs: list[str] = bd.encoded_txs
        
        sql_tx_ids: list[int] = []
        for amino_tx in amino_txs:
            # Amino encoded Tx string in the databse
            # We will update this in a future run after all blocks are indexed to decode
            
            # NOTE: Why is the amino JSON so much less effecient storage wise?
            # 0.015MB per block now compared to 0.0001 before using JSON decoding.
            unique_id = db.insert_tx(height, amino_tx)            
            sql_tx_ids.append(unique_id)


        # print(f"Saving block {height} with {len(sql_tx_ids)} txs: {sql_tx_ids}")
        db.insert_block(height, block_time, sql_tx_ids)

        # for msg_type, count in msg_types.items():
        #     db.insert_type_count(msg_type, count, height)

        # print(f"save_values_to_sql. Exited early")
        # exit(1)

        
    # Saves to it after we go through all group of blocks
    db.commit()
    pass


# from websocket import create_connection
if __name__ == "__main__":
    db = Database(os.path.join(current_dir, "data.db"))    
    db.create_tables()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    loop.close()
