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

import httpx

from chain_types import BlockData
from SQL import Database
from util import get_latest_chain_height, get_sender, run_decode_file

# ENV FILE
START_BLOCK = 7_000_000
END_BLOCK = 7_800_000
GROUPING = 500  # 50-200 is good.

RPC_ARCHIVE_LINKS: list[str] = [
    "https://rpc-archive.junonetwork.io:443",
    # "https://rpc.juno.strange.love:443", # not archive, using for testing through
    # "https://juno-rpc.reece.sh:443", # not archive, using for testing through
    # "https://juno-rpc.polkachu.com:443", # not archive, using for testing through
]

# https://github.com/Reecepbcups/juno-decode
COSMOS_PROTO_DECODER_BINARY_FILE = "juno-decode"

WALLET_PREFIX = "juno1"
VALOPER_PREFIX = "junovaloper1"
# WALLET_LENGTH = 43


current_dir = os.path.dirname(os.path.realpath(__file__))

DUMPFILE = os.path.join(os.path.dirname(__file__), "tmp-amino.json")
OUTFILE = os.path.join(os.path.dirname(__file__), "tmp-output.json")


async def download_block(client: httpx.AsyncClient, height: int) -> BlockData | None:    
    if db.get_block(height) != None:
        print(f"Block {height} is already downloaded & saved in SQL")
        return None

    RPC_ARCHIVE_URL = random.choice(RPC_ARCHIVE_LINKS)
    r = await client.get(f"{RPC_ARCHIVE_URL}/block?height={height}", timeout=30)
    if r.status_code != 200:        
        print(f"Error: {r.status_code} @ height {height}")
        with open(os.path.join(current_dir, f"errors.txt"), "a") as f:
            f.write(f"Height: {height};{r.status_code} @ {RPC_ARCHIVE_URL} @ {time.time()};{r.text}\n\n")
        return None

    block_time = ""
    encoded_block_txs: list = []
    try:
        v = r.json()["result"]["block"]

        block_time = v["header"]["time"] # 2023-04-13T17:46:31.898429796Z
        encoded_block_txs = v["data"]["txs"] # ["amino1"]
    except KeyError:
        return None
    
    # Removes CosmWasm store_codes
    encoded_block_txs = [x for x in encoded_block_txs if len(x) < 32000]
    
    return BlockData(height, block_time, encoded_block_txs)


async def main():
    global START_BLOCK, END_BLOCK

    # while loop, every 6 seconds query the RPC for latest and download.
    while True:
        last_saved: int = db.get_latest_saved_block().height
        current_chain_height = get_latest_chain_height(RPC_ARCHIVE=RPC_ARCHIVE_LINKS[0])        
        print(f"Chain height: {current_chain_height:,}. Last saved: {last_saved:,}")                
                
        if END_BLOCK > current_chain_height:
            END_BLOCK = current_chain_height

        # ensure end is a multiple of grouping
        END_BLOCK = current_chain_height - (current_chain_height % GROUPING)    
        print(f"Blocks: {START_BLOCK:,}->{END_BLOCK:,}. Download Spread: {(int(END_BLOCK) - START_BLOCK):,} blocks")        

        # Runs through groups for downloading from the RPC
        async with httpx.AsyncClient() as httpx_client:            
            # with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
            for i in range((END_BLOCK - START_BLOCK) // GROUPING + 1):
                tasks = {}
                start_time = time.time()
                for j in range(GROUPING):
                    # block index from the grouping its in
                    block = START_BLOCK + i * GROUPING + j
                    tasks[block] = asyncio.create_task(download_block(httpx_client, block))                    

                # This should never happen, just a precaution. 
                # When this does happen nothing works (ex: node down, no binary to decode)
                try:
                    values = await asyncio.gather(*tasks.values())
                    save_values_to_sql(values)                                                
                except Exception as e:
                    print(f"Erorr: main(): {e}")                    
                    continue                    
                
                print(
                    f"Finished #{len(tasks)} blocks in {time.time() - start_time} seconds @ {START_BLOCK + i * GROUPING}"
                )

                    # print(f"early return on purpose for testing"); exit(1)

        # TODO: how does this handle if we only have like 5 blocks to download? (After we sync to tip)
        print("Finished")
        time.sleep(6)        
        exit(1)

def decode_and_save_updated(db: Database, to_decode: list[dict]):
    start_time = time.time()

    # Dump our amino to file so the juno-decoder can pick it up (decodes in chunks)    
    with open(DUMPFILE, 'w') as f:
        json.dump(to_decode, f)  

    # Decodes this file, and saves to the output file (from the chain-decoder binary)
    # Calls syncronously, since we handle so many decodes in 1 call.
    values = run_decode_file(COSMOS_PROTO_DECODER_BINARY_FILE, DUMPFILE, OUTFILE)

    for data in values:
        tx_id = data["id"]
        tx_data = json.loads(data["tx"])

        tx = db.get_tx(tx_id)
        height = 0
        if tx is not None:
            height = tx.height

        sender = get_sender(tx_data["body"]["messages"][0], "juno", "junovaloper")
        if sender is None:
            print("No sender found for tx: ", tx_id)
            continue

        # get message types
        msg_types = {}
        for msg in tx_data["body"]["messages"]:                
            if msg["@type"] not in msg_types:
                msg_types[msg["@type"]] = 0

            msg_types[msg["@type"]] += 1

        msg_types_list = list(msg_types.keys())
        msg_types_list.sort()
        
        for msg_type, count in msg_types.items():                
            db.insert_msg_type_count(msg_type, count, height)            

        # save users who sent the tx to the database for the users table
        db.insert_user(sender, height, tx_id)        

        # print(tx_id, tx_data, msg_types)
        db.update_tx(tx_id, json.dumps(tx_data), json.dumps(msg_types_list)) 
        # exit(1)
    
    db.commit()
    end_time = time.time()
    print(f"Time to decode & store ({len(to_decode)}): {end_time - start_time}")     
        
    os.remove(DUMPFILE)
    os.remove(OUTFILE)
    pass


def save_values_to_sql(values: list[BlockData]):        
    values.sort(key=lambda x: x.height)

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
            unique_id = db.insert_tx(height, amino_tx)            
            sql_tx_ids.append(unique_id)
        
        db.insert_block(height, block_time, sql_tx_ids)

        # print(f"save_values_to_sql. Exited early")
        # exit(1)
        
    # Saves to it after we go through all group of blocks
    db.commit()

    # NOTE: The following allows for bulk decoding from the above Txs
    # we already sorted above
    lowest_height = values[0].height    
    highest_height = values[-1].height
    
    # NOTE: How does this handle if we somehow miss the block that Txs are in? Should be fine i think.
    txs = db.get_txs_in_range(lowest_height, highest_height)
    print(f"Total Txs in this range: {len(txs)}")

    # Get what Txs we need to decode for the custom -decode binary
    to_decode = []
    for tx in txs:
        if len(tx.tx_json) != 0:
            continue

        # ignore storecode                  
        if len(tx.tx_amino) > 30_000:                
            continue

        to_decode.append({
            "id": tx.id,   
            "tx": tx.tx_amino
        })
    
    if len(to_decode) > 0:
        decode_and_save_updated(db, to_decode)
        to_decode.clear()
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
