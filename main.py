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
import os
import random
import subprocess
import time

import httpx
from chain_types import BlockData
from dotenv import load_dotenv
from SQL import Database
from util import command_exists, get_latest_chain_height, get_sender, run_decode_file

load_dotenv()

# ==== CONFIGURATION (.ENV) ====
START_BLOCK = int(os.getenv("START_BLOCK", -1))
END_BLOCK = int(os.getenv("END_BLOCK", -1))
GROUPING = int(os.getenv("GROUPING", 100))

# if start or stop are below 0, error and exit
if START_BLOCK < 0 or END_BLOCK < 0:
    print("START_BLOCK or END_BLOCK is below 0")
    exit(1)

# download, decode, and both (when synced fully)
TASK = os.getenv("TASK", "not_set")
if TASK == "not_set":
    print("TASK not set in .env file")
    exit(1)


rpc_links: str = os.getenv("RPC_NODES", "")
if rpc_links.endswith(","):
    rpc_links = rpc_links[:-1]

RPC_ARCHIVE_LINKS: list[str] = rpc_links.split(",")
if len(RPC_ARCHIVE_LINKS) == 0:
    print("No RPC nodes found in .env file")
    exit(1)

# https://github.com/Reecepbcups/juno-decode
COSMOS_PROTO_DECODER_BINARY_FILE = os.getenv(
    "COSMOS_PROTO_DECODE_BINARY", "juno-decode"
)
if not command_exists(COSMOS_PROTO_DECODER_BINARY_FILE):
    print(f"Command {COSMOS_PROTO_DECODER_BINARY_FILE} not found")
    exit(1)

WALLET_PREFIX = os.getenv("WALLET_PREFIX", "juno1")
VALOPER_PREFIX = os.getenv("VALOPER_PREFIX", "junovaloper1")


current_dir = os.path.dirname(os.path.realpath(__file__))
DUMPFILE = os.path.join(os.path.dirname(__file__), "tmp-amino.json")
OUTFILE = os.path.join(os.path.dirname(__file__), "tmp-output.json")
DECODE_LIMIT = 10_000


async def download_block(client: httpx.AsyncClient, height: int) -> BlockData | None:
    if db.get_block(height) != None:
        if height % 1000 == 0:
            print(f"Block {height} is already downloaded & saved in SQL")
        return None

    RPC_ARCHIVE_URL = random.choice(RPC_ARCHIVE_LINKS)
    REAL_URL = f"{RPC_ARCHIVE_URL}/block?height={height}"
    r = await client.get(REAL_URL, timeout=30)
    if r.status_code != 200:
        print(f"Error: {r.status_code} @ height {height}")
        with open(os.path.join(current_dir, f"errors.txt"), "a") as f:
            f.write(
                f"Height: {height};{r.status_code} @ {RPC_ARCHIVE_URL} @ {time.time()};{r.text}\n\n"
            )
        return None

    block_time = ""
    encoded_block_txs: list = []
    try:
        v = r.json()["result"]["block"]
        block_time = v["header"]["time"]  # 2023-04-13T17:46:31.898429796Z
        encoded_block_txs = v["data"]["txs"]  # ["amino1"]
    except KeyError:
        return None

    # Removes CosmWasm store_codes
    amino_txs = []
    for x in encoded_block_txs:
        if len(x) < 32000:
            amino_txs.append(x)

    return BlockData(height, block_time, encoded_block_txs)


async def main():
    global START_BLOCK, END_BLOCK

    while True:
        last_saved: int = db.get_latest_saved_block().height
        current_chain_height = get_latest_chain_height(RPC_ARCHIVE=RPC_ARCHIVE_LINKS[0])
        print(f"Chain height: {current_chain_height:,}. Last saved: {last_saved:,}")

        if END_BLOCK > current_chain_height:
            END_BLOCK = current_chain_height

        # ensure end is a multiple of grouping
        END_BLOCK = current_chain_height - (current_chain_height % GROUPING)
        print(
            f"Blocks: {START_BLOCK:,}->{END_BLOCK:,}. Download Spread: {(int(END_BLOCK) - START_BLOCK):,} blocks"
        )

        # This is a list of list of tasks to do. Each task should be done on its own thread
        async with httpx.AsyncClient() as httpx_client:
            # with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
            for i in range((END_BLOCK - START_BLOCK) // GROUPING + 1):
                tasks = {}
                start_time = time.time()
                for j in range(GROUPING):
                    # block index from the grouping its in
                    block = START_BLOCK + i * GROUPING + j
                    tasks[block] = asyncio.create_task(
                        download_block(httpx_client, block)
                    )

                # This should never happen, just a precaution.
                # When this does happen nothing works (ex: node down, no binary to decode)
                try:
                    values = await asyncio.gather(*tasks.values())
                    if all(x is None for x in values):
                        continue
                    save_values_to_sql(values)
                except Exception as e:
                    print(f"Erorr: main(): {e}")
                    continue

                print(
                    f"Finished #{len(tasks)} blocks in {time.time() - start_time} seconds @ {START_BLOCK + i * GROUPING} -> {START_BLOCK + (i + 1) * GROUPING}"
                )

                # print(f"early return on purpose for testing"); exit(1)

        # TODO: how does this handle if we only have like 5 blocks to download? (After we sync to tip)
        print("Finished")
        time.sleep(6)
        exit(1)


def decode_and_save_updated(to_decode: list[dict]):
    global db

    start_time = time.time()

    # Dump our amino to file so the juno-decoder can pick it up (decodes in chunks)
    with open(DUMPFILE, "w") as f:
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

        db.update_tx(tx_id, json.dumps(tx_data), json.dumps(msg_types_list))
        # exit(1)

    db.commit()

    if TASK == "decode":
        print(f"Time to decode & store ({len(to_decode)}): {time.time() - start_time}")

    # os.remove(DUMPFILE)
    # os.remove(OUTFILE)
    pass


def do_decode(lowest_height: int, highest_height: int):
    global db

    # make this into groups of 10_000 blocks
    groups = []
    BLOCKS_GROUPING = 5_000

    if highest_height - lowest_height <= BLOCKS_GROUPING:
        groups.append(
            {
                "start": lowest_height,
                "end": highest_height,
            }
        )
    else:
        for i in range((highest_height - lowest_height) // BLOCKS_GROUPING + 1):
            groups.append(
                {
                    "start": lowest_height + i * BLOCKS_GROUPING,
                    "end": lowest_height + (i + 1) * BLOCKS_GROUPING,
                }
            )
        # add the final group as the difference
        if groups[-1]["end"] < highest_height:
            groups.append(
                {
                    "start": groups[-1]["end"],
                    "end": highest_height,
                }
            )

    for group in groups:
        print(f"Decoding group: {group['start']} -> {group['end']}")
        start_height = group["start"]
        end_height = group["end"]
        # print(f"Decoding {start_height} -> {end_height}")

        txs = db.get_txs_in_range(start_height, end_height)
        print(f"Total Txs in this range: {len(txs)}")

        # Get what Txs we need to decode for the custom -decode binary
        to_decode = []
        for tx in txs:
            if len(tx.tx_json) != 0:
                continue

            # ignore storecode
            if len(tx.tx_amino) > 30_000:
                continue

            to_decode.append({"id": tx.id, "tx": tx.tx_amino})

            if len(to_decode) >= DECODE_LIMIT:
                # early decode if too many Txs
                decode_and_save_updated(to_decode)
                to_decode.clear()
                db.commit()

        if len(to_decode) > 0:
            decode_and_save_updated(to_decode)
            to_decode.clear()
            db.commit()


def save_values_to_sql(values: list[BlockData]):
    global db

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

    if TASK == "sync":
        values.sort(key=lambda x: x.height if x is not None else 0)
        do_decode(values[0].height, values[-1].height)

    pass


if __name__ == "__main__":
    db = Database(os.path.join(current_dir, "data.db"))
    db.optimize_tables()
    db.create_tables()

    if TASK == "decode":
        print(
            "Doing a decode of all Txs in the range {} - {}".format(
                START_BLOCK, END_BLOCK
            )
        )
        do_decode(START_BLOCK, END_BLOCK)
        exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    loop.close()
