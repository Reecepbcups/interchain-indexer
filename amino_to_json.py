'''
running this goes through all Txs through a block range, converts to JSON, and updates the Tx value

Goal: 
- Very quickly convert all to JSON
- Proto decode? (faster than juno-decoder) OR speed uyp juno-decoder to run on all threads

'''


import asyncio
import json
import multiprocessing
import os
import subprocess
import time

from SQL import Database

current_dir = os.path.dirname(os.path.realpath(__file__))

CONVERT_START = 7_500_000
CONVERT_END = 7_819_899 # 7_819_899 # latest

CPU_THREADS = multiprocessing.cpu_count()

COSMOS_BINARY_FILE = "juno-decode"
def run_decode_single_async(tx: str) -> dict:
    # TODO: replace this with proto in the future?    
    if len(tx) > 32000:
        # Store codes
        return {}

    print(f"Decoding: {tx[0:100]}")
    res = os.popen(f"{COSMOS_BINARY_FILE} tx decode {tx} --output json").read()
    return json.loads(res)

def run_decode_file(file_loc: str, output_file_loc: str) -> dict: # change to be a list of dicts?    
    res = os.popen(f"{COSMOS_BINARY_FILE} tx decode-file {file_loc} {output_file_loc}").read()
    
    values = {}
    with open(output_file_loc, 'r') as f:
        values = json.load(f)

    return values

def run_asyncio_commands(tasks, max_concurrent_tasks=0):
    if max_concurrent_tasks == 0:
        max_concurrent_tasks = len(tasks)
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    async def run_command(task):
        async with semaphore:
            return await task
    return loop.run_until_complete(asyncio.gather(*[run_command(task) for task in tasks]))

async def main():
    db = Database(os.path.join(current_dir, "data.db"))
    # latest_block = db.get_latest_saved_block()
    # print(f"Latest Block Height: {latest_block.height}")

    total = db.get_total_blocks()
    print(f"Total Blocks: {total}")

    txs = db.get_txs_in_range(CONVERT_START, CONVERT_END)
    print(f"Total Txs: {len(txs)}")


    def do_logic(to_decode: list[dict]):
        start_time = time.time()

        # Dump our amino to file so the juno-decoder can pick it up (decodes in chunks)    
        with open('amino.json', 'w') as f:
            json.dump(to_decode, f)  

        values = run_decode_file("amino.json", "output.json")

        for data in values:
            tx_id = data["id"]
            tx_data = json.loads(data["tx"])

            # get message types
            msg_types_set = set()
            for msg in tx_data["body"]["messages"]:
                msg_types_set.add(msg["@type"])

            msg_types = list(msg_types_set)
            msg_types.sort()

            # print(tx_id, tx_data, msg_types)
            db.update_tx(tx_id, json.dumps(tx_data), json.dumps(msg_types)) 
            # exit(1)
        db.commit()
        end_time = time.time()
        print(f"Time to decode & store ({len(to_decode)}): {end_time - start_time}") 
        pass

    to_decode = []
    for tx in txs:
        if len(tx.tx_json) == 0:                        
            if len(tx.tx_amino) > 30_000:
                # ignore storecode
                continue

            to_decode.append({
                "id": tx.id,
                "tx": tx.tx_amino
            })            
                
        if len(to_decode) >= 5_000:
            do_logic(to_decode)
            to_decode.clear()            


    # if to_decode still has some though less than 5000, then run it one last time and bypass
    if len(to_decode) > 0:
        do_logic(to_decode)
        to_decode.clear()

    # done
    print("Done")
    pass




    # Loop through all Txs in a block range, then convert the amino to JSON (unless it has alreayd been processed)

    # for idx, block_height in enumerate(range(CONVERT_START, CONVERT_END)):
    #     block = db.get_block(block_height)
    #     print(f"Block: {block.height}")        


        # amino = []
        # for tx in block.tx_ids:
        #     amino.append(db.get_tx(tx).tx_amino)

        # # run run_decode_single_async on all txs in the block
        # start_time = time.time()
        # txs = pool.map(run_decode_single_async, amino)
        # end_time = time.time()

        # print(f"Time to decode ({len(amino)}): {end_time - start_time}")        

        # # update tx here with tx_id
        # for tx_id, tx in zip(block.tx_ids, txs):
        #     # db.update_tx(tx_id, json.dumps(tx))
        #     print(tx_id, tx)
        #     exit(1)
        #     pass
            


        # if idx >= 150:
        #     exit(1)



    # total = db.get_msgs_over_range("*", earliest_block, latest_height)
    # print(f"Total Msgs: {sum(total):,}")

    # type_count = db.get_msgs_over_range(
    #     "/cosmwasm.wasm.v1.MsgExecuteContract", earliest_block, latest_height
    # )
    # print(f"Total ExecuteContract: {sum(type_count):,}")

    # values = db.get_types_at_height_over_range("/cosmwasm.wasm.v1.MsgExecuteContract", earliest_block, latest_height)
    # print(len(values))\

    # txs = db.get_msg_ids_in_range(
    #     "/cosmwasm.wasm.v1.MsgExecuteContract", earliest_block, earliest_block + 1000
    # )
    # print(txs)

    # TODO: Get which heights blocks are at

    # # get the transactions at this height
    # tx_ids = latest_block.tx_ids       
    # print(f"Latest block Tx IDs. Height: {latest_block.height}: {tx_ids}")
    # # # show the first tx in the txs list
    # tx = db.get_tx(tx_ids[0])
    # print(f"First Transaction: id:{tx.id}, amino:{tx.tx_amino}")





def get_sender(msg: dict, WALLET_PREFIX: str, VALOPER_PREFIX: str) -> str | None:
    keys = [
        "sender",
        "delegator_address",
        "from_address",
        "grantee",
        "voter",
        "signer",
        "depositor",
        "proposer",
    ]

    for key in keys:
        if key in msg.keys():
            return msg[key]

    # tries to find the sender in the msg even if the key is not found
    for key, value in msg.items():
        if (
            isinstance(value, str)
            and (value.startswith(WALLET_PREFIX) or value.startswith(VALOPER_PREFIX))
            and len(value) == 43
        ):
            with open(os.path.join(current_dir, "get_sender_foundkey.txt"), "a") as f:
                f.write(f"Found sender: {value} as {key}" + " - " + str(msg) + "\n\n")
            return value

    # write error to file if there is no sender found (we need to add this type)
    with open(os.path.join(current_dir, "no_sender_error.txt"), "a") as f:
        f.write(str(msg) + "\n\n")

    return None

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())    
