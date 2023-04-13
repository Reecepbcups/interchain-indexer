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

# TODO: move this to main.py while we download Txs?

current_dir = os.path.dirname(os.path.realpath(__file__))

CONVERT_START = 7_520_000
CONVERT_END = 7_550_000 # 7_819_899 # latest

CPU_THREADS = multiprocessing.cpu_count()

COSMOS_BINARY_FILE = "juno-decode"
def run_decode_file(file_loc: str, output_file_loc: str) -> dict:
    res = os.popen(f"{COSMOS_BINARY_FILE} tx decode-file {file_loc} {output_file_loc}").read()    
    with open(output_file_loc, 'r') as f:
        return json.load(f)    

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

def do_logic(db: Database, to_decode: list[dict]):
    start_time = time.time()

    # Dump our amino to file so the juno-decoder can pick it up (decodes in chunks)    
    with open('amino.json', 'w') as f:
        json.dump(to_decode, f)  

    values = run_decode_file("amino.json", "output.json")

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
            db.insert_type_count(msg_type, count, height)
            print(f"Inserted {msg_type} {count} {height}")

        # save users who sent the tx to the database for the users table
        db.insert_user(sender, height, tx_id)
        print(f"Inserted {sender} {height} {tx_id}")

        # print(tx_id, tx_data, msg_types)
        db.update_tx(tx_id, json.dumps(tx_data), json.dumps(msg_types_list)) 
        # exit(1)

    db.commit()
    end_time = time.time()
    print(f"Time to decode & store ({len(to_decode)}): {end_time - start_time}") 
    pass


async def main():
    db = Database(os.path.join(current_dir, "data.db"))
    db.create_tables()
    # latest_block = db.get_latest_saved_block()
    # print(f"Latest Block Height: {latest_block.height}")

    total = db.get_total_blocks()
    print(f"Total Blocks: {total}")

    txs = db.get_txs_in_range(CONVERT_START, CONVERT_END)
    print(f"Total Txs in this range: {len(txs)}")    

    to_decode = []
    for tx in txs:
        if len(tx.tx_json) == 0:
            # ignore storecode                  
            if len(tx.tx_amino) > 30_000:                
                continue

            to_decode.append({
                "id": tx.id,   
                "tx": tx.tx_amino
            })                            
        if len(to_decode) >= 5_000:
            do_logic(db, to_decode) # TODO: THis will not run since we already ran tx_json. Maybe we clear all tx_json rows? then run 
            to_decode.clear()  

    # if to_decode still has some though less than 5000, then run it one last time to finish up
    if len(to_decode) > 0:
        do_logic(db, to_decode)
        to_decode.clear()
    
    print("Done")
    pass



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())    
