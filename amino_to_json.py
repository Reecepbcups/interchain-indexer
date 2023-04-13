'''
running this goes through all Txs through a block range, converts to JSON, and updates the Tx value

Goal: 
- Very quickly convert all to JSON
- Proto decode? (faster than juno-decoder) OR speed uyp juno-decoder to run on all threads

'''


import json
import multiprocessing
import os
import time
from concurrent.futures import ThreadPoolExecutor

from SQL import Database

current_dir = os.path.dirname(os.path.realpath(__file__))

CONVERT_START = 7_500_000
CONVERT_END = 7_500_100 # 7_819_899 # latest

CPU_THREADS = multiprocessing.cpu_count()

def main():
    db = Database(os.path.join(current_dir, "data.db"))
    # latest_block = db.get_latest_saved_block()
    # print(f"Latest Block Height: {latest_block.height}")

    total = db.get_total_blocks()
    print(f"Total Blocks: {total}")

    # earliest_block = db.get_earliest_block()
    # print(f"Earliest Block: {earliest_block.height}")

    # create a pool of threads for CPU_THREADS
    pool = multiprocessing.Pool(CPU_THREADS)    



    txs = db.get_txs_in_range(CONVERT_START, CONVERT_END)
    print(f"Total Txs: {len(txs)}")

    to_decode = {}
    for tx in txs:
        if len(tx.tx_json) == 0:
            # print("Decoding")
            to_decode[tx.id] = tx.tx_amino
        
        # every 100, decode
        # if len(to_decode) >= 250:
        #     start_time = time.time()
        #     decoded = pool.map(run_decode_single_async, to_decode.values())
        #     end_time = time.time()
        #     print(f"Time to decode ({len(to_decode)}): {end_time - start_time}")        
        #     # for tx_id, tx in zip(to_decode.keys(), decoded):
        #         # db.update_tx(tx_id, json.dumps(tx))
        #     # then commit here
        #     to_decode = {}

        # do the above with threading
        if len(to_decode) >= 250:
            start_time = time.time()
            with ThreadPoolExecutor() as executor:
                decoded = executor.map(run_decode_single_async, to_decode.values())                
            end_time = time.time()
            print(f"Time to decode ({len(to_decode)}): {end_time - start_time}")
            to_decode = {}



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


COSMOS_BINARY_FILE = "juno-decode"
def run_decode_single_async(tx: str) -> dict:
    # TODO: replace this with proto in the future?    
    if len(tx) > 32000:
        # Store codes
        return {}

    res = os.popen(f"{COSMOS_BINARY_FILE} tx decode {tx} --output json").read()
    return json.loads(res)


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
    main()
