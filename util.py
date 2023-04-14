import asyncio
import json
import os

import httpx

current_dir = os.path.dirname(os.path.realpath(__file__))

COSMOS_BINARY_FILE = "juno-decode"

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

        if not isinstance(value, str):
            continue

        if not (value.startswith(WALLET_PREFIX) or value.startswith(VALOPER_PREFIX)):
            continue
        
        # smart contracts are ignored. junovaloper1 = 50 characters.
        if len(value) > 50:
            continue        

        return value

    # write error to file if there is no sender found (we need to add this type)
    with open(os.path.join(current_dir, "no_sender_error.txt"), "a") as f:
        f.write(str(msg) + "\n\n")

    return None


def get_block_txs(block_data: dict) -> list:
    return block_data.get("block", {}).get("data", {}).get("txs", [])


def get_latest_chain_height(RPC_ARCHIVE: str) -> int:
    r = httpx.get(f"{RPC_ARCHIVE}/abci_info?")

    if r.status_code != 200:
        # TODO: backup RPC?
        print(
            f"Error: get_latest_chain_height status_code: {r.status_code} @ {RPC_ARCHIVE}. Exiting..."
        )
        exit(1)

    current_height = (
        r.json().get("result", {}).get("response", {}).get("last_block_height", "-1")
    )

    print(f"Current Height: {current_height}")

    return int(current_height)


def get_block_height(block_data: dict) -> int:
    height = block_data["data"]["value"]["block"]["header"]["height"]
    print(f"Block Height: {height}")

    return height


def remove_useless_data(block_data: dict) -> dict:
    # remove result_begin_block in the value section
    # del block_data["data"]["value"]["result_begin_block"]
    # del block_data["data"]["value"]["result_end_block"]
    del block_data["block"]["last_commit"]["signatures"]

    # remove useless events for us
    # Likely not used anymore since I am not subscribing
    # del block_data["events"]["commission.amount"]
    # del block_data["events"]["commission.validator"]
    # del block_data["events"]["rewards.amount"]
    # del block_data["events"]["rewards.validator"]
    # del block_data["events"]["coin_spent.amount"]
    # del block_data["events"]["coin_received.receiver"]
    # del block_data["events"]["proposer_reward.amount"]
    # del block_data["events"]["mint.amount"]
    # del block_data["events"]["coinbase.amount"]

    return block_data
