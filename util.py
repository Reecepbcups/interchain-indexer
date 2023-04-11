import json
import os

import httpx

current_dir = os.path.dirname(os.path.realpath(__file__))


def __run_decode(cosmos_binary: str, tx: str) -> dict:
    # check for max len tx (store code breaks this for CLI usage on linux)
    if len(tx) > 32766:
        # print("TX too long. Skipping...")
        return {}

    res = os.popen(f"{cosmos_binary} tx decode {tx} --output json").read()
    return json.loads(res)


def decode_txs(COSMOS_BINARY_FILE: str, block_txs: list[str]) -> list:
    decoded_txs = []

    # iterate through each and convert to json with junod
    for txs in block_txs:
        tx = __run_decode(COSMOS_BINARY_FILE, txs)
        if isinstance(tx, dict) and len(tx) > 0:
            decoded_txs.append(tx)

    return decoded_txs


def get_sender(msg: dict, WALLET_PREFIX: str) -> str | None:
    keys = ["sender", "delegator_address", "from_address", "grantee", "voter", "signer", "depositor", "proposer"]

    for key in keys:
        if key in msg.keys():
            return msg[key]

    # tries to find the sender in the msg even if the key is not found
    for key, value in msg.items():
        if (
            isinstance(value, str)
            and value.startswith(WALLET_PREFIX)
            and len(value) == 43
        ):
            with open(os.path.join(current_dir, "get_sender_foundkey.txt"), "a") as f:
                f.write(f"Found sender: {value} as {key}" + " - " + str(msg) + "\n\n")
            return value

    # write error to file if there is no sender found (we need to add this type)
    with open(os.path.join(current_dir, "no_sender_error.txt"), "a") as f:
        f.write(str(msg) + "\n\n")

    return None


def get_block_txs(block_data: dict) -> list:
    return block_data.get("block", {}).get("data", {}).get("txs", [])


def get_latest_chain_height(RPC_ARCHIVE: str, latest_saved_height: int) -> int:
    current_height = (
        httpx.get(f"{RPC_ARCHIVE}/abci_info?")
        .json()
        .get("result", {})
        .get("response", {})
        .get("last_block_height", "-1")
    )
    print(f"Current Height: {current_height}")
    if current_height == "-1":
        print("Could not get current height. Exiting...")
        return -1

    difference = int(current_height) - latest_saved_height
    print(f"Missing {difference:,} blocks")

    return int(current_height)


def get_block_events(block_data: dict) -> dict:
    return block_data.get("events", {})


def get_block_height(block_data: dict) -> int:
    height = block_data["data"]["value"]["block"]["header"]["height"]
    print(f"Block Height: {height}")

    return height


def get_unique_event_addresses(wallet_prefix: str, block_events: dict) -> list[str]:
    # any address which had some action in the block
    event_addresses: list[str] = []

    for event_key, value in block_events.items():
        if not isinstance(value, list):
            continue

        for v in value:
            if isinstance(v, str) and v.startswith(wallet_prefix):
                if v not in event_addresses:
                    event_addresses.append(v)

    return event_addresses


def remove_useless_data(block_data: dict) -> dict:
    # remove result_begin_block in the value section
    # del block_data["data"]["value"]["result_begin_block"]
    # del block_data["data"]["value"]["result_end_block"]
    del block_data["block"]["last_commit"]["signatures"]

    # remove useless events for us
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
