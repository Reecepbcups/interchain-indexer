"""
We subscribe to check when the latest block is updated, then we query it normally via the RPC & save it
"""

import json
import os

import httpx
import rel
import websocket

from SQL import Database
from util import decode_txs, get_block_txs, remove_useless_data, update_latest_height

# TODO: Save Txs & events to postgres?
# maybe a redis cache as well so other people can subscribe to redis for events?

# https://docs.tendermint.com/v0.34/rpc/
RPC_IP = "15.204.143.232:26657"  # if this is blank, we update every 6 seconds
RPC_URL = f"ws://{RPC_IP}/websocket"
RPC_ARCHIVE = "https://rpc-archive.junonetwork.io:443"

WALLET_PREFIX = "juno1"
COSMOS_BINARY_FILE = "junod"

current_dir = os.path.dirname(os.path.realpath(__file__))
# blocks = os.path.join(current_dir, "blocks")
# user_txs = os.path.join(current_dir, "users")
# txs_folder = os.path.join(current_dir, "txs")

# os.makedirs(blocks, exist_ok=True)
# os.makedirs(user_txs, exist_ok=True)
# os.makedirs(txs_folder, exist_ok=True)


# TODO: get current height. If there is a difference, then we need to query the RPC for the missing blocks.
latest_height_file = os.path.join(current_dir, "latest_height.txt")
latest_height = -1
if os.path.exists(latest_height_file):
    with open(latest_height_file, "r") as f:
        latest_height = int(f.read())
print(f"Latest Height: {latest_height}")


def get_latest_chain_height() -> int:
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

    difference = int(current_height) - latest_height
    print(f"Missing {difference:,} blocks")

    return int(current_height)


unique_tx_id = 0

ignore = [
    "ibc.core.client.v1.MsgUpdateClient",
    "ibc.core.channel.v1.MsgAcknowledgement",
]


def download_block(height: int):
    global unique_tx_id

    block_data = (
        httpx.get(f"{RPC_ARCHIVE}/block?height={height}").json().get("result", {})
    )

    # Gets block transactions, decodes them to JSON, and saves them to the block_data
    block_txs = get_block_txs(block_data)
    decoded_txs = decode_txs(COSMOS_BINARY_FILE, block_txs)
    block_data["block"]["data"]["txs"] = decoded_txs

    # Gets unique addresses from events (users/contracts interacted with during this time frame)
    # Useful for cache solutions. So if a user does not have any changes here, then we can keep them cached longer
    # This only applies when we subscribe
    # block_events = get_block_events(block_data)
    # unique_event_addresses = get_unique_event_addresses(WALLET_PREFIX, block_events)
    # block_data["events"]["all_unique_event_addresses"] = list(unique_event_addresses)

    # Removes useless events we do not need to cache which take up lots of space
    updated_data = remove_useless_data(block_data)

    # Saves data to a file
    # TODO: Replace with SQL
    # with open(os.path.join(blocks, f"{height}.json"), "w") as f:
    #     f.write(json.dumps(updated_data))

    start_tx_id = -1
    unique_id = -1
    for tx in decoded_txs:
        sender = tx.get("body", {}).get("messages", [{}])[0].get("sender", {})

        if sender == {}:
            sender = (
                tx.get("body", {}).get("messages", [{}])[0].get("delegator_address", {})
            )

        if sender == {}:
            sender = tx.get("body", {}).get("messages", [{}])[0].get("from_address", {})

        # grantee
        if sender == {}:
            sender = tx.get("body", {}).get("messages", [{}])[0].get("grantee", {})

        # voter
        if sender == {}:
            sender = tx.get("body", {}).get("messages", [{}])[0].get("voter", {})

        # if ignore is in the string of the tx, continue
        if any(x in str(tx) for x in ignore):
            continue

        # save tx to the txs folder with a unique id of unique_tx_id
        # with open(os.path.join(txs_folder, f"{unique_tx_id}.json"), "w") as f:
        #     f.write(json.dumps(tx))

        # file = f"{sender}.json"
        # txs = {}
        # if os.path.exists(os.path.join(user_txs, file)):
        #     with open(os.path.join(user_txs, file), "r") as f:
        #         txs = json.loads(f.read())

        # append new tx with height as key
        # txs[height] = unique_tx_id

        # with open(os.path.join(user_txs, file), "w") as f:
        #     f.write(json.dumps(txs))
        unique_id = db.insert_tx(tx)
        print(f"Inserted tx {unique_id} with height {height}")

        if start_tx_id == -1:
            start_tx_id = unique_id

        # insert unique_id for user
        db.insert_user(sender, height, unique_id)

    # Get height & save latest to file since we dumped it now
    update_latest_height(latest_height_file, height)

    db.insert_block(height, [i for i in range(start_tx_id, unique_id + 1)])


def on_message(ws, message):
    msg = dict(json.loads(message))

    if msg.get("result") == {}:
        print("Subscribed to New Block...")
        return

    msg_height = (
        msg.get("result", {})
        .get("data", {})
        .get("value", {})
        .get("block", {})
        .get("header", {})
        .get("height")
    )

    download_block(msg_height)


def on_error(ws, error):
    print("error", error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    print("Opened connection")
    ws.send(
        '{"jsonrpc": "2.0", "method": "subscribe", "params": ["tm.event=\'NewBlock\'"], "id": 1}'
    )
    print("Sent subscribe request")


def test_get_data():
    txs_in_block = db.get_block_txs(7781608)
    print(txs_in_block)

    # using the database get tx with id of 29
    tx = db.get_tx(txs_in_block[-1])
    print(tx)

    # get sender for a tx
    sender_txs = db.get_user_tx_ids("juno15aay8perht035ugje30r3k49ce8rrz66ld5myp")
    print(sender_txs)

    sender_txs = db.get_user_txs("juno15aay8perht035ugje30r3k49ce8rrz66ld5myp")
    print(sender_txs)


# from websocket import create_connection
if __name__ == "__main__":
    db = Database("data.db")
    # db.drop_all()
    db.create_tables()

    if False and len(RPC_IP) > 0:
        websocket.enableTrace(False)  # toggle to show or hide output
        ws = websocket.WebSocketApp(
            f"{RPC_URL}",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        ws.run_forever(
            dispatcher=rel, reconnect=5
        )  # Set dispatcher to automatic reconnection, 5 second reconnect delay if connection closed unexpectedly
        rel.signal(2, rel.abort)  # Keyboard Interrupt
        rel.dispatch()
    else:
        # while loop, every 6 seconds query the RPC for latest and download
        pass

    if False:
        for i in range(7781600, 7781610):
            # latest_height = get_latest_chain_height()
            # download_block(latest_height)
            download_block(i)

    test_get_data()
