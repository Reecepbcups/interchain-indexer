"""
We subscribe to check when the latest block is updated, then we query it normally via the RPC & save it
"""

# TODO: save Tx types to a table over time

import json
import os
import time

import httpx
import rel
import websocket

from SQL import Database
from util import (
    decode_txs,
    get_block_txs,
    get_latest_chain_height,
    get_sender,
    remove_useless_data,
)

# TODO: Save Txs & events to postgres?
# maybe a redis cache as well so other people can subscribe to redis for events?

# https://docs.tendermint.com/v0.34/rpc/
RPC_IP = "15.204.143.232:26657"  # if this is blank, we update every 6 seconds
RPC_URL = f"ws://{RPC_IP}/websocket"
RPC_ARCHIVE = "https://rpc-archive.junonetwork.io:443"

WALLET_PREFIX = "juno1"
WALLET_LENGTH = 43
COSMOS_BINARY_FILE = "junod"

MINIMUM_DOWNLOAD_HEIGHT = 6700000  # set to -1 if you want to ignore

current_dir = os.path.dirname(os.path.realpath(__file__))

ignore = [
    "ibc.core.client.v1.MsgUpdateClient",
    "ibc.core.channel.v1.MsgAcknowledgement",
]

latest_height = -1


def download_block(height: int):
    # Skip already downloaded height data
    if db.get_block_txs(height) != None:
        if height % 100 == 0:
            print(f"Block {height} is already downloaded")
        return

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
    # updated_data = remove_useless_data(block_data)

    start_tx_id = -1
    unique_id = -1

    msg_types: dict[str, int] = {}
    for idx, tx in enumerate(decoded_txs):
        messages = tx.get("body", {}).get("messages", [])

        sender = get_sender(messages[0], WALLET_PREFIX)

        for msg in messages:
            msg_type = msg.get("@type")
            if msg_type in msg_types.keys():
                msg_types[msg_type] += 1
            else:
                msg_types[msg_type] = 1

        # if ignore is in the string of the tx, continue
        if any(x in str(tx) for x in ignore):
            continue

        unique_id = db.insert_tx(tx)
        if start_tx_id == -1:
            start_tx_id = unique_id

        # insert users tx id link
        db.insert_user(str(sender), height, unique_id)

    for mtype, count in msg_types.items():
        db.insert_type_count(mtype, count, height)

    count = db.get_type_count_at_height("/cosmwasm.wasm.v1.MsgExecuteContract", height)

    num_txs = [i for i in range(start_tx_id, unique_id + 1)]
    db.insert_block(height, num_txs)

    print(f"Block {height}: {len(num_txs)} txs")


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
    # tables = db.get_all_tables()
    # print(tables)
    # schema = db.get_table_schema("messages")
    # print(schema)

    # txs_in_block = db.get_block_txs(7781750)
    # print(txs_in_block)

    # tx = db.get_tx(txs_in_block[-1])
    # print(tx)

    # sender_txs = db.get_user_tx_ids("juno195mm9y35sekrjk73anw2az9lv9xs5mztvyvamt")
    # print(sender_txs)

    # sender_txs = db.get_user_txs("juno195mm9y35sekrjk73anw2az9lv9xs5mztvyvamt")
    # print(sender_txs)

    # # all_accs = db.get_all_accounts()
    # # print(all_accs)

    # count = db.get_type_count_at_height("/cosmwasm.wasm.v1.MsgExecuteContract", 7781750)
    # print(count)

    total = db.get_total_blocks()
    print("Total Blocks", total)

    range_count = db.get_type_count_over_range(
        "/cosmwasm.wasm.v1.MsgExecuteContract", 7781750, 7782188
    )
    all_range = db.get_all_count_over_range(7781750, 7782188)
    print(sum(range_count))
    print(sum(all_range))

    # exit(1)
    pass


# from websocket import create_connection
if __name__ == "__main__":
    db = Database("data.db")
    # db.drop_all()
    db.create_tables()

    latest_height = db.get_latest_saved_block_height()
    print(f"Latest saved block height: {latest_height}")

    # Download missing blocks before trying to subscribe / 6 second loop

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
        if True:
            test_get_data()

        # while loop, every 6 seconds query the RPC for latest and download. Try catch
        while True:
            latest_height = get_latest_chain_height(
                RPC_ARCHIVE=RPC_ARCHIVE, latest_saved_height=latest_height
            )

            last_downloaded = db.get_latest_saved_block_height()
            block_diff = latest_height - last_downloaded
            if block_diff > 0:
                print(
                    f"Downloading blocks, latest height: {latest_height}. Behind by: {block_diff}"
                )

                if MINIMUM_DOWNLOAD_HEIGHT > 0:
                    if last_downloaded < MINIMUM_DOWNLOAD_HEIGHT:
                        last_downloaded = MINIMUM_DOWNLOAD_HEIGHT

                # download 1 behind just to ensure we got it
                for i in range(last_downloaded - 1, latest_height + 1):
                    download_block(i)

            time.sleep(6)
