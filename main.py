import json
import os

import rel
import websocket

from util import (
    decode_txs,
    get_block_events,
    get_block_height,
    get_block_txs,
    get_unique_event_addresses,
    remove_useless_data,
    update_latest_height,
)

# TODO: Save Txs & events to postgres?
# maybe a redis cache as well so other people can subscribe to redis for events?

# https://docs.tendermint.com/v0.34/rpc/
RPC_IP = "15.204.143.232:26657"
RPC_URL = f"ws://{RPC_IP}/websocket"
WALLET_PREFIX = "juno1"
COSMOS_BINARY_FILE = "junod"

current_dir = os.path.dirname(os.path.realpath(__file__))
blocks = os.path.join(current_dir, "blocks")
os.makedirs(blocks, exist_ok=True)


# TODO: get current height. If there is a difference, then we need to query the RPC for the missing blocks.
latest_height_file = os.path.join(current_dir, "latest_height.txt")
latest_height = -1
if os.path.exists(latest_height_file):
    with open(latest_height_file, "r") as f:
        latest_height = int(f.read())
print(f"Latest Height: {latest_height}")


def on_message(ws, message):
    msg = dict(json.loads(message))

    if msg.get("result") == {}:
        print("Subscribed to New Block...")
        return

    block_data = msg.get("result", {})

    # Get height & save latest to file
    block_height = get_block_height(block_data)
    update_latest_height(latest_height_file, block_height)

    # Gets block transactions, decodes them to JSON, and saves them to the block_data
    block_txs = get_block_txs(block_data)
    decoded_txs = decode_txs(COSMOS_BINARY_FILE, block_txs)
    block_data["data"]["value"]["block"]["data"]["txs"] = decoded_txs

    # Gets unique addresses from events (users/contracts interacted with during this time frame)
    # Useful for cache solutions. So if a user does not have any changes here, then we can keep them cached longer
    block_events = get_block_events(block_data)
    unique_event_addresses = get_unique_event_addresses(WALLET_PREFIX, block_events)
    block_data["events"]["all_unique_event_addresses"] = list(unique_event_addresses)

    # Removes useless events we do not need to cache which take up lots of space
    updated_data = remove_useless_data(block_data)

    # Saves data to a file
    # TODO: learn postgres and save with relations
    with open(os.path.join(blocks, f"{block_height}.json"), "w") as f:
        f.write(json.dumps(updated_data))


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


# from websocket import create_connection
if __name__ == "__main__":
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
