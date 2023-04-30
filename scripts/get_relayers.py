"""
Gets Relayers who have relayed.
Use data_relayer.db since we ignore these in the standard data origin db.

Config: (download, then decode with TX_AMINO_LENGTH_CUTTOFF_LIMIT set to 0 on download)

    "relaying_between_time": {
        "start": 5779000,
        "end": 7200000,
        "grouping": 1000,
        "rpc_endpoints": [
            "https://rpc-archive.junonetwork.io:443"
        ]
    },
"""

# Data: https://gist.github.com/Reecepbcups/80c84ce39ad00d8cb011a08a7a20bd1b

import json
import os
import sys
from base64 import b64decode

current_dir = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current_dir)
sys.path.append(parent)

from SQL import Database

# 5779678 -> 7990650
db = Database(os.path.join(current_dir, os.path.join(parent, "data_relayer.db")))
if db is None:
    print("No db found")
    exit(1)

earliest_block = db.get_earliest_block()
if earliest_block is None:
    print("No blocks found in db")
    exit(1)

latest_block = db.get_latest_saved_block()
if latest_block is None:
    print("No blocks found in db")
    exit(1)


# last_tx_saved = db.get_last_saved_tx().id
last_tx_saved = db.get_last_saved_tx()
if last_tx_saved is None:
    print("No txs found in db")
    exit(1)
print(f"{last_tx_saved.id=}")
# exit(1)

ibc_txs = {
    # '/ibc.applications.transfer.v1.MsgTransfer'
    # "/ibc.core.client.v1.MsgCreateClient"
    # "/ibc.core.channel.v1.MsgChannelOpenInit"
    # "/ibc.core.channel.v1.MsgChannelOpenAck"
    # "/ibc.core.channel.v1.MsgTimeout"
    # "/ibc.core.client.v1.MsgUpdateClient"
    # "/ibc.core.channel.v1.MsgRecvPacket"
    "/ibc.core.channel.v1.MsgAcknowledgement"
    # "/ibc.core.connection.v1.MsgConnectionOpenInit"
    # "/ibc.core.connection.v1.MsgConnectionOpenAck"
    # "/ibc.core.connection.v1.MsgConnectionOpenTry"
    # "/ibc.core.connection.v1.MsgConnectionOpenConfirm"
    # "/ibc.core.channel.v1.MsgChannelOpenTry"
    # "/ibc.core.channel.v1.MsgChannelOpenConfirm"
}

specific_ibc_tx_counter = 0
all_ibc_txs = 0
relayed_packets: dict[str, int] = {}
# msg_type: str

channels = {
    "cosmos": "channel-1",
    "osmosis": "channel-0",
    "evmos": "channel-70",
    "stargaze": "channel-20",
    "omniflix": "channel-78",
    "kujira": "channel-87",
    "axelar": "channel-71",
    "secret": "channel-48",
}


def get_ibc_packet_data(msg: dict) -> dict:
    data = msg["packet"]["data"]
    data = b64decode(data).decode("utf-8")
    data = json.loads(data)
    # amount = data["amount"]
    # denom = data["denom"]
    # receiver = str(data["receiver"])
    # sender = data["sender"]
    return data


# last_tx_saved.id
for i in range(1, last_tx_saved.id):
    if i % 10_000 == 0:
        print(f"Tx {i}")

    tx = db.get_tx(i)
    if tx is None:
        continue

    tx_json = json.loads(tx.tx_json)

    msg: dict
    for msg in list(tx_json["body"]["messages"]):
        # msg_type = msg["@type"]

        # We are not going to check for timeout packets
        if msg["@type"] != "/ibc.core.channel.v1.MsgAcknowledgement":
            continue

        signer = ""
        if "signer" not in msg:
            continue
        signer = msg["signer"]

        all_ibc_txs += 1

        source_channel = msg["packet"]["source_channel"]
        destination_channel = msg["packet"]["destination_channel"]

        if source_channel not in channels.values():
            continue

        # We only add for the channels we relay
        specific_ibc_tx_counter += 1

        # print(f"{source_channel=}, {destination_channel=}")
        # print(msg)
        # print(tx.tx_hash)
        # exit(1)

        if signer not in relayed_packets:
            relayed_packets[signer] = 1
        else:
            relayed_packets[signer] += 1


print("=======")

print(f"{all_ibc_txs=}")
print(f"{specific_ibc_tx_counter=}")

# sort relayed_packets values from high to low
relayed_packets = dict(
    sorted(relayed_packets.items(), key=lambda item: item[1], reverse=True)
)

for key, value in relayed_packets.items():
    print(key, value)
