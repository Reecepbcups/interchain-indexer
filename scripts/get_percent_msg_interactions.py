"""
This script gets all message types over a block period. Then saves a JSON file for each message type
with the total number of messages done. 
"""

import json
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current_dir)
sys.path.append(parent)

from SQL import Database

db = Database(os.path.join(current_dir, os.path.join(parent, "data.db")))

latest_block = db.get_latest_saved_block()
if latest_block is None:
    print("No blocks found in db")
    exit(1)


START_BLOCK = 1
# START_BLOCK = latest_block.height - 100_000  # ump all_interactions to file
END_BLOCK = latest_block.height

print(f"Getting all transactions in range of blocks: {START_BLOCK} to {END_BLOCK}")
all_txs = db.get_txs_in_range(START_BLOCK, END_BLOCK)
print(f"Total Txs found: {len(all_txs):,}")

# msg_type: amount
all_interactions: dict[str, int] = {}
for tx in all_txs:
    if len(tx.msg_types) == 0:
        continue

    _json = json.loads(tx.tx_json)
    for msg in _json["body"]["messages"]:
        if msg["@type"] not in all_interactions:
            all_interactions[msg["@type"]] = 0
        all_interactions[msg["@type"]] += 1

all_interactions = dict(
    sorted(all_interactions.items(), key=lambda item: item[1], reverse=True)
)

percent_of_total_msgs = {}
for msg_type, amount in all_interactions.items():
    percent_of_total_msgs[msg_type] = round(
        amount / sum(all_interactions.values()) * 100, 4
    )

filename = f"all_interactions-{START_BLOCK}_{END_BLOCK}.json"
with open(os.path.join(current_dir, filename), "w") as f:
    json.dump(
        {
            "start_block": START_BLOCK,
            "end_block": END_BLOCK,
            "total_msgs_amount": sum(all_interactions.values()),
            "interactions": all_interactions,
            "percents": percent_of_total_msgs,
        },
        f,
        indent=4,
    )
