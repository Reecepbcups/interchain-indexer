"""
This gets all Contract executes to find the most popular usage.
The Queries contracts to get their label for a human readable format.
"""

import asyncio
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

# get all transactions in the last 11 days, height 7755721 to 7919651 (April 20th)
# START_BLOCK = latest_block.height - 1_000_000
START_BLOCK = 1
END_BLOCK = latest_block.height
INTERACTION_CUTOFF = 100

print(f"Getting all transactions in range of blocks: {START_BLOCK} to {END_BLOCK}")
all_txs = db.get_txs_in_range(START_BLOCK, END_BLOCK)
print(f"Total Txs found: {len(all_txs):,}")

# contract_addr: amount
contracts: dict[str, int] = {}
for tx in all_txs:
    if len(tx.msg_types) == 0:
        continue

    if "MsgExecuteContract" not in tx.msg_types:
        continue

    _json = json.loads(tx.tx_json)
    for msg in _json["body"]["messages"]:
        # Add AUTHZ support?
        if msg["@type"] == "/cosmwasm.wasm.v1.MsgExecuteContract":
            if msg["contract"] not in contracts:
                contracts[msg["contract"]] = 0
            contracts[msg["contract"]] += 1


updated_contracts = {k: v for k, v in contracts.items() if v >= INTERACTION_CUTOFF}
print(
    f"Contracts amount after removing < {INTERACTION_CUTOFF}: {len(updated_contracts):,}"
)


async def main():
    print("Getting labels from contact_labels.json")
    labels: dict[str, str] = {}
    with open(os.path.join(current_dir, "contract_labels.json"), "r") as f:
        labels = json.load(f)["labels"]

    # contract_addr: {"amount": amount, "label": label}
    updated = {}
    for c_addr, amount in updated_contracts.items():
        label = ""
        if c_addr in labels.keys():
            label = labels[c_addr]
        updated[c_addr] = {"amount": amount, "label": label}

    file_name = os.path.join(
        current_dir, f"contracts_interactions-{START_BLOCK}_{END_BLOCK}.json"
    )

    updated = dict(
        sorted(updated.items(), key=lambda item: item[1]["amount"], reverse=True)
    )

    with open(
        file_name,
        "w",
    ) as f:
        json.dump(
            {"start_block": START_BLOCK, "end_block": END_BLOCK, "contracts": updated},
            f,
            indent=2,
        )


asyncio.run(main())
