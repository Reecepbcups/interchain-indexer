"""
This gets all addresses who voted on proposal 284 (WYND/JUNO incentives) and 282 (v14 upgrade)
Then will compare these to num,ber of accounts with more than 0.01 JUNO balance &/or stake
"""

import json
import os

from SQL import Database

current_dir = os.path.dirname(os.path.realpath(__file__))


db = Database(os.path.join(current_dir, "data.db"))

latest_block = db.get_latest_saved_block()
if latest_block is None:
    print("No blocks found in db")
    exit(1)
print(latest_block)

# get all transactions in the last 11 days, height 7755721 to 7919651 (April 20th)
all_txs = db.get_txs_in_range(7755721, 7919651)
print(f"Total Txs: {len(all_txs):,}")


# address: vote
voters: dict[str, str] = {}
proposal_id = "284"

# go through all Txs and get where msg_types is a vote message. If a user revotes, it overrides their last one
for tx in all_txs:
    if len(tx.msg_types) == 0:
        continue

    # print(tx.msg_types)
    if "MsgVote" in tx.msg_types:
        _json = json.loads(tx.tx_json)
        for msg in _json["body"]["messages"]:
            # Add AUTHZ support?
            if msg["@type"] == "/cosmos.gov.v1beta1.MsgVote":
                if msg["proposal_id"] == proposal_id:
                    voters[msg["voter"]] = msg["option"]

# dump voters
# with open(f"voters_{proposal_id}.json", "w") as f:
print(f"Voters: {len(voters):,}")
with open(os.path.join(current_dir, f"voters_{proposal_id}.json"), "w") as f:
    json.dump(voters, f, indent=2)
