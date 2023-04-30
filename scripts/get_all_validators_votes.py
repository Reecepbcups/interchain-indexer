"""
Depends: get_all_validators.py

Reason: For delegation subdao to compare votes between vals for scoring

Task: 
- Iterate through all votes
- check if a validator voted (Vote or weighted vote). 

If they did
- add their vote to a list.
"""

import json
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current_dir)
sys.path.append(parent)

from SQL import Database

# 5779678 -> 7990650
db = Database(os.path.join(current_dir, os.path.join(parent, "data.db")))
if db is None:
    print("No db found")
    exit(1)


with open(os.path.join(current_dir, "all_validators.json"), "r") as f:
    all_validators = dict(json.load(f))

# For delegations subdao
START_BLOCK = 5779678
END_BLOCK = 7990650

# just ended voting period at start block
IGNORE_PROPOSAL_IDS = ["54"]

all_txs = db.get_txs_in_range(START_BLOCK, END_BLOCK)


all_proposals_during_time: list[int] = []

# valaddr: [proposal_1, proposal_3, ...]
validator_voters: dict[str, list[int]] = {}


for tx in all_txs:
    if len(tx.msg_types) == 0:
        continue

    # print(tx.msg_types)
    if "MsgVote" in tx.msg_types:
        _json = json.loads(tx.tx_json)
        for msg in _json["body"]["messages"]:
            if msg["@type"] in [
                "/cosmos.gov.v1beta1.MsgVote",
                "/cosmos.gov.v1beta1.MsgVoteWeighted",
            ]:
                voter = msg["voter"]
                proposal_id = msg["proposal_id"]

                if proposal_id in IGNORE_PROPOSAL_IDS:
                    continue

                if voter not in all_validators.keys():
                    continue

                if voter not in validator_voters.keys():
                    validator_voters[voter] = []

                if proposal_id not in all_proposals_during_time:
                    all_proposals_during_time.append(int(proposal_id))

                # ensure proposal_id is not already in list
                if proposal_id not in validator_voters[voter]:
                    validator_voters[voter].append(proposal_id)

# dump voters
print(f"Validator voters: {len(validator_voters):,}")
print(f"Total Proposals: {len(all_proposals_during_time):,}")

output: dict = validator_voters.copy()
# get length of each list and compare to all_proposals_during_time as a percent
for val, proposals in validator_voters.items():
    output[val] = {
        "name": all_validators[val]["name"],
        "val_addr": all_validators[val]["val_addr"],
        "voted_amt": len(proposals),
        # "voted_on": proposals,
        "percent": round((len(proposals) / len(all_proposals_during_time)) * 100, 2),
    }

all_proposals_during_time = sorted(all_proposals_during_time)

with open(
    os.path.join(
        current_dir, f"all_validator_voters_range_{START_BLOCK}-{END_BLOCK}.json"
    ),
    "w",
) as f:
    json.dump(
        {
            "proposals_during_time": all_proposals_during_time,
            "proposals_amount": len(list(all_proposals_during_time)),
            "validators": output,
        },
        f,
        indent=2,
    )
