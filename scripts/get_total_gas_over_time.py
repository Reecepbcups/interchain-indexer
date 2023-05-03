"""
Gets gas and fees est. over time

For: https://twitter.com/luisqagt/status/1653510347322531843?s=20
"""
import json
import os
import sys

from attr import dataclass

current_dir = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current_dir)
sys.path.append(parent)

from SQL import Database

db = Database(os.path.join(current_dir, os.path.join(parent, "data.db")))
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


last_tx_saved = db.get_last_saved_tx()
if last_tx_saved is None:
    print("No txs found in db")
    exit(1)
print(f"{last_tx_saved.id=}")


seconds_in_a_day = 86_400
blocks_in_a_week = (seconds_in_a_day * 7) / 6


@dataclass
class Fee:
    denom: str
    amount: int

    def toJson(self):
        return {"denom": self.denom, "amount": self.amount}


# weekend_height_key: [{"ujuno": 10000}]
total_fees_paid: dict[int, dict[str, int]] = {}
total_ujuno_fees_paid_lifetime = 0

# height where its based off the week
total_fees_paid = {k: {} for k in range(1, 30_000_000, int(blocks_in_a_week))}
print(total_fees_paid)


def find_closest_key(target_key):
    closest_key = min(total_fees_paid.keys(), key=lambda x: abs(x - target_key))
    return closest_key


# for i in range(1, last_tx_saved.id):
for i in range(1, last_tx_saved.id):
    tx = db.get_tx_specific(i, fields=["id", "height", "tx_json"])
    if tx is None:
        continue

    if len(tx.tx_json) == 0:
        # print(f"Tx {i:,} has no tx_json (not decoded)")
        continue

    height = tx.height
    tx_json = json.loads(tx.tx_json)

    if tx.id % 50_000 == 0:
        # print(f"Tx {i:,}")
        print(f"TxId:{tx.id}")

    fees = tx_json["auth_info"]["fee"]["amount"]

    # get the closes key value from total_fees_paid to height
    closest_key = find_closest_key(height)

    if closest_key not in total_fees_paid:
        total_fees_paid[closest_key] = {}

    tmp_height = total_fees_paid[closest_key]
    for fee in fees:
        denom = fee["denom"]
        amount = int(fee["amount"])

        if amount == 0:
            continue

        if denom == "ujuno":
            total_ujuno_fees_paid_lifetime += amount

        if denom not in tmp_height:
            tmp_height[denom] = 0

        tmp_height[denom] += amount

    total_fees_paid[closest_key] = tmp_height


# print(last_tx_saved)


total_fees_paid = {k: v for k, v in total_fees_paid.items() if v != {}}
with open(os.path.join(current_dir, "all_fees_over_time.json"), "w") as f:
    json.dump(
        {
            "total_ujuno_fees": total_ujuno_fees_paid_lifetime,
            "weekly_fees": total_fees_paid,
        },
        f,
        indent=4,
    )


# TxId:20550000|Height:7878542|{'ujuno': 45258021359, 'ibc/C4CFF46FD6DE35CA4CF4CE031E643C8FDC9BA4B99AE598E9B0ED98FE3A2319F9': 2874174, 'ibc/EAC38D55372F38F1AFD68DF7FE9EF762DCF69F26520643CF3F9D292A738D8034': 500}
