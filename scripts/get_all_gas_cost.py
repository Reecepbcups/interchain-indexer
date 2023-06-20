import json
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current_dir)
sys.path.append(parent)

from base_script import current_dir, db, earliest_block, last_tx_saved, latest_block

from option_types import TxOptions

GAS_AMOUNT = 0
total_ujuno_fees = 0
total_txs = 0

# Gets last XXmil txs
for i in range(last_tx_saved.id - 10_000_000, last_tx_saved.id):
    tx = db.get_tx_by_id(i, options=[TxOptions.ID, TxOptions.HEIGHT, TxOptions.TX_JSON])
    if tx is None:
        continue

    if i % 100_000 == 0:
        print(f"Tx {i:,}")

    if len(tx.tx_json) == 0:
        continue

    height = tx.height
    tx_json = tx.tx_json
    fees = tx_json["auth_info"]["fee"]["amount"]
    # gas_amount = int(tx_json["auth_info"]["fee"]['gas_limit'])

    if "cosmwasm.wasm.v1.MsgExecuteContract" not in tx.tx_json:
        continue

    total_txs += 1

    for coin in fees:
        if coin["denom"] == "ujuno":
            amt = int(coin["amount"])
            total_ujuno_fees += amt

print(f"{GAS_AMOUNT=:,} spent over {total_txs=:,} txs")
avg = int(GAS_AMOUNT / total_txs)
print(f"Average gas cost per tx: {avg=:,}")

print(
    f"{total_ujuno_fees=:,} total ujuno fees paid. = {int(total_ujuno_fees / 1_000_000):,}JUNO"
)
avg_fees = int(total_ujuno_fees / total_txs)
print(f"Average ujuno fees paid per tx: {avg_fees=:,}")
