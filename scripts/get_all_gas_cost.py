import json
import os
import sys

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
# print(f"{last_tx_saved.id=}")

GAS_AMOUNT = 0
total_ujuno_fees = 0
total_txs = 0

# Gets last XXmil txs
for i in range(last_tx_saved.id-10_000_000, last_tx_saved.id):
    tx = db.get_tx_specific(i, fields=["id", "height", "tx_json"])
    if tx is None:
        continue    

    if i % 100_000 == 0:
        print(f"Tx {i:,}")

    if len(tx.tx_json) == 0:        
        continue
    
    height = tx.height
    tx_json = json.loads(tx.tx_json)
    fees = tx_json["auth_info"]["fee"]["amount"]
    # gas_amount = int(tx_json["auth_info"]["fee"]['gas_limit'])
    
    if "cosmwasm.wasm.v1.MsgExecuteContract" not in tx.tx_json:
        continue

    total_txs += 1    

    for coin in fees:
        if coin['denom'] == 'ujuno':            
            amt = int(coin['amount'])
            total_ujuno_fees += amt

print(f"{GAS_AMOUNT=:,} spent over {total_txs=:,} txs")
avg = int(GAS_AMOUNT / total_txs)
print(f"Average gas cost per tx: {avg=:,}")

print(f"{total_ujuno_fees=:,} total ujuno fees paid. = {int(total_ujuno_fees / 1_000_000):,}JUNO")
avg_fees = int(total_ujuno_fees / total_txs)
print(f"Average ujuno fees paid per tx: {avg_fees=:,}")
