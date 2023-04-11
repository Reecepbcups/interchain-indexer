import json
import os

from main import blocks, current_dir, data_dir, txs_dir, type_stats
from SQL import Database
from util import (
    decode_txs,
    get_block_txs,
    get_latest_chain_height,
    get_sender,
    remove_useless_data,
)

db = Database(os.path.join(current_dir, "data.db"))
# db.drop_all() # NO
db.create_tables()

# txs = os.listdir(txs_dir)
# # print(txs)

# for tx_id in txs:
#     print(tx_id)

#     with open(os.path.join(txs_dir, tx_id), "r") as f:
#         tx_data = json.load(f)

#     print(tx_id, tx_data)
#     # exit(1)

#     # This gives it a unique data id anyways
#     unique_id = db.insert_tx(tx_data)
#     print(unique_id)
#     exit(1)

"""
Loop through blocks, get the JSON TX Ids

Loop through each, save into database. Save the Unique ID for said block

Save this array to the blocks sqlite table at height
"""


def get_tx_from_json(tx_id: str | int) -> dict:
    with open(os.path.join(txs_dir, f"{tx_id}.json"), "r") as f:
        tx_data = json.load(f)
    return tx_data


# === This runs first so that way we can check and add Txs to users after we run through all block txs ===
# Really I could just do this later after indexing things? Use get_sender
# address : {tx_id: height}
# users_txs: dict[str, dict[int, int]] = {}  # not sure we need this?
# map_tx_to_user_directly: dict[int, str] = {}
# for user_file in os.listdir(users):
#     address = user_file.replace(".json", "")  # juno1...
#     if address not in users_txs:
#         users_txs[address] = {}
#     with open(os.path.join(users, user_file), "r") as f:
#         # {"6000002": 105555108552904983053324984892829352677, "6000018": 102562604531958410843295514722050323844, "6000046": 270535999102497551913438323026460662628}
#         user_data = dict(json.load(f))
#         for height, json_tx_id in user_data.items():
#             users_txs[address][json_tx_id] = height
#             map_tx_to_user_directly[json_tx_id] = address
# print(map_tx_to_user_directly)
# print(users_txs)
# exit(1)

for block_file in os.listdir(blocks):
    height = int(block_file.replace(".json", ""))

    # Get blocks Temp Txs
    with open(os.path.join(blocks, block_file), "r") as f:
        block_txs = json.load(f)

    txs_ids: list[int] = []
    for tx in block_txs:
        with open(os.path.join(txs_dir, f"{tx}.json"), "r") as f:
            tx_data = json.load(f)

            # insert this into the database
            unique_id = db.insert_tx(tx_data)
            txs_ids.append(unique_id)

    # insert the block into the database
    print("height", height)
    db.insert_block(height, txs_ids)

    # Msg Types
    with open(os.path.join(type_stats, f"{height}.json"), "r") as f:
        _msgtypes = dict(json.load(f))
        for msg_type, count in _msgtypes.items():
            db.insert_type_count(msg_type, count, height)

    db.commit()


if True:
    get_txs_from_sql = db.get_block_txs(6000018)
    print(get_txs_from_sql)
    if get_txs_from_sql is None or len(get_txs_from_sql) == 0:
        print("No txs found")
        exit(1)
    get_tx = db.get_tx(get_txs_from_sql[0])
    print(get_tx)


# We will delete the files later :)
#     # delete the file
#     if True:
#         os.remove(os.path.join(blocks, block_file))
#         os.remove(os.path.join(type_stats, block_file))
#         # remove the tx files
#         for tx in block_txs:
#             os.remove(os.path.join(txs_dir, f"{tx}.json"))
