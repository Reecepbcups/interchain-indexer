# reads the SQLite file and shows you how to perform actions

import json
import os

from chain_types import Tx
from SQL import Database

current_dir = os.path.dirname(os.path.realpath(__file__))


def main():
    db = Database(os.path.join(current_dir, "data.db"))

    # Save missing to file

    total = db.get_total_blocks()
    earliest_block = db.get_earliest_block()
    latest_block = db.get_latest_saved_block()

    # print(f"Total Blocks: {total}")
    print(f"Earliest Block: {earliest_block.height}")
    print(f"Latest Block Height: {latest_block.height}")
    exit(1)

    # missing = db.get_missing_blocks(
    #     earliest_block.height, latest_block.height
    # )  # add 1 here to ensure it works and we actually miss blocks
    # print(f"Missing Blocks total: {len(missing)}")
    # with open("missing.json", "w") as f:
    #     json.dump(missing, f)

    # block = db.get_block(839000)
    # print(block)

    # for b in range(2_000_000, 2_100_000):
    #     block = db.get_block(b)
    #     if block is None:
    #         continue

    #     if len(block.tx_ids) > 0:
    #         print(f"\nBlock {block.height} has {len(block.tx_ids)} txs")
    #         print(f"Block {block.height} has {block.time} time")

    #         tx = db.get_tx(block.tx_ids[0])
    #         print(tx)
    #         exit(1)

    # b = db.get_block(7461644)
    # # print(b)

    # # # loop through b.tx_ids, get_tx, and see if sender is set
    # for tx_id in b.tx_ids:
    #     tx = db.get_tx(tx_id)
    #     if len(tx.tx_json) == 0:
    #         print(tx_id, tx, '\n\n')

    # exit(1)

    # tx = db.get_tx(block.tx_ids[-1])
    # tx = db.get_tx(18_668_234) # bank multi send - no sender. hmm
    tx = db.get_tx(17470871)  # bank multi send - no sender. hmm
    print(tx)
    exit(1)
    # print(tx.tx_json)

    # get total Txs
    # total_txs = db.get_txs_in_range(earliest_block.height, earliest_block.height+1_000)
    # print(f"Total Txs: {len(total_txs):,}")\

    # txs: list[Tx] = db.get_users_txs_in_range(
    #     "juno1nsnhn0y0vsjq8j70z6yfxp2xk4rsjrzmn04g4h",
    #     earliest_block.height,
    #     latest_block.height,
    # )
    # print(f"User txs len: {len(txs)}")

    exit(1)

    total: int = db.get_msg_type_count_in_range(
        "*", earliest_block.height, earliest_block.height + 1
    )
    print(f"Total Msgs: {total:,}")

    txs_list = db.get_msg_types_transactions_in_range(
        "/cosmwasm.wasm.v1.MsgExecuteContract",
        earliest_block.height + 1,
        earliest_block.height + 500,
    )
    print(f"Total ExecuteContract: {len(txs_list):,}")
    print(f"A Tx from this list. ID:{txs_list[0].id}, {txs_list[0].tx_json}")

    tx_ids = db.get_msg_types_ids_in_range(
        "/cosmwasm.wasm.v1.MsgExecuteContract",
        earliest_block.height + 1,
        earliest_block.height + 5,
    )
    print(tx_ids)

    block = db.get_block(earliest_block.height + 1)
    print(f"\nBlock {block.height} has {len(block.tx_ids)} txs")

    # # get the transactions at this height
    # tx_ids = latest_block.tx_ids
    # print(f"Latest block Tx IDs. Height: {latest_block.height}: {tx_ids}")
    # # show the first tx in the txs list
    tx = db.get_tx(block.tx_ids[0])
    print(f"Getting index 0 from block:")
    print(f"\tid:{tx.id}, amino:{tx.tx_amino[0:50]}...")
    print(f"\tid:{tx.id}, json:{tx.tx_json[0:200]}...")
    print(f"\tid:{tx.id}, msg_types:{tx.msg_types}")

    # get users txs
    txs: list[Tx] = db.get_users_txs_in_range(
        "juno1nsnhn0y0vsjq8j70z6yfxp2xk4rsjrzmn04g4h",
        earliest_block.height,
        latest_block.height,
    )
    print(f"User txs len: {len(txs)}")

    # Get a specific type of transaction over time
    specific_txs = db.get_msg_types_transactions_in_range(
        "/cosmwasm.wasm.v1.MsgExecuteContract",
        earliest_block.height,
        latest_block.height,
    )
    print(
        f"{specific_txs[0].id}, {specific_txs[0].height}, {specific_txs[0].msg_types}"
    )  # if using indexc 0, should be the first Tx indexed by earliest block height


if __name__ == "__main__":
    main()
