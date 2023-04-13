# reads the SQLite file and shows you how to perform actions

import json
import os

from SQL import Database

current_dir = os.path.dirname(os.path.realpath(__file__))

def main():
    db = Database(os.path.join(current_dir, "data.db"))

    # Save missing to file
    # missing = db.get_missing_blocks(7_750_000, 7_750_900)
    # print('missing ' , missing)
    # with open("missing.json", "w") as f:        
    #     f.write(json.dumps(missing))

    # total = db.get_total_blocks()
    # print(f"Total Blocks: {total}")

    # earliest_block = db.get_earliest_block()
    # print(f"Earliest Block: {earliest_block.height}")

    latest_block = db.get_latest_saved_block()
    print(f"Latest Block Height: {latest_block.height}")

    # total = db.get_msgs_over_range("*", earliest_block, latest_height)
    # print(f"Total Msgs: {sum(total):,}")

    # type_count = db.get_msgs_over_range(
    #     "/cosmwasm.wasm.v1.MsgExecuteContract", earliest_block, latest_height
    # )
    # print(f"Total ExecuteContract: {sum(type_count):,}")

    # values = db.get_types_at_height_over_range("/cosmwasm.wasm.v1.MsgExecuteContract", earliest_block, latest_height)
    # print(len(values))\

    # txs = db.get_msg_ids_in_range(
    #     "/cosmwasm.wasm.v1.MsgExecuteContract", earliest_block, earliest_block + 1000
    # )
    # print(txs)

    # TODO: Get which heights blocks are at

    # # get the transactions at this height
    tx_ids = latest_block.tx_ids       
    print(f"Latest block Tx IDs. Height: {latest_block.height}: {tx_ids}")
    # # show the first tx in the txs list
    tx = db.get_tx(tx_ids[0])
    print(f"First Transaction: id:{tx.id}, amino:{tx.tx_amino}")


if __name__ == "__main__":
    main()
