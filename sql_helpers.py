# reads the SQLite file and shows you how to perform actions

import os

from SQL import Database

current_dir = os.path.dirname(os.path.realpath(__file__))

def main():
    db = Database(os.path.join(current_dir, "data.db"))

    total = db.get_total_blocks()
    print(f"Total Blocks: {total}")    

    earliest_block = db.get_earliest_block_height()
    print(f"Earliest Block: {earliest_block}")

    latest_height = db.get_latest_saved_block_height()
    print(f"Latest Block Height: {latest_height}")
    
    # count = db.get_all_message_count_over_range(earliest_block, latest_height)
    # print(f"Total Messages: {sum(count):,}")

    # type_count = db.get_type_count_over_range("/cosmwasm.wasm.v1.MsgExecuteContract", earliest_block, latest_height)    
    # print(f"Total ExecuteContract: {sum(type_count):,}")

    # values = db.get_types_at_height_over_range("/cosmwasm.wasm.v1.MsgExecuteContract", earliest_block, latest_height)
    # print(len(values))\

    txs = db.get_types_transaction_ids("/cosmwasm.wasm.v1.MsgExecuteContract", earliest_block, earliest_block+10)
    print(txs)

    # TODO: Get which heights blocks are at

    # # get the transactions at this height
    # txs = db.get_block_txs(latest_height)
    # print(f"Transactions at height {latest_height}: {txs}")
    # # show the first tx in the txs list
    # tx = db.get_tx(txs[0])
    # print(f"First Transaction: {tx}")


if __name__ == "__main__":
    main()    