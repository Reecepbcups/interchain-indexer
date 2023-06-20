import os
import sys
import time

"""
The original table did not have tx_hash. This script adds that.


SQLite cmds:

sqlite3 data.db
SELECT * from txs LIMIT 1;
BEGIN;
ALTER TABLE txs ADD COLUMN tx_hash TEXT;
COMMIT;
# ROLLBACK;
.exit

"""

current_dir = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current_dir)
sys.path.append(parent)

from option_types import BlockOption, TxOptions, TxQueryOption
from SQL import Database

db = Database(os.path.join(current_dir, os.path.join(parent, "data.db")))


def main():
    last_block = db.get_block(-1, BlockOption.LATEST)
    print(f"\n{last_block=}")

    # last_tx = db.get_tx(last_block.tx_ids[0])
    # print(last_tx.id)

    last_tx = db.get_tx(TxQueryOption.LATEST)
    print(f"Last Tx ID: {last_tx.id}")

    # Gets and address and prints some stats
    print("=== Addr Checker ===")
    res = db.get_txs_by_address(
        "juno1rkhrfuq7k2k68k0hctrmv8efyxul6tgn8hny6y",
        options=[
            TxOptions.ADDRESS,
            TxOptions.TX_HASH,
            TxOptions.ID,
            TxOptions.HEIGHT,
            TxOptions.MSG_TYPES,
        ],
    )
    print("Addr Tx Stats", len(res))
    print("Message types", set(tx.msg_types for tx in res))

    print("Message types", res[0])

    last_tx_by_hash = db.get_tx_by_hash(last_tx.tx_hash)
    print(last_tx_by_hash.id == last_tx.id)

    pass


if __name__ == "__main__":
    main()
