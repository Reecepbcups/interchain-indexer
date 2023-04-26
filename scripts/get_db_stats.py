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

from SQL import Database

db = Database(os.path.join(current_dir, os.path.join(parent, "data.db")))


def main():
    last_block = db.get_latest_saved_block()
    print(f"\n{last_block=}")

    # last_tx = db.get_tx(last_block.tx_ids[0])
    # print(last_tx.id)

    # get total txs
    last_tx = db.get_last_saved_tx()
    print(f"Total Txs: {last_tx.id}")

    last_tx_by_hash = db.get_tx_by_hash(last_tx.tx_hash)
    res = last_tx_by_hash.id == last_tx.id
    print(res)

    pass


if __name__ == "__main__":
    main()
