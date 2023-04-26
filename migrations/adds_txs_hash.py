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
from util import txraw_to_hash

db = Database(os.path.join(current_dir, os.path.join(parent, "data.db")))
# last_tx = db.get_last_saved_tx()
# print(last_tx)

# debugging
# last_block = db.get_latest_saved_block()
# print(last_block)
# tx = db.get_tx(20674282)
# print(tx)
# exit(1)


def main():
    # tx_hash_update_all()
    # check_for_missing_tx_hashes()
    pass


def tx_hash_update_all():
    start = time.time()
    for idx in range(1, 20598252 + 1):
        tx = db.get_tx(idx)
        if tx is None:
            continue

        # already processes
        if len(tx.tx_hash) > 0:
            continue

        # if index is a multiple of 10,000, print it
        if idx % 25_000 == 0:
            print(f"{idx:,}", time.time() - start)
            db.commit()

        tx_hash = txraw_to_hash(tx.tx_amino)
        db.update_tx_hash(tx.id, tx_hash)

    end = time.time()
    print(end - start)
    db.commit()


def check_for_missing_tx_hashes():
    # iterate over all and get any ids if any are missing the tx_hash
    for i in range(1, 20598252 + 1):
        tx = db.get_tx(i)
        if tx is None:
            continue

        if i % 25_000 == 0:
            print(f"{i:,}")

        if len(tx.tx_hash) == 0:
            print(i)
            break


if __name__ == "__main__":
    main()
