import json
import os
import sys
from dataclasses import dataclass
from typing import Optional

# Used to import DBInformation into other scripts.

current_dir = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current_dir)
sys.path.append(parent)

from option_types import BlockOption, TxOptions, TxQueryOption
from SQL import Block, Database, Tx

db = Database(os.path.join(current_dir, os.path.join(parent, "data.db")))
if db is None:
    print("No db found")
    exit(1)

earliest_block = db.get_block(-1, BlockOption.EARLIEST)
if earliest_block is None:
    print("No blocks found in db")
    exit(1)

latest_block = db.get_block(-1, BlockOption.LATEST)
if latest_block is None:
    print("No blocks found in db")
    exit(1)


last_tx_saved = db.get_tx(TxQueryOption.LATEST)
if last_tx_saved is None:
    print("No txs found in db")
    exit(1)
# print(f"{last_tx_saved.id=}")


@dataclass
class DBInformation:
    current_dir: str = current_dir
    parent: str = parent

    database: Database = db

    earliest_block: Block = earliest_block
    latest_block: Block = latest_block

    last_tx_saved: Tx = last_tx_saved


if __name__ == "__main__":
    print(earliest_block)
    print(latest_block)
    print(last_tx_saved)
