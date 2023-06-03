"""
Gets all Txs in a day. Will compare vs prices in that same time with coingecko.

Steps:
- iter all txs. Get the day it happened. Save to a map in format of: 2021-10-21
- The map should be <string: int> where int can be 0 or more.
- Export as json
"""

# TODO: move this to a base script

import json
import os

from base_script import DBInformation as scheme

days: dict[str, int] = {}

# iter blocks
for i in range(scheme.earliest_block.height, scheme.latest_block.height):
    block = scheme.database.get_block(i)
    if block is None:
        continue

    txs = len(block.tx_ids)
    date = block.time.split("T")[0]  # 2021-10-01T15:00:00Z -> 2021-10-01

    if date not in days:
        print(f"{date=}")
        days[date] = 0

    days[date] += txs

    # if i >= 50000:
    #     print(days)
    #     exit(1)

file_path = os.path.join(scheme.current_dir, "all_txs_per_day.json")
with open(file_path, "w") as f:
    json.dump(days, f, indent=4)
