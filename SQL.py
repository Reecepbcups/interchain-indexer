import json
import sqlite3
import time

from chain_types import Block, Tx
from util import txraw_to_hash


class Database:
    def __init__(self, db: str):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        # self.optimize_db(vacuum=False) # never run vacuum here
        # self.cur.execute("""PRAGMA temp_store=MEMORY""")
        # self.optimize_tables()

    def commit(self):
        self.conn.commit()

    def create_tables(self):
        self.cur.execute(
            # TODO: msg_types json array should have an auto generated index fropm the `json`->> thing with a join or something?
            """CREATE TABLE IF NOT EXISTS blocks (height serial PRIMARY KEY, date DATETIME NOT NULL, txs json, msg_types json)"""
        )

        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS transactions (id serial PRIMARY KEY, height INT, tx_json json, address VARCHAR(50), tx_hash VARCHAR(64))"""
        )

        # tx_amino table stores: tx_amino BLOB with the tx_id of the parent transaction (forign key)
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS amino_tx (tx_id INT PRIMARY KEY, tx_amino BLOB, FOREIGN KEY(tx_id) REFERENCES transactions(id))"""
        )

        # store json blob off as well?

        self.commit()

    def optimize_tables(self):
        # Blocks
        self.cur.execute(
            """CREATE INDEX IF NOT EXISTS blocks_height ON blocks (height)"""
        )
        self.cur.execute("""CREATE INDEX IF NOT EXISTS blocks_date ON blocks (date)""")

        # Transactions
        self.cur.execute(
            """CREATE INDEX IF NOT EXISTS transactions_height ON transactions (height)"""
        )
        self.cur.execute(
            """CREATE INDEX IF NOT EXISTS transactions_tx_hash ON transactions (tx_hash)"""
        )
        self.cur.execute(
            """CREATE INDEX IF NOT EXISTS transactions_address ON transactions (address)"""
        )

        # Tx Amino
        # index tx_amino_bytes table by tx_hash? (not sure this is really needed though, future possibly.)

        self.commit()

    def optimize_db(self, vacuum: bool = False):
        # self.optimize_tables()
        # Set journal mode to WAL. - https://charlesleifer.com/blog/going-fast-with-sqlite-and-python/
        self.cur.execute("""PRAGMA journal_mode=WAL""")
        # self.cur.execute("""PRAGMA synchronous=OFF""") # off? or normal?
        self.cur.execute("""PRAGMA mmap_size=30000000000""")
        self.cur.execute(f"""PRAGMA page_size=32768""")
        if vacuum:
            self.cur.execute("""VACUUM""")
            self.cur.execute("""PRAGMA optimize""")
        self.commit()

    def get_indexes(self):
        self.cur.execute("""SELECT name FROM sqlite_master WHERE type='index';""")
        return self.cur.fetchall()

    def get_all_tables(self):
        self.cur.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
        return self.cur.fetchall()

    def get_table_schema(self, table: str):
        self.cur.execute(f"""PRAGMA table_info({table})""")
        return self.cur.fetchall()

    # ===================================
    # Blocks
    # ===================================

    def insert_block(self, height: int, time: str, txs_ids: list[int]):
        # insert the height and tx_amino.
        self.cur.execute(
            """INSERT INTO blocks (height, time, txs) VALUES (?, ?, ?)""",
            (height, time, json.dumps(txs_ids)),
        )

    def get_block(self, block_height: int) -> Block | None:
        self.cur.execute(
            """SELECT * FROM blocks WHERE height=?""",
            (block_height,),
        )
        data = self.cur.fetchone()
        if data is None:
            return None

        return Block(data[0], data[1], json.loads(data[2]))

    def get_earliest_block(self) -> Block | None:
        self.cur.execute("""SELECT * FROM blocks ORDER BY height ASC LIMIT 1""")
        data = self.cur.fetchone()
        if data is None:
            return None

        return Block(data[0], data[1], json.loads(data[2]))

    def get_latest_saved_block(self) -> Block | None:
        self.cur.execute("""SELECT * FROM blocks ORDER BY height DESC LIMIT 1""")
        data = self.cur.fetchone()
        if data is None:
            return None

        return Block(data[0], data[1], json.loads(data[2]))

    def get_total_blocks(self) -> int:
        self.cur.execute("""SELECT COUNT(*) FROM blocks""")
        data = self.cur.fetchone()
        if data is None:
            return 0
        return data[0]

    def get_missing_blocks(self, start_height, end_height) -> list[int]:
        # get all blocks which we do not have value for between a range
        self.cur.execute(
            """SELECT height FROM blocks WHERE height BETWEEN ? AND ?""",
            (start_height, end_height),
        )
        data = self.cur.fetchall()
        if data is None:
            return list(range(start_height, end_height + 1))

        found_heights = set(x[0] for x in data)
        missing_heights = [
            height
            for height in range(start_height, end_height + 1)
            if height not in found_heights
        ]
        return missing_heights

    # ===================================
    # Transactions
    # ===================================

    def insert_tx(self, height: int, tx_amino: str):
        # We insert the data without it being decoded. We can update later
        # insert the height and tx_amino, then return the unique id
        # fill the other collums with empty strings
        # """CREATE TABLE IF NOT EXISTS txs (id INTEGER PRIMARY KEY AUTOINCREMENT, height INTEGER, tx_amino TEXT, msg_types TEXT, tx_json TEXT, address TEXT)"""

        tx_hash = txraw_to_hash(tx_amino)
        self.cur.execute(
            """INSERT INTO txs (height, tx_amino, msg_types, tx_json, address, tx_hash) VALUES (?, ?, ?, ?, ?, ?)""",
            (height, tx_amino, "", "", "", tx_hash),
        )
        return self.cur.lastrowid

    def update_tx(self, _id: int, tx_json: str, msg_types: str, address: str):
        # update the data after we decode it (post insert_tx)
        self.cur.execute(
            """UPDATE txs SET tx_json=?, msg_types=?, address=? WHERE id=?""",
            (tx_json, msg_types, address, _id),
        )

    def update_tx_hash(self, _id: int, tx_hash: str):
        # This is only used for the migration to add this section.
        self.cur.execute(
            """UPDATE txs SET tx_hash=? WHERE id=?""",
            (tx_hash, _id),
        )

    def get_tx_by_hash(self, tx_hash: str) -> Tx | None:
        self.cur.execute(
            """SELECT id FROM txs WHERE tx_hash=?""",
            (tx_hash,),
        )
        data = self.cur.fetchone()
        if data is None:
            return None

        return self.get_tx(data[0])

    def get_tx(self, tx_id: int) -> Tx | None:
        self.cur.execute(
            """SELECT * FROM txs WHERE id=?""",
            (tx_id,),
        )
        data = self.cur.fetchone()
        if data is None:
            return None

        return Tx(data[0], data[1], data[2], data[3], data[4], data[5], data[6] or "")

    def get_tx_specific(self, tx_id: int, fields: list[str]):
        self.cur.execute(
            f"""SELECT {','.join(fields)} FROM txs WHERE id=?""",
            (tx_id,),
        )
        data = self.cur.fetchone()
        if data is None:
            return None

        # save fields in a dict
        tx = {}
        for i in range(len(fields)):
            tx[fields[i]] = data[i]

        # fill in the missing fields with empty strings
        for tx_type in Tx.__annotations__.keys():
            if tx_type not in tx:
                tx[tx_type] = ""

        return Tx(**tx)

    def get_txs_from_address_in_range(self, address: str) -> list[dict]:
        txs: list[dict] = []

        print("Starting to wait...")
        # get just height,tx_json from txs where the address = address
        self.cur.execute(
            """SELECT height FROM txs WHERE address=?""",
            (address,),
        )

        data = self.cur.fetchall()

        print("Done...")

        if data is None:
            print("No data")
            return txs

        print("For Loop")
        for tx in data:
            txs.append({"height": tx[0], "tx_json": tx[1]})

        print("Return")
        return txs

    def get_txs_by_ids(self, tx_lower_id: int, tx_upper_id: int) -> list[Tx]:
        txs: list[Tx] = []

        if tx_lower_id == tx_upper_id or tx_lower_id > tx_upper_id:
            print("error, tx_lower_id == tx_upper_id or tx_lower_id > tx_upper_id")
            return txs

        self.cur.execute(
            """SELECT * FROM txs WHERE id BETWEEN ? AND ?""",
            (tx_lower_id, tx_upper_id),
        )
        data = self.cur.fetchall()
        if data is None:
            return txs

        for tx in data:
            txs.append(Tx(tx[0], tx[1], tx[2], tx[3], tx[4], tx[5], tx[6] or ""))

        return txs

    def get_last_saved_tx(self) -> Tx | None:
        self.cur.execute("""SELECT id FROM txs ORDER BY id DESC LIMIT 1""")
        data = self.cur.fetchone()
        if data is None:
            return None

        return self.get_tx(data[0])

    # Rename this to _in_block_range. As a user could also _in_id_range
    def get_txs_in_range(self, start_height: int, end_height: int) -> list[Tx]:
        start = time.time()

        tx_ids = {}
        # print("Getting txs ids from blocks")
        for block_index in range(start_height, end_height + 1):
            b = self.get_block(block_index)
            if b:
                for tx_id in b.tx_ids:
                    tx_ids[tx_id] = True

        txs: list[Tx] = []
        # print("Getting Txs from found tx_ids")
        for tx_id in tx_ids:
            _tx = self.get_tx(tx_id)
            if _tx:
                txs.append(_tx)

        # This is ~3.5x slower than above
        # self.cur.execute(
        #     """SELECT * FROM txs WHERE height BETWEEN ? AND ?""",
        #     (start_height, end_height),
        # )
        # data = self.cur.fetchall()
        # if data is None:
        #     return []
        # txs: list[Tx] = []
        # for x in data:
        #     txs.append(Tx(x[0], x[1], x[2], x[3], x[4], x[5]))

        # print(f"Got {len(txs)} txs in {time.time() - start} seconds")

        return txs

    def get_non_decoded_txs_in_range(
        self, start_height: int, end_height: int
    ) -> list[Tx]:
        # returns all txs which have not been decoded in the json field. This field is "" if not decoded
        self.cur.execute(
            """SELECT * FROM txs WHERE height BETWEEN ? AND ?""",
            (start_height, end_height),
        )
        data = self.cur.fetchall()
        if data is None:
            return []

        txs: list[Tx] = []
        for x in data:
            # check if tx_json is "", if so, add it to the array
            if len(x[4]) == 0:
                txs.append(Tx(x[0], x[1], x[2], x[3], x[4], x[5], x[6]))

        return txs

    # def get_users_txs_in_range(
    #     self, address: str, start_height: int, end_height: int
    # ) -> list[Tx]:
    #     latest_block = self.get_latest_saved_block()
    #     if latest_block is None:
    #         print("No (latest) blocks saved in Database")
    #         return []

    #     if end_height > latest_block.height:
    #         end_height = latest_block.height

    #     print(address, start_height, end_height)

    #     # Select all tx_id from users WHERE address=? AND height BETWEEN ? AND ?
    #     self.cur.execute(
    #         """SELECT tx_id FROM users WHERE address=? AND height BETWEEN ? AND ?""",
    #         (address, start_height, end_height),
    #     )
    #     data = self.cur.fetchall()

    #     if data is None:
    #         return []

    #     txs = []
    #     for values in data:
    #         tx = self.get_tx(values[2])
    #         if tx is not None:
    #             txs.append(tx)
    #     return txs
