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
        # height, time, txs_ids
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, time TEXT, txs TEXT)"""
        )

        # txs: id int primary key auto inc, height, tx_amino, msg_types, tx_json
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS txs (id INTEGER PRIMARY KEY AUTOINCREMENT, height INTEGER, tx_amino TEXT, msg_types TEXT, tx_json TEXT, address TEXT, tx_hash TEXT)"""
        )

        # users: address, height, tx_id
        # self.cur.execute(
        #     """CREATE TABLE IF NOT EXISTS users (address TEXT, height INTEGER, tx_id INTEGER)"""
        # )

        # messages: message_type, height, count
        # This may be extra? We could just iter txs but I guess it depends. Can add in the future
        # NOTE: Getting just the count is dumb. If we were to redo this it should be the Tx IDs
        # self.cur.execute(
        #     # This was updated in the migrations.txt on current db
        #     """CREATE TABLE IF NOT EXISTS messages (message TEXT, height INTEGER, count INTEGER)"""
        # )

        self.commit()

    def optimize_tables(self):
        # Only runs this after we have saved values
        # self.cur.execute("""VACUUM""")
        # NOTE: If you need to add a new index, you must drop then create a new index.

        # Blocks indexes. Maybe index Tx ids? Not sure how that will work with being an array.
        self.cur.execute(
            """CREATE INDEX IF NOT EXISTS blocks_height ON blocks (height)"""
        )

        # Transactions
        self.cur.execute(
            """CREATE INDEX IF NOT EXISTS txs_data_index ON txs (id, height, address, tx_hash)"""
        )

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
    # User
    # ===================================
    # def insert_user(self, address: str, height: int, tx_id: int):
    #     self.cur.execute(
    #         """INSERT INTO users (address, height, tx_id) VALUES (?, ?, ?)""",
    #         (address, height, tx_id),
    #     )

    # ===================================
    # Messages / Types (May not be needed)
    # ===================================
    '''
    def insert_msg_type(self, message_type: str, height: int, count: int):
        self.cur.execute(
            """INSERT INTO messages (message, height, count) VALUES (?, ?, ?)""",
            (message_type, height, count),
        )

    def insert_msg_type_count(self, msg_type: str, count: int, height: int):
        self.cur.execute(
            """SELECT count FROM messages WHERE message=? AND height=?""",
            (height, msg_type),
        )
        data = self.cur.fetchone()
        if data is not None:
            print(f"Block {height} already has {msg_type}")
            return

        self.cur.execute(
            """INSERT INTO messages (message, height, count) VALUES (?, ?, ?)""",
            (msg_type, height, count),
        )

    def get_msg_type_count_at_exact_height(self, msg_type: str, height: int) -> int:
        self.cur.execute(
            """SELECT count FROM messages WHERE message=? AND height=?""",
            (msg_type, height),
        )
        data = self.cur.fetchone()
        if data is None:
            return 0
        return data[0]

    def get_msg_type_count_in_range(self, msg_type: str, start: int, end: int) -> int:
        """
        If msg_type is '*', counts all messages
        Returns a list of @ of Txs per block in the range requested

        Ex: Blocks 10 through 20
        Returns a list of 10 items, each item being the # of txs total
        """

        if msg_type == "*" or msg_type == "" or msg_type is None:
            self.cur.execute(
                """SELECT count FROM messages WHERE height>=? AND height<=?""",
                (start, end),
            )
        else:
            self.cur.execute(
                """SELECT count FROM messages WHERE message=? AND height>=? AND height<=?""",
                (msg_type, start, end),
            )

        data = self.cur.fetchall()
        if data is None:
            return []

        return sum([x[0] for x in data])

    def get_msg_types_transactions_in_range(
        self, msg_type: str, start: int, end: int
    ) -> list[Tx]:
        """
        Returns a list of tx_ids that have the msg_type in the range requested
        """    
        self.cur.execute(
            """SELECT * FROM txs WHERE msg_types LIKE ? AND height>=? AND height<=?""",
            (f"%{msg_type}%", start, end),
        )
        data = self.cur.fetchall()
        if data is None:
            return []
        
        blankTx = Tx(0, 0, "", [], "", "")

        txs: list[Tx] = []
        for tx in data:
            blankTx.id = tx[0]
            blankTx.height = tx[1]
            blankTx.tx_amino = tx[2]
            blankTx.msg_types = json.loads(tx[3])
            blankTx.tx_json = tx[4]
            blankTx.address = tx[5]
            txs.append(blankTx)    

        return txs

    def get_msg_types_ids_in_range(
        self, msg_type: str, start: int, end: int
    ) -> list[int]:
        """
        Returns a list of tx_ids that have the msg_type in the range requested
        """
        txs: list[int] = []

        self.cur.execute(
            """SELECT id FROM txs WHERE msg_types LIKE ? AND height>=? AND height<=?""",
            (f"%{msg_type}%", start, end),
        )
        data = self.cur.fetchall()
        if data is None:
            return []

        for tx in data:
            txs.append(tx[0])

        return list(txs)
    '''

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
