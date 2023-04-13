import json
import sqlite3
from dataclasses import dataclass

'''

        def insert_type_count(self, msg_type: str, count: int, height: int):
        # NOTE: This needed? - check if height already has this height
        self.cur.execute(
            """SELECT height FROM messages WHERE height=? AND message=?""",
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
        # self.conn.commit()

    def insert_user(self, address: str, height: int, tx_id: int):
        self.cur.execute(
            """INSERT INTO users (address, height, tx_id) VALUES (?, ?, ?)""",
            (address, height, tx_id),
        )
        # self.conn.commit()

    def get_type_count_at_height(self, msg_type: str, height: int) -> int:
        self.cur.execute(
            """SELECT count FROM messages WHERE message=? AND height=?""",
            (msg_type, height),
        )
        data = self.cur.fetchone()
        if data is None:
            return 0
        return data[0]

    def get_msgs_over_range(self, msg_type: str, start: int, end: int) -> list[int]:
        """
        If msg_type is '*' or None or blank, counts all messages
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
        return [x[0] for x in data]

    def get_types_at_height_over_range(
        self, msg_type: str, start_height: int, end_height: int
    ) -> list[int]:
        # Retuns a list of every height said msg_type is found. Ex: [6700000, 6700001, 6700002, 6700005, 6700007, ...]
        self.cur.execute(
            """SELECT height FROM messages WHERE message=? AND height>=? AND height<=?""",
            (msg_type, start_height, end_height),
        )
        data = self.cur.fetchall()
        if data is None:
            return []

        return list(set([x[0] for x in data]))

def _get_transactions_Msg_Types(self, tx: dict) -> list[str]:
        if tx is None or tx == {}:
            return []

        messages = set()

        for msg in tx["body"]["messages"]:
            _type = msg["@type"]
            messages.add(_type)

        return list(messages)

    # If I saved it properly, would be a lot better
    def get_msg_ids_in_range(
        self, msg_type: str, start_height: int, end_height: int
    ) -> list[int]:
        # loop through all blocks in the range
        found_heights = self.get_types_at_height_over_range(
            msg_type, start_height, end_height
        )

        # get each one of those Txs from the database.
        tx_ids = set()
        for height in found_heights:
            # query height for all transactions
            self.cur.execute(
                """SELECT txs FROM blocks WHERE height=?""",
                (height,),
            )
            blocks_txs = self.cur.fetchone()
            if blocks_txs is None:
                continue

            blocks_txs = json.loads(blocks_txs[0])
            for tx_id in blocks_txs:
                # query what type of message this tx is
                self.cur.execute(
                    """SELECT tx FROM txs WHERE id=?""",
                    (tx_id,),
                )
                tx_data = self.cur.fetchone()
                if tx_data is None:
                    continue

                tx_data = json.loads(tx_data[0])
                # print(tx_data)
                msg_types = self._get_transactions_Msg_Types(tx_data)
                if msg_type in msg_types:
                    tx_ids.add(tx_id)

        res = list(tx_ids)
        res.sort()
        return res
'''

# create a dataclass for Block

@dataclass
class Block:
    height: int
    time: str
    tx_ids: list[int]

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

class Database:
    def __init__(self, db: str):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def create_tables(self):
        # Blocks: contains height and a list of integer ids
        # Txs: a list of unique integer ids and a string of the tx. Where Txs = a JSON array
        # Users a list of unique addresses and a list of integer ids

        # height, time, txs_ids
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, time TEXT, txs TEXT)"""
        )

        # txs: id int primary key auto inc, height, tx_amino, msg_types, tx_json
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS txs (id INTEGER PRIMARY KEY AUTOINCREMENT, height INTEGER, tx_amino TEXT, msg_types TEXT, tx_json TEXT)"""
        )

        # users: address, height, tx_id
        # messages: message_type, height, count
        

        self.commit()

    def get_all_tables(self):
        self.cur.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
        return self.cur.fetchall()

    def get_table_schema(self, table: str):
        self.cur.execute(f"""PRAGMA table_info({table})""")
        return self.cur.fetchall()

    def insert_tx(self, height: int, tx_amino: str):
        # We insert the data without it being decoded. We can update later 
        # insert the height and tx_amino, then return the unique id
        # fill the other collums with empty strings
        self.cur.execute(
            """INSERT INTO txs (height, tx_amino, msg_types, tx_json) VALUES (?, ?, ?, ?)""",
            (height, tx_amino, "", ""),
        )
        return self.cur.lastrowid
    
    # def update_tx(self, id: int, tx_json: str, msg_types: str):
    #     # update the data after we decode it (post insert_tx)
    #     self.cur.execute(
    #         """UPDATE txs SET tx_json=?, msg_types=? WHERE id=?""",
    #         (tx_json, msg_types, id),
    #     )        
    #     return self.cur.lastrowid

    def insert_block(self, height: int, time: str, txs_ids: list[int]):
        # insert the height and tx_amino, then return the unique id
        self.cur.execute(
            """INSERT INTO blocks (height, time, txs) VALUES (?, ?, ?)""",
            (height, time, json.dumps(txs_ids)),
        )
        return self.cur.lastrowid
    

    def get_earliest_block_height(self) -> int:
        self.cur.execute("""SELECT height FROM blocks ORDER BY height ASC LIMIT 1""")
        data = self.cur.fetchone()
        if data is None:
            return -1
        return data[0]

    def get_total_blocks(self) -> int:
        self.cur.execute("""SELECT COUNT(*) FROM blocks""")
        data = self.cur.fetchone()
        if data is None:
            return 0
        return data[0]
    
    def get_missing_blocks(self, start_height, end_height) -> list[int]:
        # get all blocks which we do not have value for between a range
        self.cur.execute(
            """SELECT height FROM blocks WHERE height>=? AND height<=?""",
            (start_height, end_height),
        )
        data = self.cur.fetchall()
        if data is None:
            return []
        
        found_heights = set(x[0] for x in data)
        missing_heights = [height for height in range(start_height, end_height+1) if height not in found_heights]
        return missing_heights
    
    def get_block(self, block_height: int) -> Block | None:
        self.cur.execute(
            """SELECT * FROM blocks WHERE height=?""",
            (block_height,),
        )
        data = self.cur.fetchone()        
        if data is None:
            return None
        # return {
        #     "height": data[0],
        #     "time": data[1],
        #     "txs": json.loads(data[2]),
        # }
        return Block(data[0], data[1], json.loads(data[2]))

    def get_latest_saved_block_height(self) -> int:
        self.cur.execute("""SELECT height FROM blocks ORDER BY height DESC LIMIT 1""")
        data = self.cur.fetchone()
        if data is None:
            return 0
        return data[0]

    def get_tx_amino(self, tx_id: int) -> str:
        self.cur.execute(
            """SELECT tx_amino FROM txs WHERE id=?""",
            (tx_id,),
        )
        data = self.cur.fetchone()
        if data is None:
            return ""
        return data[0]
    
    # get_tx_json ,_ need to write a mass decode script
    
    # TODO: Will process this after the fact
    # def get_user_tx_ids(self, address: str) -> list[int]:
    #     # get all Tx ids for a given address
    #     self.cur.execute(
    #         """SELECT tx_id FROM users WHERE address=?""",
    #         (address,),
    #     )
    #     data = self.cur.fetchall()
    #     if data is None:
    #         return []

    #     return [tx_id[0] for tx_id in data]

    # def get_user_txs(self, address: str) -> dict:
    #     tx_ids = self.get_user_tx_ids(address)
    #     txs = {}
    #     for tx_id in tx_ids:
    #         tx = self.get_tx(tx_id)
    #         txs[tx_id] = tx
    #     return txs

    # def get_all_accounts(self) -> list[str]:
    #     # return all accounts and the len of txs they have
    #     self.cur.execute(
    #         """SELECT address, COUNT(tx_id) FROM users GROUP BY address""",
    #     )
    #     data = self.cur.fetchall()
    #     if data is None:
    #         return []
    #     return data
