import json
import sqlite3


class Database:
    def __init__(self, db: str):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()

    def drop_all(self):
        self.cur.execute("""DROP TABLE IF EXISTS blocks""")
        self.cur.execute("""DROP TABLE IF EXISTS txs""")
        self.cur.execute("""DROP TABLE IF EXISTS users""")
        self.conn.commit()

    def create_tables(self):
        # Blocks: contains height and a list of integer ids
        # Txs: a list of unique integer ids and a string of the tx. Where Txs = a JSON array
        # Users a list of unique addresses and a list of integer ids

        # Create blocks table
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS blocks (
                height integer,
                txs text
            )"""
        )

        # Create txs table
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS txs (
                id integer PRIMARY KEY,
                tx text
            )"""
        )

        # Create users table
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS users (
                address text,
                height integer,
                tx_id integer
            )"""
        )

        self.conn.commit()

    def insert_tx(self, tx_data: dict) -> int:
        data = json.dumps(tx_data)
        self.cur.execute(
            """INSERT INTO txs (tx) VALUES (?)""",
            (data,),
        )
        self.conn.commit()

        return self.cur.lastrowid or -1

    def insert_block(self, height: int, txs: list[int]):
        data = json.dumps(txs)
        self.cur.execute(
            """INSERT INTO blocks (height, txs) VALUES (?, ?)""",
            (height, data),
        )
        self.conn.commit()

    def insert_user(self, address: str, height: int, tx_id: int):
        self.cur.execute(
            """INSERT INTO users (address, height, tx_id) VALUES (?, ?, ?)""",
            (address, height, tx_id),
        )
        self.conn.commit()

    def get_block_txs(self, height: int) -> list[int]:
        self.cur.execute(
            """SELECT txs FROM blocks WHERE height=?""",
            (height,),
        )
        data = self.cur.fetchone()
        if data is None:
            return []
        return json.loads(data[0])

    def get_tx(self, tx_id: int) -> dict:
        self.cur.execute(
            """SELECT tx FROM txs WHERE id=?""",
            (tx_id,),
        )
        data = self.cur.fetchone()
        if data is None:
            return {}
        return json.loads(data[0])

    def get_user_tx_ids(self, address: str) -> list[int]:
        # get from users
        self.cur.execute(
            """SELECT tx_id FROM users WHERE address=?""",
            (address,),
        )
        data = self.cur.fetchone()  # (14,)
        if data is None:
            return []

        if isinstance(data[0], int):
            return [data[0]]

        return list(data[0])

    def get_user_txs(self, address: str) -> dict:
        tx_ids = self.get_user_tx_ids(address)
        txs = {}
        for tx_id in tx_ids:
            tx = self.get_tx(tx_id)
            height = tx.get("height")
            txs[height] = tx
        return txs
