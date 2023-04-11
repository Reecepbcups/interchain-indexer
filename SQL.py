import json
import sqlite3


class Database:
    def __init__(self, db: str):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def drop_all(self):
        self.cur.execute("""DROP TABLE IF EXISTS blocks""")
        self.cur.execute("""DROP TABLE IF EXISTS txs""")
        self.cur.execute("""DROP TABLE IF EXISTS users""")
        self.cur.execute("""DROP TABLE IF EXISTS messages""")
        self.commit()

    def create_tables(self):
        # Blocks: contains height and a list of integer ids
        # Txs: a list of unique integer ids and a string of the tx. Where Txs = a JSON array
        # Users a list of unique addresses and a list of integer ids

        # Create blocks table
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS blocks (
                height integer PRIMARY KEY not null,
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
                height integer not null,
                tx_id integer
            )"""
        )

        # create a message types table
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS messages (
                message text not null,
                height integer not null,
                count integer not null
            )"""
        )

        self.commit()

    def get_all_tables(self):
        self.cur.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
        return self.cur.fetchall()

    def get_table_schema(self, table: str):
        self.cur.execute(f"""PRAGMA table_info({table})""")
        return self.cur.fetchall()

    def insert_tx(self, tx_data: dict) -> int:
        data = json.dumps(tx_data)
        self.cur.execute(
            """INSERT INTO txs (tx) VALUES (?)""",
            (data,),
        )
        # self.conn.commit()

        return self.cur.lastrowid or -1

    def insert_block(self, height: int, txs: list[int]):
        data = json.dumps(txs)
        self.cur.execute(
            """INSERT INTO blocks (height, txs) VALUES (?, ?)""",
            (height, data),
        )
        # self.conn.commit()

    def insert_type_count(self, msg_type: str, count: int, height: int):
        self.cur.execute(
            """INSERT INTO messages (message, height, count) VALUES (?, ?, ?)""",
            (msg_type, height, count),
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

    def get_type_count_over_range(
        self, msg_type: str, start: int, end: int
    ) -> list[int]:
        self.cur.execute(
            """SELECT count FROM messages WHERE message=? AND height>=? AND height<=?""",
            (msg_type, start, end),
        )
        data = self.cur.fetchall()
        if data is None:
            return []
        return [x[0] for x in data]

    def get_all_count_over_range(self, start: int, end: int) -> list[int]:
        self.cur.execute(
            """SELECT count FROM messages WHERE height>=? AND height<=?""",
            (start, end),
        )
        data = self.cur.fetchall()
        if data is None:
            return []
        return [x[0] for x in data]

    def get_total_blocks(self) -> int:
        self.cur.execute("""SELECT COUNT(*) FROM blocks""")
        data = self.cur.fetchone()
        if data is None:
            return 0
        return data[0]

    def insert_user(self, address: str, height: int, tx_id: int):
        self.cur.execute(
            """INSERT INTO users (address, height, tx_id) VALUES (?, ?, ?)""",
            (address, height, tx_id),
        )
        # self.conn.commit()

    def get_block_txs(self, height: int) -> list[int] | None:
        self.cur.execute(
            """SELECT txs FROM blocks WHERE height=?""",
            (height,),
        )
        data = self.cur.fetchone()
        if data is None:
            return None
        return json.loads(data[0])

    def get_latest_saved_block_height(self) -> int:
        self.cur.execute("""SELECT height FROM blocks ORDER BY height DESC LIMIT 1""")
        data = self.cur.fetchone()
        if data is None:
            return -1
        return data[0]

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
        # get all Tx ids for a given address
        self.cur.execute(
            """SELECT tx_id FROM users WHERE address=?""",
            (address,),
        )
        data = self.cur.fetchall()
        if data is None:
            return []

        return [tx_id[0] for tx_id in data]

    def get_user_txs(self, address: str) -> dict:
        tx_ids = self.get_user_tx_ids(address)
        txs = {}
        for tx_id in tx_ids:
            tx = self.get_tx(tx_id)
            txs[tx_id] = tx
        return txs

    def get_all_accounts(self) -> list[str]:
        # return all accounts and the len of txs they have
        self.cur.execute(
            """SELECT address, COUNT(tx_id) FROM users GROUP BY address""",
        )
        data = self.cur.fetchall()
        if data is None:
            return []
        return data
