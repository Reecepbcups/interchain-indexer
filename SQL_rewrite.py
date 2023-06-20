import base64
import json
import sqlite3
import time
from typing import Any

import regex

from chain_types import Block, Tx
from util import _decode_single_test, get_sender, run_decode_file, txraw_to_hash

# ?
IGNORED_CONTRACTS = {
    "ojo oracle": "juno1yqm8q56hjv8sd4r37wdhkkdt3wu45gc5ptrjmd9k0nhvavl0354qwcf249"
}

# matches `/cosmwasm.wasm.v1.SomeMsgHere` in the amino of a tx
AMINO_MESSAGE_REGEX = regex.compile(
    b"\/[a-zA-Z0-9]*\.[a-zA-Z0-9]*\.[a-zA-Z0-9]*\.[a-zA-Z0-9]*"
)


class Database:
    def __init__(self, db: str):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def execute(
        self,
        cmds: str | list[str],
        commit=False,
        resp=False,
        resp_one=False,
    ) -> list[Any]:
        if type(cmds) == str:
            cmds = [cmds]

        for cmd in cmds:
            self.cur.execute(cmd)

        if commit:
            self.commit()

        if resp_one:
            return self.cur.fetchone()
        if resp:
            return self.cur.fetchall()

        return []

    def create_tables(self):
        # store json blob off as well?
        self.execute(
            [
                """CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, date DATETIME NOT NULL, txs TEXT)""",
                """CREATE TABLE IF NOT EXISTS txs (id INTEGER PRIMARY KEY AUTOINCREMENT, height INT, msg_types BLOB, address VARCHAR(50), tx_hash VARCHAR(64) UNIQUE)""",
                # tx subgroups
                """CREATE TABLE IF NOT EXISTS amino_tx (id INTEGER PRIMARY KEY AUTOINCREMENT, tx_amino BLOB, FOREIGN KEY(id) REFERENCES transactions(id))""",
                # json tx
                """CREATE TABLE IF NOT EXISTS json_tx (id INTEGER PRIMARY KEY AUTOINCREMENT, tx_json BLOB, FOREIGN KEY(id) REFERENCES transactions(id))""",
                # TODO:
                """CREATE TABLE IF NOT EXISTS tx_events (id INTEGER PRIMARY KEY AUTOINCREMENT, event_json BLOB, FOREIGN KEY(id) REFERENCES transactions(id))""",
            ]
        )
        self.commit()

    def optimize_tables(self):
        self.execute(
            [
                # Blocks
                """CREATE INDEX IF NOT EXISTS height ON blocks (height)""",
                """CREATE INDEX IF NOT EXISTS date ON blocks (date)""",
                # txs
                """CREATE INDEX IF NOT EXISTS height ON txs (height)""",
                """CREATE INDEX IF NOT EXISTS tx_hash ON txs (tx_hash)""",
                """CREATE INDEX IF NOT EXISTS address ON txs (address)""",
                """CREATE INDEX IF NOT EXISTS msg_types ON txs (msg_types->'$.[]')""",  # ??
            ]
        )

        # Tx Amino
        # index tx_amino_bytes table by tx_hash? (not sure this is really needed though, future possibly.)

        self.commit()

    def optimize_db(self, vacuum: bool = False):
        # self.optimize_tables()
        # Set journal mode to WAL. - https://charlesleifer.com/blog/going-fast-with-sqlite-and-python/
        cmds = [
            """PRAGMA journal_mode=WAL""",
            """PRAGMA mmap_size=30000000000""",
            """PRAGMA page_size=32768""",
            # """PRAGMA synchronous=OFF"""  # off? or normal?
        ]

        if vacuum:
            cmds += ["""VACUUM""", """PRAGMA optimize"""]

        self.execute(cmds)

    def get_indexes(self, table_name):
        return self.execute(f"""PRAGMA table_name(transactions_height)""", resp=True)

    def get_schema(self, table_name: str):
        return self.execute(f"""PRAGMA table_info({table_name})""", resp=True)

    def query(self, query: str, output=False):
        self.cur.execute(query)
        if output:
            v = self.cur.fetchall()
            print(v)

    def get_all_tables(self):
        return self.execute(
            """SELECT name FROM sqlite_master WHERE type='table';""", resp=True
        )

    def get_table_schema(self, table: str):
        return self.execute(f"""PRAGMA table_info({table})""", resp=True)

    def get_msg_types_from_amino(self, tx_amino: str) -> list[str]:
        possible_msg_types: list[bytes] = regex.findall(
            AMINO_MESSAGE_REGEX, base64.b64decode(tx_amino)
        )

        return list(
            {
                msg_type.decode("utf-8")
                for msg_type in possible_msg_types
                if b"secp256k1.PubKey" not in msg_type  # not a message
            }
        )

    def insert_tx(self, height: int, tx_amino: str) -> int:
        # We insert the data without it being decoded. We can update later
        tx_hash = txraw_to_hash(tx_amino)

        # Already in table (remove in the future? Only affects inserts so may be fine.)
        data = self.execute(
            f"""SELECT id FROM txs WHERE tx_hash='{tx_hash}'""",
            resp_one=True,
        )
        if data is not None:
            # print("tx_hash already in table")
            return data[0]

        msg_types = self.get_msg_types_from_amino(tx_amino)

        self.cur.execute(
            """INSERT INTO txs (height, msg_types, address, tx_hash) VALUES (?, ?, ?, ?)""",
            (height, json.dumps(msg_types), "", tx_hash),
        )
        tx_id = self.cur.lastrowid

        self.execute(
            f"""INSERT INTO amino_tx (id, tx_amino) VALUES ({tx_id}, '{tx_amino}')""",
        )

        # input blank JSON now, will update later when we check for the values set to ""/Null. Same for address
        self.execute(
            f"""INSERT INTO json_tx (id, tx_json) VALUES ({tx_id}, '')""",
        )

        return int(tx_id or 0)

    def iter_all_txs_for_decoding(self):
        return self.execute("""SELECT id, tx_amino FROM amino_tx""", resp=True)

    # use id or the tx_hash? (prob id for the mass decoder)
    def update_decoded_tx(
        self, tx_id: int, tx_json: dict, WALLET_PREFIX: str, VALOPER_PREFIX: str
    ):
        sender = get_sender(-1, tx_json, WALLET_PREFIX, VALOPER_PREFIX)
        print(f"Updating Tx: #{tx_id}", sender)

        self.cur.execute(
            """UPDATE txs SET address=? WHERE id=?""",
            (sender, tx_id),
        )

        self.execute(
            f"""UPDATE json_tx SET tx_json='{json.dumps(tx_json)}' WHERE id={tx_id}""",
        )


if __name__ == "__main__":
    import os

    current_dir = os.path.dirname(os.path.realpath(__file__))
    db = Database(os.path.join(current_dir, "data.db"))
    db.create_tables()
    db.optimize_tables()
    db.optimize_db(vacuum=False)

    # res = db.get_msg_types_from_amino(
    #     "CqYJCqcBCiQvY29zbXdhc20ud2FzbS52MS5Nc2dFeGVjdXRlQ29udHJhY3QSfworanVubzFndTkydzN3a3dhd3E2cThlcGRzeWNlNng3cTVnenZ2NTV3MDR5axI/anVubzE5bm53aDQ5bHdzcXk2YzV3ZzlwOTQzeXQ5dHhlNW13NmtkenRlY2w1ajRxM3JneWgwaDBzZWt3bDhjGg97IndpdGhkcmF3Ijp7fX0KpwEKJC9jb3Ntd2FzbS53YXNtLnYxLk1zZ0V4ZWN1dGVDb250cmFjdBJ/CitqdW5vMWd1OTJ3M3drd2F3cTZxOGVwZHN5Y2U2eDdxNWd6dnY1NXcwNHlrEj9qdW5vMTYzdXBlOXlteHRjNWZzeDBrdnJmY3l4OWU1cHV1MnpocXQ4MmxleHJsYWp6bXg5c203OXNoYWM4OGYaD3sid2l0aGRyYXciOnt9fQqnAQokL2Nvc213YXNtLndhc20udjEuTXNnRXhlY3V0ZUNvbnRyYWN0En8KK2p1bm8xZ3U5Mnczd2t3YXdxNnE4ZXBkc3ljZTZ4N3E1Z3p2djU1dzA0eWsSP2p1bm8xeXAwYTdlMnk2Y2MybXR1eDkycXptMjRneXU4NXk4YTJhZGY4NWs5dzMzaHN3ZnpzOGU3cXJsYXpxcxoPeyJ3aXRoZHJhdyI6e319CqcBCiQvY29zbXdhc20ud2FzbS52MS5Nc2dFeGVjdXRlQ29udHJhY3QSfworanVubzFndTkydzN3a3dhd3E2cThlcGRzeWNlNng3cTVnenZ2NTV3MDR5axI/anVubzF3NzVmMm53Z2d6bmhxN2txM3hxbWtmc3pwMnN1YzdmMG0zc3RtcDV2eHg1OXl2bjU3bnRxa2Zmbm4yGg97IndpdGhkcmF3Ijp7fX0KpwEKJC9jb3Ntd2FzbS53YXNtLnYxLk1zZ0V4ZWN1dGVDb250cmFjdBJ/CitqdW5vMWd1OTJ3M3drd2F3cTZxOGVwZHN5Y2U2eDdxNWd6dnY1NXcwNHlrEj9qdW5vMXdjdWNzeTYzZDZybTBjZGM1cXQ2YWtkODZxOG1wMDhoNnBhZHM0NDgwMmdkZ2NlNmFya3E0eHV1dmUaD3sid2l0aGRyYXciOnt9fQqnAQokL2Nvc213YXNtLndhc20udjEuTXNnRXhlY3V0ZUNvbnRyYWN0En8KK2p1bm8xZ3U5Mnczd2t3YXdxNnE4ZXBkc3ljZTZ4N3E1Z3p2djU1dzA0eWsSP2p1bm8xZTlscmo2Nzlnbnl6MzJuZXE5emhoZXIzanAyNzB5d21kbW5zZHFodWV1a2hlYW13NXdscWw5NHN1dhoPeyJ3aXRoZHJhdyI6e319CqcBCiQvY29zbXdhc20ud2FzbS52MS5Nc2dFeGVjdXRlQ29udHJhY3QSfworanVubzFndTkydzN3a3dhd3E2cThlcGRzeWNlNng3cTVnenZ2NTV3MDR5axI/anVubzF5d2F2OGowcDVheng3em40Y2c2YW13aGxudDg1YThoNXgwYWg1NWdlNDBsaGh5c2c2NnBxNngyNjUzGg97IndpdGhkcmF3Ijp7fX0SagpRCkYKHy9jb3Ntb3MuY3J5cHRvLnNlY3AyNTZrMS5QdWJLZXkSIwohAoWVDnsIvM1mME6TpGfTKSV/WBEJ6ec4n+2xcvwKY6yQEgQKAgh/GNIUEhUKDwoFdWp1bm8SBjEzMTA4OBD41moaQEtbKNG8pDhdp7p4vpFpDHcWj3dJ9FY2nAmhjXA2ypyGcD2u5MUTXPyBosdIM//+PbXEyx3Z09IxaIBBIgmdk/k="
    # )
    # print(res)

    import test_data

    for k, v in test_data.TRANSACTIONS.items():
        for _tx in v:
            db.insert_tx(k, _tx)

    # LEGACY: Single decode instance
    # for tx_id, tx_amino in db.iter_all_txs_for_decoding():
    # j = _decode_single_test("junod", tx_amino)
    # db.update_decoded_tx(tx_id, j, "juno", "junovaloper")

    # mass decode
    j = run_decode_file(
        "juno-decode",
        os.path.join(current_dir, "_input.json"),
        os.path.join(current_dir, "_out.json"),
    )
    for res in j:
        db.update_decoded_tx(res["id"], json.loads(res["tx"]), "juno", "junovaloper")

    db.commit()

    # print(db.get_schema("transactions"))

    # show all txs in transactions
    # db.query("""SELECT * FROM transactions""", output=True)
    # db.query(
    #     """SELECT * FROM txs""",
    #     output=True,
    # )

    db.query("""SELECT count(*) FROM txs""", output=True)
    db.query("""SELECT count(*) FROM amino_tx""", output=True)
    db.query("""SELECT count(*) FROM json_tx""", output=True)

    # select all txs which are from juno1rkhrfuq7k2k68k0hctrmv8efyxul6tgn8hny6y in the database
    for res in db.execute(
        """SELECT id, height, msg_types FROM txs WHERE address='juno1rkhrfuq7k2k68k0hctrmv8efyxul6tgn8hny6y'""",
        resp=True,
    ):
        print(res)

        if "/cosmwasm.wasm.v1.MsgExecuteContract" in res[2]:
            # get json
            tx_json = db.execute(
                f"""SELECT tx_json FROM json_tx WHERE id={res[0]}""",
                resp_one=True,
            )

            print(tx_json)

        # db.query(
        # query Tx json if


exit(1)


class temp_remove_later:
    # ===================================
    # Blocks
    # ===================================

    # TODO: this is from the above class, tmp here
    def __init__(self, db: str):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()

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
            (id,),
        )
        data = self.cur.fetchone()
        if data is None:
            return None

        return Tx(data[0], data[1], data[2], data[3], data[4], data[5], data[6] or "")

    def get_tx_specific(self, tx_id: int, fields: list[str]):
        self.cur.execute(
            f"""SELECT {','.join(fields)} FROM txs WHERE id=?""",
            (id,),
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
                for id in b.tx_ids:
                    tx_ids[id] = True

        txs: list[Tx] = []
        # print("Getting Txs from found tx_ids")
        for id in tx_ids:
            _tx = self.get_tx(id)
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

    #     # Select all id from users WHERE address=? AND height BETWEEN ? AND ?
    #     self.cur.execute(
    #         """SELECT id FROM users WHERE address=? AND height BETWEEN ? AND ?""",
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
