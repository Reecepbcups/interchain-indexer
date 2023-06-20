import base64
import json
import sqlite3
from typing import Any

import regex

from chain_types import Block, Tx
from option_types import BlockOption, TxOptions, TxQueryOption
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

    def get_tx(self, qOpt: TxQueryOption = TxQueryOption.STANDARD) -> Tx | None:
        if qOpt == TxQueryOption.STANDARD:
            raise ValueError("TxQueryOption.STANDARD is not supported yet.")

        order_by = "ASC" if qOpt == qOpt.EARLIEST else "DESC"
        data = self.execute(
            f"""SELECT id FROM txs ORDER BY id {order_by} LIMIT 1""",
            resp_one=True,
        )
        if data is None:
            return None

        return self.get_tx_by_id(data[0])

    def get_txs_by_ids(
        self, tx_lower_id: int, tx_upper_id: int, options: list[TxOptions]
    ) -> list[Tx]:
        txs: list[Tx] = []

        tx_hashes = self.execute(
            f"""SELECT tx_hash FROM txs WHERE id BETWEEN {tx_lower_id} AND {tx_upper_id}""",
            resp=True,
        )

        if tx_hashes is None:
            return txs

        for tx_hash in tx_hashes:
            t = self.get_tx_by_hash(tx_hash[0], options)
            if t:
                txs.append(t)

        return txs if tx_hashes is not None else []

    # TODO: Used when we are ensuring all Txs have been properly decoded.
    def get_txs_not_decoded(self, start_height: int, end_height: int) -> list[Tx]:
        found = self.execute(
            f"""SELECT id FROM txs WHERE id NOT IN (SELECT id FROM json_tx) AND height BETWEEN {start_height} AND {end_height}""",
            resp=True,
        )

        if found is None:
            return []

        is_decoded = set(tx_id[0] for tx_id in found)

        to_decode = []
        for tx_id in range(start_height, end_height + 1):
            if tx_id not in is_decoded:
                t = self.get_tx_by_id(
                    tx_id,
                    options=[TxOptions.ID, TxOptions.TX_HASH, TxOptions.AMINO],
                )
                if t:
                    to_decode.append(t)

        return to_decode

    def get_tx_json(self, tx_id: int = -1, tx_hash: str = "") -> dict:
        """
        Use either the Tx ID or the Tx Hash to get the JSON of the Tx.
        Returns the dict or {} if none
        """

        if tx_id != -1 and tx_hash != "":
            raise ValueError(
                "tx_id and tx_hash cannot both be set for get_tx_json. Choose 1."
            )

        # Since we do not save the hash in the Txs, we grab from the parent table.
        if tx_hash != "":
            tx_id = self.get_tx_id_from_hash(tx_hash)

        res = self.execute(
            f"""SELECT tx_json FROM json_tx WHERE id={tx_id}""",
            resp_one=True,
        )
        if res is None:
            return {}

        return json.loads(res[0])

    def get_tx_id_from_hash(self, tx_hash: str) -> int:
        res = self.execute(
            f"""SELECT id FROM txs WHERE tx_hash='{tx_hash}'""",
            resp_one=True,
        )

        return res[0] if res is not None else -1

    def get_tx_amino(self, tx_id: int = -1, tx_hash: str = "") -> str:
        if tx_id == -1 and tx_hash == "":
            raise ValueError(
                "tx_id and tx_hash cannot both be set for get_tx_amino. Choose 1."
            )

        if tx_hash != "":
            tx_id = self.get_tx_id_from_hash(tx_hash)

        res = self.execute(
            f"""SELECT tx_amino FROM amino_tx WHERE id={tx_id}""",
            resp_one=True,
        )

        return res[0] if res is not None else ""

    def get_tx_by_hash(self, tx_hash: str, options: list[TxOptions] = []) -> Tx | None:
        wantsTxJSON = TxOptions.TX_JSON in options or len(options) == 0
        wantsTxAMINO = TxOptions.AMINO in options and len(options) == 0

        if len(options) == 0:
            options = [tx for tx in TxOptions]

        # remove the options if they are in the list
        options = [
            option
            for option in options
            if option not in [TxOptions.TX_JSON, TxOptions.AMINO]
        ]

        fields = "*"
        if len(options) != 0:
            fields = ", ".join([option.value for option in options])

        tx = self.execute(
            f"""SELECT {fields} FROM txs WHERE tx_hash='{tx_hash}'""",
            resp_one=True,
        )
        if tx is None:
            return None

        tx_json = {}
        if wantsTxJSON:
            tx_json = self.get_tx_json(tx_hash=tx_hash)

        tx_amino = ""
        if wantsTxAMINO:
            # query the AMINO only from said function.
            tx_amino = self.get_tx_amino(tx_hash=tx_hash)

        # TODO: Events?

        # automatically unpack the fields based off the inputs
        tx_dict = {}
        for op in options:
            tx_dict[op.value] = tx[options.index(op)]

        # set the other defaults based off the key default
        for tx_type in Tx.__annotations__.keys():
            if tx_type not in tx_dict:
                tx_dict[tx_type] = ""

        tx_dict["tx_json"] = tx_json
        tx_dict["tx_amino"] = tx_amino

        return Tx(**tx_dict)

    def get_txs_by_address(
        self, address: str, options: list[TxOptions] = []
    ) -> list[Tx]:
        txs: list[Tx] = []
        data = self.execute(
            f"""SELECT tx_hash FROM txs WHERE address='{address}'""",
            resp=True,
        )

        if data is None:
            return txs

        for tx_hash in data:
            t = self.get_tx_by_hash(tx_hash[0], options)
            if t:
                txs.append(t)

        return txs

    def get_txs_by_address_in_range(
        self, address: str, start_height: int, end_height: int, options: list[TxOptions]
    ) -> list[Tx]:
        txs: list[Tx] = []

        # sort Txs by height
        tx_hashes = self.execute(
            f"""SELECT tx_hash FROM txs WHERE address='{address}' AND height BETWEEN {start_height} AND {end_height} ORDER BY height ASC""",
            resp=True,
        )

        if tx_hashes is None:
            return txs

        for tx_hash in tx_hashes:
            t = self.get_tx_by_hash(tx_hash[0], options)
            if t:
                txs.append(t)

        return txs

    def get_tx_by_id(self, tx_id: int, options: list[TxOptions] = []) -> Tx | None:
        tx_hash = self.execute(
            f"""SELECT tx_hash FROM txs WHERE id={tx_id}""",
            resp_one=True,
        )

        if tx_hash is None:
            return None

        return self.get_tx_by_hash(tx_hash[0], options)

    def get_txs_in_range(
        self, start_height: int, end_height: int, options: list[TxOptions]
    ) -> list[Tx]:
        txs: list[Tx] = []

        # sort Txs by height
        tx_hashes = self.execute(
            f"""SELECT tx_hash FROM txs WHERE height BETWEEN {start_height} AND {end_height} ORDER BY height ASC""",
            resp=True,
        )

        if tx_hashes is None:
            return txs

        for tx_hash in tx_hashes:
            t = self.get_tx_by_hash(tx_hash[0], options)
            if t:
                txs.append(t)

        return txs

    # ===================================
    # Blocks
    # ===================================

    def get_total_blocks(self) -> int:
        count = self.execute("""SELECT count(*) FROM blocks""", resp=True)
        return count[0][0]

    def insert_block(self, height: int, time: str, txs_ids: list[int]):
        try:
            self.cur.execute(
                """INSERT INTO blocks (height, date, txs) VALUES (?, ?, ?)""",
                (height, time, json.dumps(txs_ids)),
            )
        except Exception as e:
            print("insert_block", e)

    def get_block(
        self, block_height: int, option: BlockOption = BlockOption.STANDARD
    ) -> Block:
        cmd = f"""SELECT height, date, txs FROM blocks WHERE height={block_height}"""

        if option == BlockOption.EARLIEST:
            cmd = (
                f"""SELECT height, date, txs FROM blocks ORDER BY height ASC LIMIT 1"""
            )
        elif option == BlockOption.LATEST:
            cmd = (
                f"""SELECT height, date, txs FROM blocks ORDER BY height DESC LIMIT 1"""
            )

        b = self.execute(
            cmd,
            resp_one=True,
        )
        if b is None:
            return Block(-1, "", [])

        return Block(b[0], b[1], json.loads(b[2]))

    def find_missing_blocks(self, start_height: int, end_height: int) -> list[int]:
        # run a query which finds all missing heights in the range for the blocks table
        missing = self.execute(
            f"""SELECT height FROM blocks WHERE height BETWEEN {start_height} AND {end_height}""",
            resp=True,
        )

        heights = set(range(start_height, end_height + 1))

        for m in missing:
            heights.remove(m[0])

        return list(heights)


if __name__ == "__main__":
    import os

    current_dir = os.path.dirname(os.path.realpath(__file__))
    db = Database(os.path.join(current_dir, "data.db"))
    db.create_tables()
    db.optimize_tables()
    db.optimize_db(vacuum=False)

    print(db.get_total_blocks())
    print(db.find_missing_blocks(2, 10))

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

    db.insert_block(3, "2021-01-01 10:10:12", [1, 2, 3, 4, 5])
    db.commit()

    print(db.get_block(3))
    print(db.get_block(-1))

    db.query("""SELECT * FROM blocks""", output=True)

    # Query specific JSON data depending on your needs. Get only what you need, easily unpacked.
    print(
        db.get_tx_by_hash(
            "EF99CD08ECE18098A406A459C0F56C9B9F3F7EB6DCB7D9DA483C974D63C15D4A"
        )
    )

    print(
        db.get_tx_by_hash(
            "EF99CD08ECE18098A406A459C0F56C9B9F3F7EB6DCB7D9DA483C974D63C15D4A",
            options=[
                TxOptions.ID,
                TxOptions.HEIGHT,
                TxOptions.ADDRESS,
                TxOptions.TX_JSON,
            ],
        )
    )

    print(
        db.get_tx_by_hash(
            "EF99CD08ECE18098A406A459C0F56C9B9F3F7EB6DCB7D9DA483C974D63C15D4A",
            options=[
                TxOptions.ID,
                TxOptions.AMINO,
            ],
        )
    )

    # wraps the tx_hash function
    print(
        db.get_tx_by_id(
            1,
            options=[
                TxOptions.ID,
                TxOptions.AMINO,
            ],
        )
    )

    res = db.get_txs_by_address_in_range(
        "juno1rkhrfuq7k2k68k0hctrmv8efyxul6tgn8hny6y",
        0,
        10_000_000,
        options=list(tx for tx in TxOptions),
    )
    print(len(res))

    # select all txs which are from juno1rkhrfuq7k2k68k0hctrmv8efyxul6tgn8hny6y in the database
    '''
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
    '''
