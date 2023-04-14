import json
import sqlite3

from chain_types import Block, Tx

'''        



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


class Database:
    def __init__(self, db: str):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def create_tables(self):
        # height, time, txs_ids
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, time TEXT, txs TEXT)"""
        )

        # txs: id int primary key auto inc, height, tx_amino, msg_types, tx_json
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS txs (id INTEGER PRIMARY KEY AUTOINCREMENT, height INTEGER, tx_amino TEXT, msg_types TEXT, tx_json TEXT)"""
        )

        # users: address, height, tx_id
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS users (address TEXT, height INTEGER, tx_id INTEGER)"""
        )        

        # messages: message_type, height, count
        # This may be extra? We could just iter txs but I guess it depends. Can add in the future
        # NOTE: May remove later
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS messages (message TEXT, height INTEGER, count INTEGER)"""
        )
        
        self.commit()

    def get_all_tables(self):
        self.cur.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
        return self.cur.fetchall()

    def get_table_schema(self, table: str):
        self.cur.execute(f"""PRAGMA table_info({table})""")
        return self.cur.fetchall()
    

    # ===================================
    # User
    # ===================================
    def insert_user(self, address: str, height: int, tx_id: int):
        self.cur.execute(
            """INSERT INTO users (address, height, tx_id) VALUES (?, ?, ?)""",
            (address, height, tx_id),
        )        

    # ===================================
    # Messages / Types (May not be needed)
    # ===================================
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
    
    def get_msg_types_transactions_in_range(self, msg_type: str, start: int, end: int) -> list[Tx]:
        """
        Returns a list of tx_ids that have the msg_type in the range requested
        """
        txs: list[Tx] = []
        
        self.cur.execute(            
            """SELECT * FROM txs WHERE msg_types LIKE ? AND height>=? AND height<=?""",
            (f"%{msg_type}%", start, end),
        )
        data = self.cur.fetchall()
        if data is None:
            return []
        
        for tx in data:
            txs.append(Tx(tx[0], tx[1], tx[2], json.loads(tx[3]), tx[4]))            

        return list(txs)

    def get_msg_types_ids_in_range(self, msg_type: str, start: int, end: int) -> list[int]:
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
    
    def get_latest_saved_block(self) -> Block:
        self.cur.execute("""SELECT * FROM blocks ORDER BY height DESC LIMIT 1""")
        data = self.cur.fetchone()
        if data is None:
            return Block(0, "", [])
        
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
            return list(range(start_height, end_height+1))
        
        found_heights = set(x[0] for x in data)
        missing_heights = [height for height in range(start_height, end_height+1) if height not in found_heights]
        return missing_heights
    
    # ===================================
    # Transactions
    # ===================================
       
    def insert_tx(self, height: int, tx_amino: str):
        # We insert the data without it being decoded. We can update later 
        # insert the height and tx_amino, then return the unique id
        # fill the other collums with empty strings
        self.cur.execute(
            """INSERT INTO txs (height, tx_amino, msg_types, tx_json) VALUES (?, ?, ?, ?)""",
            (height, tx_amino, "", ""),
        )
        return self.cur.lastrowid
    
    def update_tx(self, id: int, tx_json: str, msg_types: str):
        # update the data after we decode it (post insert_tx)
        self.cur.execute(
            """UPDATE txs SET tx_json=?, msg_types=? WHERE id=?""",
            (tx_json, msg_types, id),
        )    

    def get_tx(self, tx_id: int) -> Tx | None:
        self.cur.execute(
            """SELECT * FROM txs WHERE id=?""",
            (tx_id,),
        )
        data = self.cur.fetchone()
        if data is None:
            return None
                
        return Tx(data[0], data[1], data[2], data[3], data[4])
    

    def get_txs_in_range(self, start_height: int, end_height: int) -> list[Tx]:
        latest_block = self.get_latest_saved_block()
        if end_height > latest_block.height:
            end_height = latest_block.height    
            
        self.cur.execute(
            """SELECT * FROM txs WHERE height BETWEEN ? AND ?""",
            (start_height, end_height),
        )
        data = self.cur.fetchall()
        if data is None:
            return []                                    
        
        return [Tx(x[0], x[1], x[2], x[3], x[4]) for x in data]    


    def get_users_txs_in_range(self, address: str, start_height: int, end_height: int) -> list[Tx]:
        latest_block = self.get_latest_saved_block()
        if end_height > latest_block.height:
            end_height = latest_block.height    
            
        self.cur.execute(
            """SELECT * FROM users WHERE address=? AND height BETWEEN ? AND ?""",
            (address, start_height, end_height),
        )
        data = self.cur.fetchall()
        if data is None:
            return []        
    
        txs = []
        for values in data:
            tx = self.get_tx(values[2])
            if tx is not None:
                txs.append(tx)
        return txs