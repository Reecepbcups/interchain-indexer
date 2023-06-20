from enum import Enum


class TxOptions(Enum):
    # These should match up with the colum names in the table
    ID = "id"
    HEIGHT = "height"
    AMINO = "tx_amino"
    TX_JSON = "tx_json"
    MSG_TYPES = "msg_types"
    ADDRESS = "address"
    TX_HASH = "tx_hash"


class BlockOption(Enum):
    STANDARD = "standard"
    EARLIEST = "earliest"
    LATEST = "latest"


class TxQueryOption(Enum):
    STANDARD = "standard"
    EARLIEST = "earliest"
    LATEST = "latest"
