from dataclasses import dataclass


@dataclass
class BlockData:
    height: int
    block_time: str
    encoded_txs: list[str]


@dataclass
class DecodeGroup:
    start: int
    end: int


@dataclass
class Block:
    height: int
    time: str
    tx_ids: list[int]


@dataclass
class Tx:
    id: int
    height: int
    tx_amino: str
    msg_types: list[str]  # JSON.load
    tx_json: str
    address: str
    tx_hash: str
