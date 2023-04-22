from dataclasses import dataclass


@dataclass
class Contract:
    contract_addr: str
    label: str
