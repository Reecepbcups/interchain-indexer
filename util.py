import base64
import hashlib
import json
import os
from shutil import which

import httpx

current_dir = os.path.dirname(os.path.realpath(__file__))


def txraw_to_hash(tx_raw_amino: str) -> str:
    """
    Converts a base txRaw string into the tx hash on chain. Useful to query with
    appd q tx <hash>, an RPC, or via mintscan.io
    """
    decoded_bytes = base64.b64decode(tx_raw_amino)
    tx_hash = hashlib.sha256(decoded_bytes).hexdigest()
    return tx_hash.upper()


def run_decode_file(
    COSMOS_BINARY_FILE: str, file_loc: str, output_file_loc: str
) -> dict:
    # TODO: What if I just return to stdout instead of an output file, and speef improvement? Need to benchmark
    res = os.popen(
        f"{COSMOS_BINARY_FILE} tx decode-file {file_loc} {output_file_loc}"
    ).read()
    with open(output_file_loc, "r") as f:
        return json.load(f)


def command_exists(cmd):
    if which(cmd) == None:
        return False
    return True


def get_sender(
    height: int, msg: dict, WALLET_PREFIX: str, VALOPER_PREFIX: str
) -> str | None:
    # MultibankSend not yet supported
    keys = [
        "sender",
        "delegator_address",
        "from_address",
        "grantee",
        "voter",
        "signer",
        "depositor",
        "proposer",
    ]

    for key in keys:
        if key in msg.keys():
            return msg[key]

    # tries to find the sender in the msg even if the key is not found
    for key, value in msg.items():
        if not isinstance(value, str):
            continue

        if not (value.startswith(WALLET_PREFIX) or value.startswith(VALOPER_PREFIX)):
            continue

        # smart contracts are ignored. junovaloper1 = 50 characters.
        if len(value) > 50:
            continue

        return value

    # write error to file if there is no sender found (we need to add this type)
    with open(os.path.join(current_dir, "no_sender_error.txt"), "a") as f:
        f.write(f"Height:{height} -" + str(msg) + "\n\n")

    return None


def get_latest_chain_height(RPC_ARCHIVE: str) -> int:
    r = httpx.get(f"{RPC_ARCHIVE}/abci_info?")

    if r.status_code != 200:
        # TODO: backup RPC?
        print(
            f"Error: get_latest_chain_height status_code: {r.status_code} @ {RPC_ARCHIVE}. Exiting..."
        )
        exit(1)

    current_height = (
        r.json().get("result", {}).get("response", {}).get("last_block_height", "-1")
    )

    print(f"Current Height: {current_height}")

    return int(current_height)
