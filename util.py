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
    # if output_file_loc does not exist, make it
    if not os.path.exists(output_file_loc):
        with open(output_file_loc, "w") as f:
            f.write("{}")

    # TODO: What if I just return to stdout instead of an output file, and speef improvement? Need to benchmark
    res = os.popen(
        f"{COSMOS_BINARY_FILE} tx decode-file {file_loc} {output_file_loc}"
    ).read()
    with open(output_file_loc, "r") as f:
        return json.load(f)


def _decode_single_test(COSMOS_BINARY_FILE: str, amino: str) -> dict:
    cmd = f"{COSMOS_BINARY_FILE} tx decode {amino} --output json"
    res = os.popen(cmd).read()
    return json.loads(res)


def command_exists(cmd):
    if which(cmd) == None:
        return False
    return True


def get_sender(height: int, msg: dict, WALLET_PREFIX: str, VALOPER_PREFIX: str) -> str:
    # MultibankSend not supported (SDK limitation.)
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
        if key in dict(msg["body"]).keys():
            return msg[key]

    # tries to find the sender in the msg even if the key is not found
    for key, value in dict(msg["body"]).items():
        # print(key, value)

        if key == "messages":
            subMsg: dict
            for subMsg in list(value):
                for k, v in subMsg.items():
                    if k in keys:
                        return v

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

    return "UKNOWN"


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
