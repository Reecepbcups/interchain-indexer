# This script gets all validators from a rest endpoint.
# This is just a helper method for the indexer, not using the indexer.

import json
import os

# # pip install bech32 - https://pypi.org/project/bech32/
import bech32
import httpx

current_dir = os.path.dirname(os.path.realpath(__file__))
REST_URL = "https://juno-api.reece.sh/cosmos/staking/v1beta1/validators"


def address_convert(address="", prefix="cosmos"):
    _, data = bech32.bech32_decode(address)
    return bech32.bech32_encode(prefix, data)


headers = {
    "accept": "application/json",
}

params = {
    "pagination.limit": "1000",
}

response = httpx.get(
    REST_URL,
    params=params,
    headers=headers,
)

# get validators key
validators = response.json()["validators"]

vals: dict[str, dict] = {}
for val in validators:
    # what if its a smart contract though? can they vote
    opperator_addr = val["operator_address"]
    name = val["description"]["moniker"]

    normal_addr = address_convert(opperator_addr, prefix="juno")
    # print(normal_addr)

    vals[normal_addr] = {
        "name": name,
        "val_addr": opperator_addr,
    }


# save vals to file here
with open(os.path.join(current_dir, "all_validators.json"), "w") as f:
    f.write(json.dumps(vals, indent=4))
