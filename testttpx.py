# create an async httpx call to a website

url = "https://rpc-archive.junonetwork.io:443"  #

import asyncio
import json
import multiprocessing
import os

import httpx

from util import decode_txs

# create a folder name tmpBlocks
current_dir = os.path.dirname(os.path.abspath(__file__))
blocks = os.path.join(current_dir, "tmpBlocks")
os.makedirs(blocks, exist_ok=True)


async def download(height: int) -> list[dict]:
    decoded_txs: list[dict] = []
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{url}/block?height={height}")

        # get status code
        if r.status_code != 200:
            print(f"Error: {r.status_code} @ height {height}")
            return []

        data = r.json()

        txs = data["result"]["block"]["data"]["txs"]

        decoded_txs = decode_txs("junod", txs)

        print(f"Decoded {len(decoded_txs)} txs @ height {height}")

    # save to JSON maybe? This way we can asyncly save it. Then we will convert to .db after?

    with open(os.path.join(blocks, f"{height}.json"), "w") as f:
        f.write(json.dumps(decoded_txs))

    return decoded_txs


async def main():
    tasks = []

    with multiprocessing.Pool(8) as pool:
        for i in range(6_000_000, 6_000_025):
            tasks.append(asyncio.create_task(download(i)))

        await asyncio.gather(*tasks)


asyncio.run(main())
