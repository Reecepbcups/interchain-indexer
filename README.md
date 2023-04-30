# Cosmos Indexer

## Past Index Archive Download

- <https://reece.sh/private/juno/juno_1-7990650.tar.gz>
- SQLite data.db - 14GB Compressed, 55GB de-compressed. *(missing 2578098 and 4136531 per halts)*

## Snapshot Export Data

- <https://exports.reece.sh/juno> (Bank, Staking, and Sequence exports every 20k blocks)

## Compression & Decompression

```bash
# compressed
tar -czvf network_start-end.tar.gz data.db
# decompressed
tar -xzvf name-of-archive.tar.gz
```

---

## Getting Started

```bash
cp chain_config.json.example chain_config.json
# Setup the values correctly and ranges for blocks

# TASK Types:
# - download
# - decode
# - missing
# - sync (When you have it all indexed, use this to stay up on the tip. This gets latest chain & downloaded, and downloads / decodes all inbetween)

python3 main.py 0
python3 main.py 1
...
```

## Notes

```text
- Addresses of UNKOWN are for MultiSendMessages. These Messages do not contain the actual addresses.
```

## Sections
```json
    "sections": {
        "genesis": {
            "start": 1,
            "end": 2579000,
            "grouping": 1000,
            "rpc_endpoints": [
                "https://rpc-v2-archive.junonetwork.io:443"
            ]
        },
        "2nd": {
            "start": 2578000,
            "end": 4136600,
            "grouping": 1,
            "rpc_endpoints": [
                "https://rpc-v3-archive.junonetwork.io:443"
            ]
        },
        "end1": {
            "start": 4136500,
            "end": 6000000,
            "grouping": 1000,
            "rpc_endpoints": [
                "https://rpc-archive.junonetwork.io:443"
            ]
        },
        "end2": {
            "start": 6000000,
            "end": 8000000,
            "grouping": 1000,
            "rpc_endpoints": [
                "https://rpc-archive.junonetwork.io:443"
            ]
        }
    }
```