# cosmos-indexer

## Getting Started

```bash
cp chain_config.json.example chain_config.json
# Setup the values correctly and ranges for blocks

# TASK Types:
# - download
# - decode
# - missing
# - sync (When you have it all indexed, use this to stay up on the tip.)

python3 main.py 0
python3 main.py 1
...
```

## Notes

```text
- Addresses of UNKOWN are for MultiSendMessages. These Messages do not contain the actual address. There are only a few.
- Blocks 2578098 and 4136531 are missing due to skipping on upgrades. Should I put empty Block data here?
```

## Archive Downloads:

- TODO

## Compression & Decompression

```bash
# compressed
tar -czvf juno_start-end.tar.gz data.db

# decompressed
tar -xzvf name-of-archive.tar.gz
```
