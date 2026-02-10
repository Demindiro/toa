# TOA key-value store

Interface and backends for a simple key-value store.

Keys and values can be of arbitrary length.

## Backends

- in-memory backend using `BTreeMap` (requires `alloc` feature).
- on-disk backend using `sled` (requires `sled` feature).
