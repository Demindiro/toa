# Appender

Appender is an append-only object-/filesystem for immutable storage.

## Reasoning

When it comes to data, a few things are generally true:

- temporary data is quickly deleted.
- non-temporary data is valuable and kept around for a long time,
  if not forever.
- you want multiple copies of valuable data, at minimum 3.

This system is designed to exploit these needs in several ways:

1. All files have a unique hash based on their contents.
2. Freeing space involves wiping an entire partition.
3. Storage is tiered, with higher tiers cleared out more frequently.

A typical, minimal setup will look like this:

1. file cache in RAM, managed by the OS.
2. Tier 0 with 2 partitions on an SSD.
3. Tier 1 with 2 partitions on a large HDD.
4. Tier 2 with any number of partitions to "cold" storage.

Having at least two partitions allows uninterrupt use even during migration
in preparation of a partition wipe.


## On-disk format

Objects are grouped in packs.
Each pack is committed atomically.
Each pack is independent of other packs.

A pack consists of several structures:

- A plain list of objects
- A HAMT mapping keys to objects (Hash Array Mapped Trie)
- A record trie of compressed, encrypted strips of data

All cryptographic hashes use TOA hash except when noted otherwise.

### Key & encryption

All data is always encrypted by default.
This enabled the option to password-protect the data later without needing
re-encryption.

The key is stored externally by a platform-specific mechanism.


### Pack reference

| bytes   | short description |
| -------:|:----------------- |
|    31:0 | key               |
|   63:32 | record trie root  |
|   71:64 | object trie root  |

If the pack content's must be secret,
encrypt the pack reference.


### Record trie

A record trie represents an address spase containing arbitrary data.

Leaf records consist only of plain data.
Parent records consist of pointers to other nodes.

Records have a maximum uncompressed length of 128KiB.
The trie always has a depth of 3.

!!! note `17 + (17 - 5) * 3 = 53`,
    i.e. a single pack can contain up to 8PiB of data.

Keep in mind that records are encrypted,
hence the poly1305 hash.

| bytes | short description     |
| -----:|:--------------------- |
|  15:0 | poly1305 tag          |
| 23:16 | byte offset           |
| 27:24 | compressed info   (1) |
| 31:28 | uncompressed info (2) |

| bits  | description                |
| -----:|:-------------------------- |
|  13:0 | algorithm (1) / (zero) (2) |
| 31:14 | length in bytes            |

The low 64 bits of the nonce is the record index in little-endian.
The high 32 bits of the nonce is the depth in little-endian,
starting from 0 for the leaf record.

!!! note `record_index = byte_offset >> depth`.

### Hash to object trie (HAMT)

Nodes *must not* cross a record boundary.

Objects *may* cross a record boundary.

Objects *should* be stored in records separate from nodes.

Objects *can* be aligned to an OS page boundary to avoid
needing an extra copy when memory-mapping,
but this is not required.

!!! note https://dotat.at/prog/qp/blog-2015-10-04.html
    This is about QP tries, but are strongly related to HAMT.

#### Leaf

| bytes | short description |
| -----:|:----------------- |
|  31:0 | hash              |
| 39:32 | offset            |
| 47:40 | length            |

#### Parent

| bytes | short description |
| -----:|:----------------- |
|   1:0 | populated         |
|   7:2 | (zero)            |
| ...:8 | branches          |


## Write log

Writing new data is done using a simple append-log structure.
The log structure assumes the presence of an in-memory index
for fast lookups and is designed to be regularly flushed:

It consists of 2 parts:

- a list of 64KiB chunks, each optionally compressed.
- a list of (hash, entry) tuples describing chunks.

| bytes | short description |
| -----:|:----------------- |
|  31:0 | hash              |
| 63:32 | entry             |

| bytes | short description     |
| -----:|:--------------------- |
|   7:0 | (zero)                |
|  15:8 | object length         |
| 23:16 | chunk byte offset     |
| 27:24 | compressed info   (1) |
| 31:28 | uncompressed info (2) |

| bits  | description                |
| -----:|:-------------------------- |
|   7:0 | algorithm (1) / (zero) (2) |
|  31:8 | length in bytes            |

**Note** lack of encryption.


## Containers

Objects by themselves likely aren't useful for common tasks.
A few container formats are defined to integrate with existing systems.

### Plain container

The plain container starts with the magic string "Plainey Appender".
It ends with a metadata table and an unencrypted pack reference.

It is preceded by a 32-bit meta table size,
which in turn is preceded by the corresponding meta table.

### UNIX container

The UNIX container is designed as an equivalent to TAR files.
It supports basic attributes common to all UNIX systems:
UID, GID, file

- UID
- GID
- file permissions
- modified time

Only regular files, directories and symbolic links are supported.

The UNIX container reuses the plain container,
but stores the hash of the root directory right before the pack reference.


### Write log

The write log uses a directory with three files:

- `CHUNKS`: chunk list
- `ENTRIES`: entry list
- `META`: magic + key + metatable

| bytes | short description |
| -----:|:----------------- |
|  15:0 | magic "ToA write log\x26\x01\x22" |
| 20:16 | meta table size   |
|   ... | meta table        |


### Meta table

The meta table is used by some containers.

To support various usecases, it provides a table of key-value pairs.
Keys must be valid UTF-8.
Keys are prefixed with a 8-bit length in bytes.
Values may be any arbitrary data.
Values are prefixed with a 16-bit length in bytes.
