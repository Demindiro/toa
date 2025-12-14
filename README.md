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

There are several structures:

1. An external file pointing to the latest snapshot.
2. A skiplist of snapshots, from new to old.
3. A trie of compressed records.
4. A QP trie mapping hashes to objects.

All cryptographic hashes use BLAKE3 except when noted otherwise.

### 1. External file

The exact format is OS-dependent, but the following structure is recommended:

| bytes   | short description        |
| -------:|:------------------------ |
|    15:0 | magic "Appender 2025/11" |
|   19:16 | argon2id memory (KiB)    |
|   20:20 | argon2id parallelism     |
|   21:21 | argon2id iterations      |
|   22:22 | block size               |
|   23:23 | (zero)                   |
|   27:24 | argon2id hash            |
|   30:28 | (zero)                   |
|   39:31 | head LBA                 |
|   47:40 | (zero)                   |
|   63:48 | argon2id salt            |
|   95:64 | head hash                |
|  127:96 | encryption key           |
| ...:128 | path                     |

The magic must always have the value "Appender 2025/11".

The password is optional.
If any argon2id parameter is non-zero, it is enabled.
The hash is derived from the low 32-bits of the BLAKE3
hash of the password hash.
The password validity can be checked with the hash
before attempting decryption.
The salt is always 16 bytes.

The head LBA points to the latest snapshot,
which is always at most 512 bytes and aligned to LBA.
The hash is used to ensure integrity of the 512 bytes.

The encryption key is used with ChaCha12.
Encryption is always enabled.
If a password is used, the key is XORed with the password hash.

!!! note Forcing encryption allows a password to be enabled at a later time.

### 2. Snapshots

| bytes   | short description |
| -------:|:----------------- |
|    15:0 | poly1305          |
|   39:16 | nonce             |
|   55:40 | object trie root  |
|   63:56 | length            |
|  127:64 | record trie root  |


Only bytes 127:40 are encrypted.
poly1305 and nonce are stored unencrypted.


### 3. Record trie

A record trie represents an address spase containing arbitrary data.

Leaf records consist only of plain data.
Parent records consist of pointers to other nodes.

Keep in mind that records are encrypted,
hence the poly1305 hash.

| bytes | short description     |
| -----:|:--------------------- |
|  15:0 | poly1305              |
| 39:16 | nonce                 |
| 47:40 | byte offset           |
| 51:48 | compressed length     |
| 55:52 | uncompressed length   |
| 63:56 | (zero)                |

Data past the uncompressed length is assumed to be zero.

The record pitch is a power of two and must be at least (1 << 1).


### 4. Hash to object "QP trie"

Nodes *must not* cross a record boundary.

Objects *may* cross a record boundary.

Objects *should* be stored in records separate from nodes.

Objects *can* be aligned to an OS page boundary to avoid
needing an extra copy when memory-mapping,
but this is not required.

The trie is a variant of QP tries.
The main difference is the lack of nibble index
and hence lack of prefix compression.
This is because, normally, cryptographic hashes have
high noise and won't share prefixes in a meaningful sense.

!!! note https://dotat.at/prog/qp/blog-2015-10-04.html

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

#### External node

| bytes | short description |
| -----:|:----------------- |
|   7:0 | snapshot ID       |
|  15:8 | offset            |


## OS integration

### Linux

TODO
