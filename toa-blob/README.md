This crate contains a storage system for large append-only blobs.

It is designed with SMR disks in mind (although the author has
great difficulty finding host-managed SMR disks). For this reason
it requires two partitions: one for storing the header and another
with append-only zones.

The header is only partially encrypted. It points to zones which
are used as logs. The logs must be parsed entirely to reconstruct
in-memory state.

Blobs are append-only. To free memory a blob must be wiped entirely.
Blobs can be written to in byte-increments for convenience.
Incomplete blocks are written to the log until a whole block can
be appended to a zone.

*All integers are in little-endian format*.

## Required algorithms

- argon2id
- chacha12poly1305
- zstd

## Header

| bytes   | name                   |
| -------:|:---------------------- |
|     3:0 | magic ("ToaB")         |
|     7:4 | version (0x20260307)   |
|    15:8 | generation             |
|   95:16 | keyslot 0              |
|  175:96 | keyslot 1              |
| 255:176 | keyslot 2              |
| 495:255 | encrypted area         |
| 511:496 | encrypted area tag     |

### Keyslot

| bytes | name                   |
| -----:|:---------------------- |
|   0:0 | type (0 = none, 1 = argon2id) |
|   1:3 | (pad)                  |
|   7:4 | M cost                 |
|  11:8 | T cost                 |
| 15:12 | P cost                 |
| 31:16 | salt                   |
| 63:32 | master key             |
| 79:64 | master key tag         |

master key tag is poly1305 of encrypted master key.

### Encrypted area

| bytes   | name                   |
| -------:|:---------------------- |
|    15:0 | zone partition ID      |
|   23:16 | zone size              |
|   27:24 | log zone ID            |
|   31:28 | log zone head          |
|   35:32 | log block size         |
|   63:36 | (pad)                  |
|   95:64 | log encryption key     |
|  239:44 | (pad)                  |

## Format

The partition is split into equal fixed-size zones.
The first two zones are reserved for the header and log.
It is designed to host a small number of large blobs.
All metadata is loaded into memory.

### Log

The log records all operations committed since the store was created.
The log must be replayed to reconstruct in-memory state.

Log entries are grouped in fixed-size blocks. Each block is encrypted.

| bytes   | name                   |
| -------:|:---------------------- |
|    15:0 | tag                    |
|  ...:16 | entries                |


#### Entry types

All entries start with a 8-bit type ID.
All entries are aligned to a 8-byte boundary,
padded with zeros if necessary.

##### 0. Nop

| type  | name                  |
|:----- |:--------------------- |
| u8    | (type)                |
| u24   | (pad)                 |
| u32   | padding size          |
| u8[]  | padding               |

##### 1. Create blob

| type  | name                  |
|:----- |:--------------------- |
| u8    | (type)                |
| u8    | name length           |
| u16   | blob ID               |
| u32   | data zone count       |
| u32   | table zone count      |
| u256  | encryption key        |
| u32   | nonce[95:64]          |
| u32[] | data zone IDs         |
| u32[] | table zone IDs        |
| u8[]  | name                  |
| u8[]  | (pad)                 |

##### 2. Delete blob

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u8    | (pad)                  |
| u16   | blob ID                |
| u32   | (pad)                  |

##### 3. Add zone to blob

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u8    | (pad)                  |
| u16   | blob ID                |
| u32   | (pad)                  |

##### 5. Append blob tail

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u8    | (pad)                  |
| u16   | blob ID                |
| u32   | data length            |
| u8[]  | data                   |
| u8[]  | (pad)                  |

##### 6. Allocate zone

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u8    | (pad)                  |
| u16   | blob ID                |
| u32   | zone ID                |

##### 7. Commit blob tail

Takes a full blob tail and appends it to the main blob.
This automatically clears the tail buffer.

The offset doubles as nonce. As blobs are append-only
this should not result in any vulnerabilities.

> **TODO** what if there is a power outage between
> writing the record and the log entry?

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u8    | compression algorithm  |
| u16   | blob ID                |
| u32   | compressed size        |
| u64   | offset                 |
| u128  | tag                    |



### Blob

A blob has one or more zones attached.
The list of zones of each blob is kept entirely in-memory.
