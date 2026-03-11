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

## Header

| bytes   | name                   |
| -------:|:---------------------- |
|     3:0 | magic ("ToaB")         |
|     7:4 | version (0x20260307)   |
|    15:8 | generation             |
|   31:16 | zone partition ID      |
|   39:32 | zone size              |
|   47:40 | zone block size        |
|   55:48 | log zone ID + head     |
|   64:56 | (pad)                  |

## Format

The partition is split into equal fixed-size zones.
The first two zones are reserved for the header and log.
It is designed to host a small number of large blobs.
All metadata is loaded into memory.

### Log

The log records all operations committed since the store was created.
The log must be replayed to reconstruct in-memory state.

Log entries are grouped in fixed-size blocks.
Log entries must not cross blocks.

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
| u8[]  | name                  |
| u8[]  | (pad)                 |

##### 2. Delete blob

This operations performs a **swap remove**: if the blob to be deleted
is not the last element, the last element is put in the place of the
deleted blob. This ensures the blob array remains contiguous.

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u24   | (pad)                  |
| u32   | blob index             |

##### 3. Add zone to blob

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u24   | (pad)                  |
| u32   | blob index             |
| u64   | zone ID                |

##### 4. Rename blob

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u8    | name length            |
| u16   | (pad)                  |
| u32   | blob index             |
| u8[]  | name                   |
| u8[]  | (pad)                  |

##### 5. Append blob tail

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u8    | (pad)                  |
| u16   | data length            |
| u32   | blob index             |
| u8[]  | data                   |
| u8[]  | (pad)                  |

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
| u16   | (pad)                  |
| u32   | blob index             |
| u64   | offset                 |
| u32   | compressed size        |
| u32   | (pad)                  |



### Blob

A blob has one or more zones attached.
The list of zones of each blob is kept entirely in-memory.



## Design rationale

### No encryption

The original design included encryption with authentication, but this has
been removed as it adds non-trivial complexity and is unlikely to offer
notable benefits for typical usage.

This blob store is designed for use with a locally attached disk.
Compromising a local disk requires physical access. There are a few scenarios:

1. The machine is stolen.
2. The machine is accessed while active.
3. The machine is accessed while offline.

In scenario 2 the user already lost. Scenario 3 could theoretically allow
attacks, but is contrived compared to more typical attacks.
A non-authenticated scheme is sufficient for scenario 1 provided the
contents of the disk are no longer trusted if it is recovered somehow.

For a remote/network disk authentication is necessary, but an object-oriented
protocol would be more convenient than a block protocol at such a level.
Hence, this scenario is not considered.
