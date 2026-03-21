This crate contains a storage system for large append-only blobs.

It is designed with SMR disks in mind (although the author has
great difficulty finding host-managed SMR disks). It does not
require any conventional zones.

Device settings are duplicated in the header. If there is a mismatch,
the store will refuse to mount.

The header is only partially encrypted. It points to zones which
are used as logs. The logs must be parsed entirely to reconstruct
in-memory state.

Blobs are append-only. To free memory a blob must be wiped entirely.
Blobs can be written to in byte-increments for convenience.
Incomplete blocks are written to the log until a whole block can
be appended to a zone.

Each blob has a unique ID. This ID is stable, ensuring an ID can
be used to refer directly to a blob. ID `0xffff_ffff` is reserved.

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

The log is written to the first and last zone.
On store load, both logs are scanned.
If there is a mismatch, fsck is required.

#### Entry types

All entries start with a 8-bit type ID.
All entries are aligned to a 8-byte boundary,
padded with zeros if necessary.

##### 0. Log block end

| type  | name                  |
|:----- |:--------------------- |
| u8    | (type)                |
| u56   | (zero)                |

A log block is terminated prematurely if this entry is encountered.

For non-zoned devices: if a block contains only this entry, it indicates
the end of the log.

##### 1. Create blob

The blob ID must not conflict with any other active blob IDs.

The blob ID should be as low as possible.

| type  | name                  |
|:----- |:--------------------- |
| u8    | (type)                |
| u8    | name length           |
| u16   | (pad)                 |
| u32   | blob ID               |
| u8[]  | name                  |
| u8[]  | (pad)                 |

##### 2. Delete blob

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u24   | (pad)                  |
| u32   | blob ID                |

##### 3. Add zone to blob

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u24   | (pad)                  |
| u32   | blob ID                |
| u64   | zone ID                |

##### 4. Rename blob

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u8    | name length            |
| u16   | (pad)                  |
| u32   | blob ID                |
| u8[]  | name                   |
| u8[]  | (pad)                  |

##### 5. Append blob tail

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u8    | (pad)                  |
| u16   | data length            |
| u32   | blob ID                |
| u8[]  | data                   |
| u8[]  | (pad)                  |

##### 6. Next log zone

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u24   | (pad)                  |
| u32   | zone ID                |

The switch to the next zone should be immediate.
No entries in the same zone should follow after this entry,
i.e. keep it all zeros.

The mirror zone **must** use a different zone ID.

##### 7. Commit blob tail

Takes a full blob tail and appends it to the main blob.
This automatically clears the tail buffer.

The length is the *total* length of the blob.
It must be strictly larger than the previous committed length.

The extra length may be larger than what is contained in the
current tail buffer, implying additional data. It cannot
be smaller however.

> **TODO** what if there is a power outage between
> writing the record and the log entry?

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u24   | (pad)                  |
| u32   | blob ID                |
| u64   | length                 |

##### 8. Create unzoned blob

Much like a regular blob, except this one will never allocate zones.
Instead, all data is kept in the tail.
Only recommended for blobs that will remain significantly shorter than
a single zone.

Blob ID allocation follows same recommendations as "1. Create blob".

| type  | name                  |
|:----- |:--------------------- |
| u8    | (type)                |
| u8    | name length           |
| u16   | (pad)                 |
| u32   | blob ID               |
| u8[]  | name                  |
| u8[]  | (pad)                 |

##### 9. Clear blob

Erase all data from a blob, releasing associated zones and clearing the tail.
Unlike delete, the blob and its ID is kept.

| type  | name                   |
|:----- |:---------------------- |
| u8    | (type)                 |
| u24   | (pad)                  |
| u32   | blob ID                |

##### 84. Header

Every log zone must start with this header.

Generation starts at 1 and increases monotonically every time
the log is rewritten.

| type  | name                   |
|:----- |:---------------------- |
| u32   | magic "ToaB"           |
| u32   | version 0x20260307     |
| u64   | generation             |
| u32   | block size             |
| u32   | zone blocks            |
| u32   | zone count             |
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

### No compression

The original design was intended to include support for compression,
but compression is ineffective if higher layers use encryption or their
own compression. It would also require a table to map offsets to record,
adding at least one disk seek of latency.

The main reason to handle tables was to avoid allocating a potentially
huge zone for storing a small table, but it should be possible to mitigate
this overhead using a tiered log.
