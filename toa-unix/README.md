# UNIX-TOA utilities

## Directory format

As directories are primarily read by programs,
it is in a binary format.

For every entry there is a 32-byte descriptor at the start of the data blob.

| bytes | short description |
| -----:|:----------------- |
|   1:0 | type+permissions  |
|   2:2 | name length       |
|   7:3 | length            |
|  11:8 | UID               |
| 15:12 | GID               |
| 23:16 | name offset       |
| 31:24 | modified time     |

If the length is 0 then the real length must be found by looking up the object.

| bits | short description |
| ----:|:----------------- |
|  8:0 | permissions       |
| 10:9 | type              |

Type 0 is a regular file, type 1 is a directory and type 2 is a symbolic link.

All item data is stored in objects, linked by the directory.

Names are stored after the descriptor array.

Modified time is in terms of microseconds.

Entries MUST be sorted by name.
Names blob MUST be in order of the entries.

> This increases the chances of identical directories being deduplicated.

The paths MUST be valid UTF-8.

The special entries `.` and `..` are never included.
