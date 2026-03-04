# Appender

Appender is an append-only object-/filesystem for immutable storage.


## Reasoning

TODO (the previous one was bad and rambly).


## Format

*All integers are in little-endian format.*

The store has two categories of files:

- Essential files
- Acceleration files, which can be (re)generated from the essential files.

Essential files are:

- Full data chunks file, with chunk data of 8KiB each.
- Full refs chunks file, with chunk refs of 8KiB each.
- Partial data chunks file, with chunk data strictly less than 8KiB.
- Partial refs chunks file, with chunk data strictly less than 8KiB.
- Data pairs file, containing pairs of 64 bytes each.
- Refs pairs file, containing pairs of 64 bytes each.
- Roots file, containing roots of 96 bytes each.
- Small objects, with root, data and references inline.

Note the lack of index files. These are considered acceleration files as they
can be generated from only the essential files.


### Full chunks file

The full chunks file is a large blob with a size a multiple of 8KiB.

Data and refs are separate as the hash domain differs.


### Partial chunks file

Each partial chunk is prefixed with a 16-bit length *in bits*.

All chunks start on a 8-byte boundary.


### Pairs file

The pairs file is a blob with a size a multiple of 64 bytes,
with each entry representing a pair of hashes.


### Roots file

The roots file is a blob with a size a multiple of 96 bytes.

| bytes | name                |
| -----:|:------------------- |
|  31:0 | data hash           |
| 63:32 | refs hash           |
| 79:64 | data length (bits)  |
| 95:80 | refs length (bits)  |


### Small objects

The small objects file is not strictly necessary, but a useful optimization
to more compactly store small objects.

Each object starts with a 32-bit header.

| bits  | name                  |
| -----:|:--------------------- |
|  20:0 | data length (bytes)   |
| 31:20 | refs length (hashes)  |

All objects start on a 8-byte boundary.
