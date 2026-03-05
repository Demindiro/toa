## Format

*All integers are in little-endian format.*

The store has two categories of files:

- Essential files
- Acceleration files, which can be (re)generated from the essential files.

The essential files are designed to be parseable with a simple linear scan.

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

The store is designed for simple implementations to use a single table
for mapping all hashes to file offsets.
File offsets are all aligned to 8 bytes,
leaving the 3 lower bits for type tagging.


### Full chunks file

The full chunks file is a large blob with a size a multiple of 8KiB.

Data and refs are separate as the hash domain differs.


### Partial chunks file

Each partial chunk is prefixed with a 16-bit length *in bits*.

All chunks start on a 8-byte boundary.


### Pairs file

The pairs file is a blob with a size a multiple of 80 bytes,
with each entry representing a pair of CVs and the total
size of chunks referenced.

| bytes | name               |
| -----:|:------------------ |
|  31:0 | left CV            |
| 63:32 | right CV           |
| 79:64 | length (bits)      |


### Roots file

The roots file is a blob with a size a multiple of 64 bytes.

| bytes | name                |
| -----:|:------------------- |
|  31:0 | data hash           |
| 63:32 | refs hash           |


### Small objects

The small objects file is not strictly necessary, but a useful optimization
to more compactly store small objects.

Each object starts with a 32-bit header.

| bits  | name                  |
| -----:|:--------------------- |
|  11:0 | refs length (hashes)  |
| 31:12 | data length (bytes)   |

The header is immediately followed by hashes, then bytes.

All objects start on a 8-byte boundary.
