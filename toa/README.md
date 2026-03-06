## Format

*All integers are in little-endian format.*

The store has two categories of files:

- Essential files
- Acceleration files, which can be (re)generated from the essential files.

The essential files are designed to be parseable with a simple linear scan.

The store is split in two identical parts, one for data objects
and another for refs objects. Each store has three essential files:

- Full chunks file, with chunk data of 8KiB each.
- Partial chunks file, with chunk data strictly less than 8KiB.
- Pairs file, containing pairs of 80 bytes each.

Note the lack of index files. These are considered acceleration files as they
can be generated from only the essential files.

The store is designed for simple implementations to use a single table
for mapping all hashes to file offsets.
File offsets are all aligned to 8 bytes,
leaving the 3 lower bits for type tagging.

Lastly, exactly one root can be kept track of at any time.

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
