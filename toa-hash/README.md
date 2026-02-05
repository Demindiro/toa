# TOA hash

`toa-hash` is a hash function designed to include both data and references
to other objects.
It is a tree hash using [TurboSHAKE128][turboshake128] as core.
It supports sparse files of up to 2^128 bits (2^125 bytes).

## Specification

An object consists of two parts:

- a blob of binary data
- a blob of references

The data and references blob are individually hashed,
then aggregated in a 96-byte root structure which is then hashed
with `D = DF_ROOT`.

| bytes | description                                   |
| -----:|:--------------------------------------------- |
|  31:0 | binary data hash                              |
| 63:31 | references hash                               |
| 79:64 | binary data length in bits (little-endian)    |
| 95:80 | references length in bits (little-endian)     |

The blobs are then split into chunks of 8192 bytes each.
Each chunk is hashed individually with `D = DF_LEAF | (DF_DATA or DF_REFS)`,
producing a "Chaining Value" (CV).
Each pair of CVs is combined and hashed with `D = DF_DATA or DF_REFS`,
producing a new CV, forming a complete binary tree.

```
  D0      CV           CV             CV
         /  \         /  \           /  \
       D0    D1     CV    D2       CV    CV              ...
                   / \            / \    / \
                 D0  D1         DO  D1  D2  D3
```

> 8192 bytes is chosen as chunk size as 1 - (8192 / (168 * 49)) is 0.0049,
> i.e. less than 1% overhead.
> Smaller chunk sizes have more than 1% overhead.


### Domain flags

- `DF_ROOT = 1 << 0`
- `DF_DATA = 1 << 1`
- `DF_REFS = 1 << 2`
- `DF_LEAF = 1 << 3`

[turboshake128]: https://keccak.team/files/TurboSHAKE.pdf
