# TOA hash

`toa-hash` is a hash function designed to include both data and references
to other objects.
It is a tree hash using [TurboSHAKE128][turboshake128] as core.
It supports sparse files of up to 2^128 bits (2^125 bytes).

## Specification

There are two objects:

- "data objects", consisting of plain bytes.
- "reference objects", consisting of references to other objects.

Each blob is hashed using the same tree hash algorithm,
producing a 256-bit hash for each blob.
However, the leaf nodes use domain flags to differentiate between
data chunks, reference chunks and pairs (parent nodes).

> **Warning**
>
> To verify a root hash, the hash of both blobs must be verified too.
> This is because an XOR is trivially reversible.
> Consider:
>
>     Root = D xor E
>
> If an attacker wishes to substitute `D` with `D'` one simply calculates
> `E'` such that:
>
>     E' = D' xor D xor E
>
>     D' xor E'  =  D' xor D' xor D xor E  =  D xor E  =  Root
>
> Verifying both D and E thwarts this attack, as it is impossible to find
> a preimage of E'.

### Tree hash

A blob is split into chunks of 8192 bytes each,
with the last chunk being 8192 bytes or less.
Each chunk is hashed individually with `D = DF_LEAF | (DF_DATA or DF_REFS)`,
producing a "Chaining Value" (CV).
Each pair of CVs is combined and suffixed with the total size of referenced
chunks in bits, then  hashed with `D = DF_DATA or DF_REFS`,
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


#### Example: hashing a blob of 4O000 bytes (320000 bits)

```
data = A || B || C || D || E

|A| = 65536 bits
|B| = 65536 bits
|C| = 65536 bits
|D| = 65536 bits
|E| =  7232 bits

a = H_chunk(A)
b = H_chunk(B)
c = H_chunk(C)
d = H_chunk(D)
e = H_chunk(E)

t = H_pair(a, b, 65536 * 2)
u = H_pair(c, d, 65536 * 2)
v = H_pair(t, u, 65536 * 4)
w = H_pair(v, e, 65536 * 4 + 7232)

H(data) = w

        w
       / \
      v   \
     / \   \
    /   \   \
   t     u   \
  / \   / \   \
 a   b c   d   e
```


### Domain flags

- `DF_REFS = 1`
- `DF_LEAF = 2`
- `DF_PAIR = 3`

[turboshake128]: https://keccak.team/files/TurboSHAKE.pdf



## Design notes

Documenting some hard decisions:

### Combined data+references versus separate data & reference objects

Just about any object type in any language is able to contain both plain data
and references to other objects. It would make sense then to support the same.
However, due to the need for precise scanning it is necessary to know which
parts of an object are references and which plain data. An easy way to achieve
this is to group data together and references together, then simply tracking
the length of each group.

The next and seemingly under-explored problem is how to design a hash that
accounts for both of these groups. Domain separation is essential, so
if using a tree hash these groups need to be hashed separately.
Then both hashes need to be combined.

An alternate approach is to allow only two types of objects:

- An object which consists solely of data.
- An object which consists solely of references.

An object that needs both data and references can have its first reference
point to a blob of data. This structure is not unlike LISPs, which have
symbols/strings and lists.

The former approach is theoretically more pure, as only one object type needs
to exist and objects in most programming languages map more directly to it,
but *the latter approach does not require combining hashes* of two trees.
At least with the approaches the author attempted, this saves a non-trivial
amount of complexity.
