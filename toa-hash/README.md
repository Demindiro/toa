# TOA hash

`toa-hash` is a hash function designed to include both data and references
to other objects.
It is a tree hash using [TurboSHAKE128][turboshake128] as core.
It supports sparse files of up to 2^128 bits (2^125 bytes).

## Specification

An object consists of two parts:

- a blob of binary data
- a blob of references

Each blob is hashed using the same tree hash algorithm,
producing a 256-bit hash for each blob.
The two hashes are XORed to produce a single root hash.

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

- `DF_DATA = 1 << 0`
- `DF_REFS = 1 << 1`
- `DF_LEAF = 1 << 2`

[turboshake128]: https://keccak.team/files/TurboSHAKE.pdf
