# TOA hash

`toa-hash` is a hash function designed to include both data and references
to other objects.
It is a tree hash using [TurboSHAKE128][turboshake128] as core.
It supports sparse files of up to 2^128 bits (2^125 bytes).

> **Warning** this hash is specifically designed for Toa.
> It is *not* suitable as a MAC due to the lack of a root flag,
> which is necessary for subtree-freeness (a non-goal of this hash).

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

### Determining length of a pair

This turns out to be tricky to get right, so the algorithm is described here.

Recall that the tree is a full binary tree. It is not balanced however.
Only the subtree on the left of the root is a perfect binary tree.
A stack can be used to construct the parent hashes with limited memory.

Determining when to collapse the stack is easy for a perfect binary tree
with full leaf nodes: Count the number of 1 bits in the length, then
keep merging values from the top of the stack until the stack depth
matches the number of bits.

Collapsing the stack for the right-most side of the tree is non-trivial and
easy to get wrong, since levels may be skipped when collapsing the stack.
Which levels to skip can be determined using only the total length.
The main idea is to exploit the need to match every right subtree with a
_perfect_ left subtree. A perfect subtree always has a length a power of 2.

1. Set `mask = 0xffff`
2. Apply `(not mask)` to the `top_i` (see below)
3. Count the number of trailing zero bits
4. Set `mask = ((1 << (bits + 1)) - 1)`
5. Merge 2 top stack elements with `pair_len = len & mask`
6. Repeat from (2) until stack is empty

With this scheme there is another edge case however: trees where the final
chunk is full. This should be handled by first subtracting epsilon from the
length. i.e. `top_i = length - ε`.

It is also important to **not eagerly commit full chunks to a perfect tree**.
See case 4.000, 6.000 and 12.000 in the examples below.


Examples:

```
floor(1.xxx - ε) = 1 = 0b1
.-t-.   1
x   p
^   ^

floor(2.xxx - ε) = 2 = 0b10
  .---t-.    1
.-o-.   '    0
x   x   p
  ^     ^

floor(3.000 - ε) = 2 = 0b10
  .---t-.     1
.-o-.   '     0
x   x   x
  ^     ^

floor(3.xxx - ε) = 3 = 0b11
  .---t---.     1
.-o-.   .-t-.   1
x   x   x   p
  ^     ^   ^

floor(4.000 - ε) = 3 = 0b11
  .---t---.     1
.-o-.   .-t-.   1     !!!
x   x   x   x
  ^     ^   ^

floor(4.xxx - ε) = 4 = 0b100
      .-------t-.   1
  .---o---.     |   0
.-o-.   .-o-.   '   0
x   x   x   x   p
      ^       ^

floor(5.xxx - ε) = 5 = 0b101
      .-----t-----.         1
  .---o---.       '         0
.-o-.   .-o-.   .-t-.       1
x   x   x   x   x   p
      ^         ^   ^

floor(6.000 - ε) = 5 = 0b101
      .-------t---.         1
  .---o---.       '         0
.-o-.   .-o-.   .-t-.       1        !!!
x   x   x   x   x   x
      ^         ^   ^

floor(6.xxx - ε) = 6 = 0b110
      .-------t------.      1
  .---o---.       .--t--.   1
.-o-.   .-o-.   .-o-.   '   0
x   x   x   x   x   x   p

floor(7.xxx - ε) = 7 = 0b111
      .-------t-------.         1
  .---o---.       .---t---.     1
.-o-.   .-o-.   .-o-.   .-t-.   1
x   x   x   x   x   x   x   p

floor(11.xxx - ε) = 11 = 0b1011
	          .---------------t-------.        1
      .-------o-------.               '        0
  .---o---.       .---o---.       .---t---.    1
.-o-.   .-o-.   .-o-.   .-o-.   .-o-.   .-t-.  1
x   x   x   x   x   x   x   x   x   x   x   p
              ^                   ^     ^   ^

floor(12.000 - ε) = 11 = 0b1011
	          .---------------t-------.        1
      .-------o-------.               '        0
  .---o---.       .---o---.       .---t---.    1
.-o-.   .-o-.   .-o-.   .-o-.   .-o-.   .-t-.  1        !!!
x   x   x   x   x   x   x   x   x   x   x   x
              ^                   ^     ^   ^
```
