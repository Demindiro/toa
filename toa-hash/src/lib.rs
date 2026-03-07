#![cfg_attr(not(test), no_std)]
#[forbid(unused_must_use, unsafe_code)]
use core::fmt;
use sha3::{
    TurboShake128, TurboShake128Core,
    digest::{ExtendableOutput, Reset, Update, XofReader},
};

const DF_DATA: u8 = 1;
const DF_REFS: u8 = 2;
const DF_PAIR: u8 = 3;

const CHUNK_SIZE: usize = 1 << 13;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Domain {
    Data = DF_DATA,
    Refs = DF_REFS,
}

/// Chaining value
#[derive(
    Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, bytemuck::Pod, bytemuck::Zeroable,
)]
#[repr(transparent)]
pub struct Hash([u8; 32]);

#[derive(Clone)]
pub struct TreeHasher {
    stack: arrayvec::ArrayVec<Hash, { 128 - 13 }>,
    chunk: TurboShake128,
    len: u128,
}

impl TreeHasher {
    fn new(domain: Domain) -> Self {
        Self {
            stack: Default::default(),
            chunk: TurboShake128::from_core(TurboShake128Core::new(domain as u8)),
            len: 0,
        }
    }

    fn update(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let (y, n) = self.chunk_update(data);
            data = &data[n..];
            if let Some(y) = y {
                let y = self.collapse(y);
                self.stack.push(y);
            }
        }
    }

    fn chain(mut self, data: &[u8]) -> Self {
        self.update(data);
        self
    }

    fn finalize(mut self) -> Hash {
        let len = self.len << 3;
        let mut y = self.chunk_take();
        let mut mask = 0xffff;
        let top_i = len.wrapping_sub(1); // special-case for len=0
        while let Some(x) = self.stack.pop() {
            debug_assert_eq!(
                (top_i & !mask).count_ones(),
                1 + self.stack.len() as u32,
                "length bits should correlate to stack depth"
            );
            let bits = (top_i & !mask).trailing_zeros();
            mask = (1 << (bits + 1)) - 1;
            let pair_len = (top_i & mask) + 1;
            y = hash_pair(x, y, pair_len);
        }
        y
    }

    /// # Returns
    ///
    /// - `None` if the chunk isn't full, otherwise Hash of chunk.
    /// - amount of bytes consumed.
    fn chunk_update(&mut self, data: &[u8]) -> (Option<Hash>, usize) {
        if data.is_empty() {
            return (None, 0);
        }
        let hash = self.chunk_is_full().then(|| self.chunk_take());
        let n = data.len().min(CHUNK_SIZE - self.chunk_len());
        debug_assert_ne!(n, 0, "some data should be consumed");
        let data = &data[..n];
        self.chunk.update(data);
        self.len += n as u128;
        (hash, n)
    }

    fn chunk_take(&mut self) -> Hash {
        let mut hash = [0; 32];
        self.chunk.clone().finalize_xof().read(&mut hash);
        self.chunk.reset();
        Hash(hash)
    }

    fn chunk_len(&self) -> usize {
        self.len as usize % CHUNK_SIZE
    }

    fn chunk_is_full(&self) -> bool {
        // 8192 = 0 (mod 8192)
        self.len > 0 && self.chunk_len() == 0
    }

    fn collapse(&mut self, mut y: Hash) -> Hash {
        let mut shift = 0;
        while self.stack.len() >= ((self.len - 1) & !0x1fff).count_ones() as usize {
            let x = self.stack.pop().expect("chunk_pos() >= 1");
            shift += 1;
            let len = ((CHUNK_SIZE as u128) << 3) << shift;
            y = hash_pair(x, y, len);
        }
        y
    }
}

impl Hash {
    pub fn slice_as_bytes(slice: &[Self]) -> &[[u8; 32]] {
        bytemuck::cast_slice(slice)
    }

    pub fn slice_as_bytes_mut(slice: &mut [Self]) -> &mut [[u8; 32]] {
        bytemuck::cast_slice_mut(slice)
    }

    pub fn slice_from_bytes(slice: &[[u8; 32]]) -> &[Self] {
        bytemuck::cast_slice(slice)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// # Panics
    ///
    /// If `bytes.len() != 32`.
    pub fn from_slice(bytes: &[u8]) -> Self {
        Self::from_bytes(bytes.try_into().expect("length of hash not 32 bytes"))
    }

    pub fn to_hex(&self) -> [u8; 64] {
        let mut b = [0; 64];
        for (w, x) in b.chunks_exact_mut(2).zip(self.0) {
            let f = |i| b"0123456789abcdef"[usize::from(i)];
            w[0] = f(x >> 4);
            w[1] = f(x & 15);
        }
        b
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        core::str::from_utf8(&self.to_hex()).expect("ascii").fmt(f)
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

pub fn hash(domain: Domain, data: &[u8]) -> Hash {
    TreeHasher::new(domain).chain(data).finalize()
}

/// # Panics
///
/// If there are more than `CHUNK_SIZE` bytes.
pub fn hash_chunk(domain: Domain, data: &[u8]) -> Hash {
    assert!(data.len() <= CHUNK_SIZE);
    ts_hash(domain as u8, data)
}

/// `len`: number of data *bits* of leaf nodes.
pub fn hash_pair(x: Hash, y: Hash, len: u128) -> Hash {
    let mut buf = [0; 80];
    buf[00..32].copy_from_slice(x.as_bytes());
    buf[32..64].copy_from_slice(y.as_bytes());
    buf[64..].copy_from_slice(&len.to_le_bytes());
    ts_hash(DF_PAIR, &buf)
}

fn ts_hash(domain: u8, data: &[u8]) -> Hash {
    let mut cv = [0; 32];
    TurboShake128::from_core(TurboShake128Core::new(domain))
        .chain(data)
        .finalize_xof()
        .read(&mut cv);
    Hash(cv)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(x: Hash, y: Hash, len: usize) -> Hash {
        hash_pair(x, y, (len as u128) << 3)
    }
    macro_rules! p {
        ($($f:ident $n:literal)*) => {
            $(fn $f(x: Hash, y: Hash) -> Hash {
                p(x, y, CHUNK_SIZE*$n)
            })*
        };
    }
    p!(p2 2 p3 3 p4 4 p5 5 p6 6 p7 7 p8 8 p9 9 p12 12);

    fn test_chunks<const N: usize, F>(f: F)
    where
        F: FnOnce([Hash; N]) -> Hash,
    {
        let mut t = [0; N];
        t.iter_mut().enumerate().for_each(|(i, x)| *x = i as u8);
        let t = t.map(|x| [x; CHUNK_SIZE]);
        let cv = t.each_ref().map(|x| ts_hash(DF_DATA, x));
        let expect = (f)(cv);
        let result = hash(Domain::Data, t.as_flattened());
        assert_eq!(result, expect);
    }

    #[test]
    fn hash_tree_empty() {
        test_chunks(|[]| ts_hash(DF_DATA, b""))
    }

    #[test]
    fn hash_tree_one_byte() {
        let expect = ts_hash(DF_DATA, b"x");
        let result = hash(Domain::Data, b"x");
        assert_eq!(result, expect);
    }

    #[test]
    fn hash_tree_chunk_plus_one_byte() {
        let data = [b'x'; CHUNK_SIZE + 1];
        let a = ts_hash(DF_DATA, &data[..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA, &data[CHUNK_SIZE..]);
        assert_eq!(p(a, b, CHUNK_SIZE + 1), hash(Domain::Data, &data));
    }
    #[test]
    fn hash_tree_2_chunks_minus_one_byte() {
        let data = [b'x'; 2 * CHUNK_SIZE - 1];
        let a = ts_hash(DF_DATA, &data[CHUNK_SIZE * 0..][..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA, &data[CHUNK_SIZE * 1..][..]);
        assert_eq!(p(a, b, 2 * CHUNK_SIZE - 1), hash(Domain::Data, &data));
    }
    #[test]
    fn hash_tree_2_chunks_plus_one_byte() {
        let data = [b'x'; 2 * CHUNK_SIZE + 1];
        let a = ts_hash(DF_DATA, &data[CHUNK_SIZE * 0..][..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA, &data[CHUNK_SIZE * 1..][..CHUNK_SIZE]);
        let c = ts_hash(DF_DATA, &data[CHUNK_SIZE * 2..][..]);
        assert_eq!(
            p(p2(a, b), c, 2 * CHUNK_SIZE + 1),
            hash(Domain::Data, &data)
        );
    }
    #[test]
    fn hash_tree_3_chunks_minus_one_byte() {
        let data = [b'x'; 3 * CHUNK_SIZE - 1];
        let a = ts_hash(DF_DATA, &data[CHUNK_SIZE * 0..][..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA, &data[CHUNK_SIZE * 1..][..CHUNK_SIZE]);
        let c = ts_hash(DF_DATA, &data[CHUNK_SIZE * 2..][..]);
        assert_eq!(
            p(p2(a, b), c, 3 * CHUNK_SIZE - 1),
            hash(Domain::Data, &data)
        );
    }
    #[test]
    fn hash_tree_3_chunks_plus_one_byte() {
        let data = [b'x'; 3 * CHUNK_SIZE + 1];
        let a = ts_hash(DF_DATA, &data[CHUNK_SIZE * 0..][..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA, &data[CHUNK_SIZE * 1..][..CHUNK_SIZE]);
        let c = ts_hash(DF_DATA, &data[CHUNK_SIZE * 2..][..CHUNK_SIZE]);
        let d = ts_hash(DF_DATA, &data[CHUNK_SIZE * 3..][..]);
        let ab = p2(a, b);
        let cd = p(c, d, CHUNK_SIZE + 1);
        let abcd = p(ab, cd, 3 * CHUNK_SIZE + 1);
        assert_eq!(abcd, hash(Domain::Data, &data));
    }

    #[test]
    fn hash_tree_1_chunk() {
        test_chunks(|[a]| a)
    }
    #[test]
    fn hash_tree_2_chunks() {
        test_chunks(|[a, b]| p2(a, b))
    }
    #[test]
    fn hash_tree_3_chunks() {
        test_chunks(|[a, b, c]| p(p2(a, b), c, 3 * CHUNK_SIZE))
    }
    #[test]
    fn hash_tree_4_chunks() {
        test_chunks(|[a, b, c, d]| p4(p2(a, b), p2(c, d)))
    }
    #[test]
    fn hash_tree_5_chunks() {
        test_chunks(|[a, b, c, d, e]| p5(p4(p2(a, b), p2(c, d)), e))
    }
    #[test]
    fn hash_tree_6_chunks() {
        test_chunks(|[a, b, c, d, e, f]| p6(p4(p2(a, b), p2(c, d)), p2(e, f)))
    }
    #[test]
    fn hash_tree_7_chunks() {
        test_chunks(|[a, b, c, d, e, f, g]| p7(p4(p2(a, b), p2(c, d)), p3(p2(e, f), g)))
    }
    #[test]
    fn hash_tree_8_chunks() {
        test_chunks(|[a, b, c, d, e, f, g, h]| p8(p4(p2(a, b), p2(c, d)), p4(p2(e, f), p2(g, h))))
    }
    #[test]
    fn hash_tree_9_chunks() {
        test_chunks(|[a, b, c, d, e, f, g, h, i]| {
            p9(p8(p4(p2(a, b), p2(c, d)), p4(p2(e, f), p2(g, h))), i)
        })
    }

    // Works fine because of TreeHasher::collapse()
    #[test]
    fn hash_tree_12_chunks() {
        test_chunks(|[a, b, c, d, e, f, g, h, i, j, k, l]| {
            let abcd = p4(p2(a, b), p2(c, d));
            let efgh = p4(p2(e, f), p2(g, h));
            let ijkl = p4(p2(i, j), p2(k, l));
            p12(p8(abcd, efgh), ijkl)
        })
    }

    // Doesn't work because there may be nodes missing between
    // root node and right-most subtree.
    // Apparently previous tests did not catch this despite expectations...
    // or maybe they did and I was being totally silly. I do remember having
    // issues before...
    //
    // In fact, there might be nodes missing at any level between subtrees
    // on the right side. This will be annoying to test...
    //
    // ```
    //                .----------o----------.
    //        .------o------.                '
    //    .--o--.         .--o--.         .--o--.
    // .-o-.   .-o-.   .-o-.   .-o-.   .-o-.   .-o-.
    // x   x   x   x   x   x   x   x   x   x   x   p
    // ```
    #[test]
    fn hash_tree_11_chunks_plus_one_byte() {
        let data = vec![b'x'; 11 * CHUNK_SIZE + 1];
        let f2 = |i: usize, n: usize| {
            let data = &data[CHUNK_SIZE * i..][..n];
            let a = ts_hash(DF_DATA, &data[CHUNK_SIZE * 0..][..CHUNK_SIZE]);
            let b = ts_hash(DF_DATA, &data[CHUNK_SIZE * 1..]);
            let x = p(a, b, n);
            assert_eq!(x, hash(Domain::Data, data));
            x
        };
        let f4 = |i: usize, n: usize| {
            let data = &data[CHUNK_SIZE * i..][..n];
            let ln = n.next_power_of_two() >> 1;
            let rn = n - ln;
            let ab = f2(i + 0, ln);
            let cd = f2(i + 2, rn);
            let x = p(ab, cd, n);
            assert_eq!(x, hash(Domain::Data, data));
            x
        };
        let abcd = f4(0, 4 * CHUNK_SIZE);
        let efgh = f4(4, 4 * CHUNK_SIZE);
        let ijkl = f4(8, 3 * CHUNK_SIZE + 1);
        assert_eq!(
            p(p8(abcd, efgh), ijkl, data.len()),
            hash(Domain::Data, &data)
        );
    }

    #[test]
    fn hash_to_hex() {
        assert_eq!(
            Hash([0; 32]).to_hex(),
            *b"0000000000000000000000000000000000000000000000000000000000000000"
        );
        assert_eq!(
            Hash([1; 32]).to_hex(),
            *b"0101010101010101010101010101010101010101010101010101010101010101"
        );
        assert_eq!(
            Hash([0xf7; 32]).to_hex(),
            *b"f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7"
        );
    }
}
