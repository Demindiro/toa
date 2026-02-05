#![cfg_attr(not(test), no_std)]
#[forbid(unused_must_use, unsafe_code)]

use core::fmt;
use sha3::{
    TurboShake128, TurboShake128Core,
    digest::{ExtendableOutput, Update, XofReader},
};

const DF_ROOT: u8 = 1 << 0;
const DF_DATA: u8 = 1 << 1;
const DF_REFS: u8 = 1 << 2;
const DF_LEAF: u8 = 1 << 3;

const CHUNK_SIZE: usize = 1 << 13;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
pub struct Hash([u8; 32]);

impl Hash {
    pub fn slice_as_bytes(slice: &[Self]) -> &[[u8; 32]] {
        bytemuck::cast_slice(slice)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = &mut [0; 64];
        for (w, r) in s.chunks_exact_mut(2).zip(self.0) {
            w[0] = b"0123456789abcdef"[usize::from(r >> 4)];
            w[1] = b"0123456789abcdef"[usize::from(r & 15)];
        }
        core::str::from_utf8(s).expect("ascii").fmt(f)
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

pub fn hash(data: &[u8], refs: &[Hash]) -> Hash {
    let refs = Hash::slice_as_bytes(refs).as_flattened();
    let data_len = (data.len() as u128) << 3;
    let refs_len = (refs.len() as u128) << 3;
    let data = tree_hash(DF_DATA, data);
    let refs = tree_hash(DF_REFS, refs);
    root_hash(data, refs, data_len, refs_len)
}

fn root_hash(data: Hash, refs: Hash, data_len: u128, refs_len: u128) -> Hash {
    let mut b = [0; 96];
    b[..32].copy_from_slice(data.as_bytes());
    b[32..64].copy_from_slice(refs.as_bytes());
    b[64..80].copy_from_slice(&data_len.to_le_bytes());
    b[80..].copy_from_slice(&refs_len.to_le_bytes());
    ts_hash(DF_ROOT, &b)
}

/// `domain` must be either `DF_DATA` or `DF_REFS`.
fn tree_hash(domain: u8, data: &[u8]) -> Hash {
    assert!(domain == DF_DATA || domain == DF_REFS);
    if data.is_empty() {
        return ts_hash(domain | DF_LEAF, &[]);
    }
    let mut stack = [Hash([0; 32]); 52];
    let mut stack_i = 0usize;
    for (i, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
        let mut x = ts_hash(domain | DF_LEAF, chunk);
        while stack_i >= (i + 1).count_ones() as usize {
            stack_i -= 1;
            x = ts_pair(domain, &[stack[stack_i], x]);
        }
        stack[stack_i] = x;
        stack_i += 1;
    }
    stack_i -= 1;
    while stack_i > 0 {
        stack_i -= 1;
        let [x, y] = stack[stack_i..][..2] else {
            unreachable!()
        };
        stack[stack_i] = ts_pair(domain, &[x, y]);
    }
    stack[0]
}

fn ts_hash(domain: u8, data: &[u8]) -> Hash {
    let mut hash = [0; 32];
    TurboShake128::from_core(TurboShake128Core::new(domain))
        .chain(data)
        .finalize_xof()
        .read(&mut hash);
    Hash(hash)
}

fn ts_pair(domain: u8, cv: &[Hash; 2]) -> Hash {
    ts_hash(domain, cv.map(|x| x.0).as_flattened())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(x: Hash, y: Hash) -> Hash {
        ts_pair(DF_DATA, &[x, y])
    }

    fn test_chunks<const N: usize, F>(f: F)
    where
        F: FnOnce([Hash; N]) -> Hash,
    {
        let mut t = [0; N];
        t.iter_mut().enumerate().for_each(|(i, x)| *x = i as u8);
        let t = t.map(|x| [x; CHUNK_SIZE]);
        let cv = t.each_ref().map(|x| ts_hash(DF_DATA | DF_LEAF, x));
        let expect = (f)(cv);
        let result = tree_hash(DF_DATA, t.as_flattened());
        assert_eq!(result, expect);
    }

    #[test]
    fn hash_tree_empty() {
        test_chunks(|[]| ts_hash(DF_DATA | DF_LEAF, b""))
    }

    #[test]
    fn hash_tree_one_byte() {
        let expect = ts_hash(DF_DATA | DF_LEAF, b"x");
        let result = tree_hash(DF_DATA, b"x");
        assert_eq!(result, expect);
    }

    #[test]
    fn hash_tree_1_chunk() {
        test_chunks(|[a]| a)
    }
    #[test]
    fn hash_tree_2_chunks() {
        test_chunks(|[a, b]| p(a, b))
    }
    #[test]
    fn hash_tree_3_chunks() {
        test_chunks(|[a, b, c]| p(p(a, b), c))
    }
    #[test]
    fn hash_tree_4_chunks() {
        test_chunks(|[a, b, c, d]| p(p(a, b), p(c, d)))
    }
    #[test]
    fn hash_tree_5_chunks() {
        test_chunks(|[a, b, c, d, e]| p(p(p(a, b), p(c, d)), e))
    }
    #[test]
    fn hash_tree_6_chunks() {
        test_chunks(|[a, b, c, d, e, f]| p(p(p(a, b), p(c, d)), p(e, f)))
    }
    #[test]
    fn hash_tree_7_chunks() {
        test_chunks(|[a, b, c, d, e, f, g]| p(p(p(a, b), p(c, d)), p(p(e, f), g)))
    }
    #[test]
    fn hash_tree_8_chunks() {
        test_chunks(|[a, b, c, d, e, f, g, h]| p(p(p(a, b), p(c, d)), p(p(e, f), p(g, h))))
    }
    #[test]
    fn hash_tree_9_chunks() {
        test_chunks(|[a, b, c, d, e, f, g, h, i]| p(p(p(p(a, b), p(c, d)), p(p(e, f), p(g, h))), i))
    }
}
