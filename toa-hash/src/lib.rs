#![cfg_attr(not(test), no_std)]
#[forbid(unused_must_use, unsafe_code)]
use core::fmt;
use sha3::{
    TurboShake128, TurboShake128Core,
    digest::{ExtendableOutput, Reset, Update, XofReader},
};

const DF_ROOT: u8 = 1 << 0;
const DF_DATA: u8 = 1 << 1;
const DF_REFS: u8 = 1 << 2;
const DF_LEAF: u8 = 1 << 3;

const CHUNK_SIZE: usize = 1 << 13;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
pub struct Hash(pub [u8; 32]);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
pub struct DataHash(Hash);
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
pub struct RefsHash(Hash);

#[derive(Clone)]
pub struct DataHasher(TreeHasher);
#[derive(Clone)]
pub struct RefsHasher(TreeHasher);

/// Chaining value
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
struct Cv(Hash);

#[derive(Clone)]
struct TreeHasher {
    stack: arrayvec::ArrayVec<Cv, { 128 - 13 }>,
    domain: u8,
    chunk: TurboShake128,
    len: u128,
}

impl Hash {
    pub fn slice_as_bytes(slice: &[Self]) -> &[[u8; 32]] {
        bytemuck::cast_slice(slice)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
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

impl DataHash {
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl RefsHash {
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl Default for DataHasher {
    fn default() -> Self {
        Self(TreeHasher::new(DF_DATA))
    }
}

impl Default for RefsHasher {
    fn default() -> Self {
        Self(TreeHasher::new(DF_REFS))
    }
}

impl DataHasher {
    pub fn update<T>(&mut self, data: T)
    where
        T: AsRef<[u8]>,
    {
        self.0.update(data.as_ref())
    }

    pub fn chain<T>(self, data: T) -> Self
    where
        T: AsRef<[u8]>,
    {
        Self(self.0.chain(data.as_ref()))
    }

    pub fn finalize(self) -> DataHash {
        DataHash(self.0.finalize().0)
    }
}

impl RefsHasher {
    pub fn update<T>(&mut self, data: T)
    where
        T: AsRef<[Hash]>,
    {
        self.0.update(bytemuck::cast_slice(data.as_ref()))
    }

    pub fn chain<T>(self, data: T) -> Self
    where
        T: AsRef<[Hash]>,
    {
        Self(self.0.chain(bytemuck::cast_slice(data.as_ref())))
    }

    pub fn finalize(self) -> RefsHash {
        RefsHash(self.0.finalize().0)
    }
}

impl TreeHasher {
    fn new(domain: u8) -> Self {
        assert!(domain == DF_DATA || domain == DF_REFS);
        Self {
            domain,
            stack: Default::default(),
            chunk: TurboShake128::from_core(TurboShake128Core::new(domain | DF_LEAF)),
            len: 0,
        }
    }

    fn update(&mut self, mut data: &[u8]) {
        while let Some((mut y, n)) = self.chunk_update(data) {
            data = &data[n..];
            while self.stack.len() >= self.chunk_pos().count_ones() as usize {
                let x = self.stack.pop().expect("chunk_pos() >= 1");
                y = ts_pair(self.domain, &[x, y]);
            }
            self.stack.push(y);
        }
    }

    fn chain(mut self, data: &[u8]) -> Self {
        self.update(data);
        self
    }

    fn finalize(mut self) -> Cv {
        let mut y = if !self.chunk_is_empty() {
            self.chunk_take()
        } else if let Some(y) = self.stack.pop() {
            y
        } else {
            return self.chunk_take();
        };
        while let Some(x) = self.stack.pop() {
            y = ts_pair(self.domain, &[x, y]);
        }
        y
    }

    /// # Returns
    ///
    /// `None` if the chunk isn't full, otherwise CV and amount of bytes consumed.
    fn chunk_update(&mut self, data: &[u8]) -> Option<(Cv, usize)> {
        if data.is_empty() {
            return None;
        }
        let n = data.len().min(CHUNK_SIZE - self.chunk_len());
        let data = &data[..n];
        self.chunk.update(data);
        self.len += n as u128;
        self.chunk_is_empty().then(|| (self.chunk_take(), n))
    }

    fn chunk_take(&mut self) -> Cv {
        let mut hash = [0; 32];
        self.chunk.clone().finalize_xof().read(&mut hash);
        self.chunk.reset();
        Cv(Hash(hash))
    }

    fn chunk_len(&self) -> usize {
        self.len as usize % CHUNK_SIZE
    }

    fn chunk_pos(&self) -> u128 {
        self.len / CHUNK_SIZE as u128
    }

    fn chunk_is_empty(&self) -> bool {
        self.chunk_len() == 0
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        core::str::from_utf8(&self.to_hex()).expect("ascii").fmt(f)
    }
}

impl fmt::Display for DataHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for RefsHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Debug for DataHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "data:{}", self.0)
    }
}

impl fmt::Debug for RefsHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "refs:{}", self.0)
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
    TreeHasher::new(domain).chain(data).finalize().0
}

fn ts_hash(domain: u8, data: &[u8]) -> Hash {
    let mut hash = [0; 32];
    TurboShake128::from_core(TurboShake128Core::new(domain))
        .chain(data)
        .finalize_xof()
        .read(&mut hash);
    Hash(hash)
}

fn ts_pair(domain: u8, cv: &[Cv; 2]) -> Cv {
    Cv(ts_hash(domain, cv.map(|x| x.0.0).as_flattened()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(x: Hash, y: Hash) -> Hash {
        ts_pair(DF_DATA, &[x, y].map(Cv)).0
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
    fn hash_tree_chunk_plus_one_byte() {
        let data = [b'x'; CHUNK_SIZE + 1];
        let a = ts_hash(DF_DATA | DF_LEAF, &data[..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE..]);
        assert_eq!(p(a, b), tree_hash(DF_DATA, &data));
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
