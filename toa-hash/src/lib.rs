#![cfg_attr(not(test), no_std)]
#[forbid(unused_must_use, unsafe_code)]
use core::fmt;
use sha3::{
    TurboShake128, TurboShake128Core,
    digest::{ExtendableOutput, Reset, Update, XofReader},
};

const DF_DATA: u8 = 1 << 0;
const DF_REFS: u8 = 1 << 1;
const DF_LEAF: u8 = 1 << 2;

const CHUNK_SIZE: usize = 1 << 13;

#[derive(Clone)]
pub struct DataHasher(TreeHasher);
#[derive(Clone)]
pub struct RefsHasher(TreeHasher);

/// Chaining value
#[derive(
    Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, bytemuck::Pod, bytemuck::Zeroable,
)]
#[repr(transparent)]
struct Cv([u8; 32]);

#[derive(Clone)]
struct TreeHasher {
    stack: arrayvec::ArrayVec<Cv, { 128 - 13 }>,
    domain: u8,
    chunk: TurboShake128,
    len: u128,
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

    pub fn finalize(self) -> DataCv {
        DataCv(self.0.finalize())
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

    pub fn finalize(self) -> RefsCv {
        RefsCv(self.0.finalize())
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
            y = self.collapse(y);
            self.stack.push(y);
        }
    }

    fn chain(mut self, data: &[u8]) -> Self {
        self.update(data);
        self
    }

    fn finalize(mut self) -> Cv {
        let len = self.len << 3;
        let mut y = if !self.chunk_is_empty() {
            self.chunk_take()
        } else if let Some(y) = self.stack.pop() {
            y
        } else {
            return self.chunk_take();
        };
        let d = len.next_power_of_two().trailing_zeros();
        let d = d as usize - self.stack.len();
        let mut mask = !((1 << d) - 1);
        while let Some(x) = self.stack.pop() {
            mask <<= 1;
            y = ts_pair(self.domain, x, y, len & !mask);
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
        Cv(hash)
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

    fn collapse(&mut self, mut y: Cv) -> Cv {
        let mut shift = 0;
        while self.stack.len() >= self.chunk_pos().count_ones() as usize {
            let x = self.stack.pop().expect("chunk_pos() >= 1");
            shift += 1;
            let len = ((CHUNK_SIZE as u128) << 3) << shift;
            y = ts_pair(self.domain, x, y, len);
        }
        y
    }
}

macro_rules! impl_hash {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            bytemuck::Pod,
            bytemuck::Zeroable,
        )]
        #[repr(transparent)]
        pub struct $name(Cv);

        impl $name {
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
                self.0.as_bytes()
            }

            pub fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(Cv(bytes))
            }

            /// # Panics
            ///
            /// If `bytes.len() != 32`.
            pub fn from_slice(bytes: &[u8]) -> Self {
                Self::from_bytes(bytes.try_into().expect("length of hash not 32 bytes"))
            }

            pub fn to_hex(&self) -> [u8; 64] {
                self.0.to_hex()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

impl_hash!(Hash);
impl_hash!(DataCv);
impl_hash!(RefsCv);

impl Cv {
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

impl fmt::Display for Cv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        core::str::from_utf8(&self.to_hex()).expect("ascii").fmt(f)
    }
}

impl fmt::Debug for Cv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

pub fn hash(data: &[u8], refs: &[Hash]) -> Hash {
    let refs = Hash::slice_as_bytes(refs).as_flattened();
    let data = DataCv(tree_hash(DF_DATA, data));
    let refs = RefsCv(tree_hash(DF_REFS, refs));
    root_hash(data, refs)
}

pub fn root_hash(data: DataCv, refs: RefsCv) -> Hash {
    let x = data.as_bytes().iter();
    let y = refs.as_bytes().iter();
    let mut z = [0; 32];
    for ((x, y), z) in x.zip(y).zip(z.iter_mut()) {
        *z = x ^ y;
    }
    Hash(Cv(z))
}

/// # Panics
///
/// If there are more than `CHUNK_SIZE` bytes.
pub fn data_chunk_cv<T>(data: T) -> DataCv
where
    T: AsRef<[u8]>,
{
    let data = data.as_ref();
    assert!(data.len() <= CHUNK_SIZE);
    DataCv(ts_hash(DF_DATA | DF_LEAF, data))
}

/// # Panics
///
/// If there are more than `CHUNK_SIZE / 32` items.
pub fn refs_chunk_cv<T>(refs: T) -> RefsCv
where
    T: AsRef<[Hash]>,
{
    let refs = refs.as_ref();
    assert!(refs.len() <= CHUNK_SIZE / 32);
    RefsCv(ts_hash(DF_REFS | DF_LEAF, bytemuck::cast_slice(refs)))
}

pub fn data_pair_cv(x: DataCv, y: DataCv, len: u128) -> DataCv {
    DataCv(ts_pair(DF_DATA, x.0, y.0, len))
}

pub fn refs_pair_cv(x: RefsCv, y: RefsCv, len: u128) -> RefsCv {
    RefsCv(ts_pair(DF_REFS, x.0, y.0, len))
}

/// `domain` must be either `DF_DATA` or `DF_REFS`.
fn tree_hash(domain: u8, data: &[u8]) -> Cv {
    TreeHasher::new(domain).chain(data).finalize()
}

fn ts_hash(domain: u8, data: &[u8]) -> Cv {
    let mut cv = [0; 32];
    TurboShake128::from_core(TurboShake128Core::new(domain))
        .chain(data)
        .finalize_xof()
        .read(&mut cv);
    Cv(cv)
}

/// `len`: number of data bits of leaf nodes.
fn ts_pair(domain: u8, x: Cv, y: Cv, len: u128) -> Cv {
    let mut buf = [0; 80];
    buf[00..32].copy_from_slice(x.as_bytes());
    buf[32..64].copy_from_slice(y.as_bytes());
    buf[64..].copy_from_slice(&len.to_le_bytes());
    ts_hash(domain, &buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(x: Cv, y: Cv, len: usize) -> Cv {
        ts_pair(DF_DATA, x, y, (len as u128) << 3)
    }
    macro_rules! p {
        ($($f:ident $n:literal)*) => {
            $(fn $f(x: Cv, y: Cv) -> Cv {
                p(x, y, CHUNK_SIZE*$n)
            })*
        };
    }
    p!(p2 2 p3 3 p4 4 p5 5 p6 6 p7 7 p8 8 p9 9);

    fn test_chunks<const N: usize, F>(f: F)
    where
        F: FnOnce([Cv; N]) -> Cv,
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
        assert_eq!(p(a, b, CHUNK_SIZE + 1), tree_hash(DF_DATA, &data));
    }
    #[test]
    fn hash_tree_2_chunks_minus_one_byte() {
        let data = [b'x'; 2 * CHUNK_SIZE - 1];
        let a = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 0..][..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 1..][..]);
        assert_eq!(p(a, b, 2 * CHUNK_SIZE - 1), tree_hash(DF_DATA, &data));
    }
    #[test]
    fn hash_tree_2_chunks_plus_one_byte() {
        let data = [b'x'; 2 * CHUNK_SIZE + 1];
        let a = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 0..][..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 1..][..CHUNK_SIZE]);
        let c = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 2..][..]);
        assert_eq!(
            p(p2(a, b), c, 2 * CHUNK_SIZE + 1),
            tree_hash(DF_DATA, &data)
        );
    }
    #[test]
    fn hash_tree_3_chunks_minus_one_byte() {
        let data = [b'x'; 3 * CHUNK_SIZE - 1];
        let a = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 0..][..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 1..][..CHUNK_SIZE]);
        let c = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 2..][..]);
        assert_eq!(
            p(p2(a, b), c, 3 * CHUNK_SIZE - 1),
            tree_hash(DF_DATA, &data)
        );
    }
    #[test]
    fn hash_tree_3_chunks_plus_one_byte() {
        let data = [b'x'; 3 * CHUNK_SIZE + 1];
        let a = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 0..][..CHUNK_SIZE]);
        let b = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 1..][..CHUNK_SIZE]);
        let c = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 2..][..CHUNK_SIZE]);
        let d = ts_hash(DF_DATA | DF_LEAF, &data[CHUNK_SIZE * 3..][..]);
        let ab = p2(a, b);
        let cd = p(c, d, CHUNK_SIZE + 1);
        let abcd = p(ab, cd, 3 * CHUNK_SIZE + 1);
        assert_eq!(abcd, tree_hash(DF_DATA, &data));
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

    #[test]
    fn cv_to_hex() {
        assert_eq!(
            Cv([0; 32]).to_hex(),
            *b"0000000000000000000000000000000000000000000000000000000000000000"
        );
        assert_eq!(
            Cv([1; 32]).to_hex(),
            *b"0101010101010101010101010101010101010101010101010101010101010101"
        );
        assert_eq!(
            Cv([0xf7; 32]).to_hex(),
            *b"f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7"
        );
    }
}
