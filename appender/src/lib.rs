#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![forbid(unsafe_code, unused_must_use, elided_named_lifetimes)]

extern crate alloc;

mod builder;
pub mod device;
pub mod object;
pub mod pack;
mod reader;
pub mod record;

pub use blake3;
pub use builder::{Builder, worker};
pub use chacha20poly1305::Key;
pub use reader::{Object, Reader, cache};
pub use toa_core::Hash;

use chacha20poly1305::Nonce;

const DEPTH: u8 = 3;
const PITCH: u8 = 17;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct PackRef(pub [u8; pack::Pack::LEN]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PackOffset(u64);

#[derive(Clone, Copy, Debug)]
pub struct ObjectRaw {
    offset: PackOffset,
    len: u64,
}

impl ObjectRaw {
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn offset(&self) -> u64 {
        self.offset.0
    }

    pub fn subslice(&self, offset: u64, len: u64) -> Option<ObjectRaw> {
        let start = self.offset.0.checked_add(offset)?;
        let end = start.checked_add(len)?;
        (end <= self.offset.0 + self.len).then_some(Self {
            offset: PackOffset(start),
            len,
        })
    }
}

fn record_nonce(depth: u32, index: u64) -> Nonce {
    let mut nonce = Nonce::default();
    nonce[8..].copy_from_slice(&depth.to_le_bytes());
    nonce[..8].copy_from_slice(&index.to_le_bytes());
    nonce
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{SeedableRng, rngs::StdRng};

    struct TestBuild {
        builder: Builder<Vec<u8>, worker::ForcedQueue<worker::Work>>,
    }

    struct TestRead {
        reader: Reader<Vec<u8>, cache::MicroLru<Box<[u8]>>>,
    }

    impl TestBuild {
        fn finish(self) -> TestRead {
            let (dev, pack) = self.builder.finish().expect("build finish failure");
            let pack = pack.expect("no objects committed");
            let reader = Reader::new(dev, Default::default(), pack).expect("corrupt pack");
            TestRead { reader }
        }

        fn add(&mut self, data: &[u8]) -> Hash {
            self.builder.add(data).expect("add failed")
        }
    }

    impl TestRead {
        fn assert_eq(&self, key: &Hash, value: &[u8]) {
            let x = self
                .reader
                .get(&key)
                .expect("get failed")
                .expect("object does not exist");
            let x = x.read_exact(0, usize::MAX).expect("read_exact failed");
            let x = x.into_bytes().expect("into_bytes failed");
            let f = String::from_utf8_lossy;
            assert!(&x == value, "{} <> {}", f(&x), f(value));
        }
    }

    fn init() -> TestBuild {
        let rng = StdRng::from_seed([0; 32]);
        let builder = Builder::new(Default::default(), Default::default(), rng);
        TestBuild { builder }
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

    #[test]
    fn insert_one_empty() {
        let mut s = init();
        let key = s.add(b"");
        let s = s.finish();
        s.assert_eq(&key, &[]);
    }

    #[test]
    fn insert_one() {
        let mut s = init();
        let key = s.add(b"Hello, world!");
        let s = s.finish();
        s.assert_eq(&key, b"Hello, world!");
    }

    #[test]
    fn insert_two() {
        let mut s = init();
        let a = s.add(b"Hello, world!");
        let b = s.add(b"Greetings!");
        let s = s.finish();
        s.assert_eq(&a, b"Hello, world!");
        s.assert_eq(&b, b"Greetings!");
    }

    #[test]
    fn insert_many() {
        let mut s = init();
        let f = |x| format!("A number {x}").into_bytes();
        let keys = (0..1 << 12).map(|i| s.add(&f(i))).collect::<Vec<_>>();
        let s = s.finish();
        keys.iter()
            .enumerate()
            .for_each(|(i, k)| s.assert_eq(k, &f(i)));
    }

    /// This test crosses record boundaries and is used in particular to test crypto nonce errors.
    #[test]
    fn insert_one_large() {
        let mut s = init();
        let v = (0..1 << 19)
            .fold(String::new(), |s, _| s + "x")
            .into_bytes();
        let k = s.add(&v);
        let s = s.finish();
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_one_large_zeros() {
        let mut s = init();
        let v = vec![0; 1 << 20];
        let k = s.add(&v);
        let s = s.finish();
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_many_large() {
        let n = 1 << 21;
        let mut s = init();
        let keys = (0..=255)
            .map(|x| (x, s.add(&vec![x; n])))
            .collect::<Vec<_>>();
        let s = s.finish();
        keys.iter().for_each(|(x, k)| s.assert_eq(k, &vec![*x; n]));
    }

    // TODO we need tests to ensure crypto works!
}
