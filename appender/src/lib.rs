#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![forbid(unsafe_code, unused_must_use, elided_named_lifetimes)]

extern crate alloc;

mod builder;
pub mod device;
pub mod object;
pub mod pack;
mod reader;
pub mod record;

pub use builder::Builder;
pub use chacha20poly1305::Key;
pub use reader::{Object, Reader, cache};

use chacha20poly1305::Nonce;
use core::fmt;

const DEPTH: u8 = 3;
const PITCH: u8 = 17;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Hash(pub [u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct PackRef(pub [u8; pack::Pack::LEN]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PackOffset(u64);

#[derive(Clone, Copy, Debug)]
struct ObjectPointer {
    offset: PackOffset,
    len: u64,
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|x| write!(f, "{x:02x}"))
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
        builder: Builder<Vec<u8>>,
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
            let mut x = self
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
        let builder = Builder::new(Default::default(), rng);
        TestBuild { builder }
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

    // TODO we need tests to ensure crypto works!
}
