pub mod builder;
pub mod reader;

use crate::Hash;
use core::mem;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Nibble(u8);
#[derive(Clone, Copy, Debug)]
struct NibbleIndex(u8);

#[repr(C)]
struct Leaf2 {
    hash: [u8; 32],
    offset: u64,
    len: u64,
}

const _: () = assert!(mem::size_of::<Leaf2>() == 48);

impl Leaf2 {
    fn into_bytes(self) -> [u8; 48] {
        let mut buf = [0; 48];
        buf[..32].copy_from_slice(&self.hash);
        buf[32..40].copy_from_slice(&self.offset.to_le_bytes());
        buf[40..].copy_from_slice(&self.len.to_le_bytes());
        buf
    }

    fn from_bytes(b: &[u8; 48]) -> Self {
        Self {
            hash: b[..32].try_into().unwrap(),
            offset: u64::from_le_bytes(b[32..40].try_into().unwrap()),
            len: u64::from_le_bytes(b[40..].try_into().unwrap()),
        }
    }
}

impl NibbleIndex {
    fn get(&self, key: &Hash) -> Nibble {
        debug_assert!(self.0 & 3 == 0, "{}", self.0);
        let (i, b) = (self.0 >> 3, (self.0 & 4) ^ 4);
        Nibble((key.0[usize::from(i)] >> b) & 0xf)
    }

    fn next(self) -> Self {
        Self(self.0 + 4)
    }
}
