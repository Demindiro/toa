use crate::record;
use core::mem;

#[repr(C)]
pub struct Snapshot {
    pub poly1305: u128,
    pub object_trie_root: u64,
    pub len: u64,
    pub record_trie_root: record::Entry,
}

const _: () = assert!(mem::size_of::<Snapshot>() == 64);

impl Snapshot {
    pub fn into_bytes(self) -> [u8; 64] {
        let mut buf = [0; 64];
        buf[..16].copy_from_slice(&self.poly1305.to_le_bytes());
        buf[16..24].copy_from_slice(&self.object_trie_root.to_le_bytes());
        buf[24..32].copy_from_slice(&self.len.to_le_bytes());
        buf[32..].copy_from_slice(&self.record_trie_root.into_bytes());
        buf
    }

    pub fn from_bytes(b: &[u8; 64]) -> Self {
        Self {
            poly1305: u128::from_le_bytes(b[..16].try_into().unwrap()),
            object_trie_root: u64::from_le_bytes(b[16..24].try_into().unwrap()),
            len: u64::from_le_bytes(b[24..32].try_into().unwrap()),
            record_trie_root: record::Entry::from_bytes(b[32..].try_into().unwrap()),
        }
    }
}
