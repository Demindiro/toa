use crate::{Poly1305, record};
use chacha20poly1305::XNonce;
use core::mem;

#[repr(C)]
pub struct Snapshot {
    pub poly1305: Poly1305,
    pub nonce: XNonce,
    pub _reserved_0: [u64; 3],
    pub object_trie_root: u64,
    pub len: u64,
    pub _reserved_1: [u64; 2],
    pub record_trie_root: record::Entry,
}

const _: () = assert!(mem::size_of::<Snapshot>() == 128);

impl Snapshot {
    pub fn into_bytes(self) -> [u8; mem::size_of::<Snapshot>()] {
        let mut buf = [0; mem::size_of::<Snapshot>()];
        buf[..16].copy_from_slice(self.poly1305.as_slice());
        buf[16..40].copy_from_slice(self.nonce.as_slice());
        buf[64..72].copy_from_slice(&self.object_trie_root.to_le_bytes());
        buf[72..80].copy_from_slice(&self.len.to_le_bytes());
        buf[96..].copy_from_slice(&self.record_trie_root.into_bytes());
        buf
    }

    pub fn from_bytes(b: &[u8; mem::size_of::<Snapshot>()]) -> Self {
        Self {
            poly1305: *Poly1305::from_slice(&b[..16]),
            nonce: *XNonce::from_slice(&b[16..40]),
            _reserved_0: [0; 3],
            object_trie_root: u64::from_le_bytes(b[64..72].try_into().unwrap()),
            len: u64::from_le_bytes(b[72..80].try_into().unwrap()),
            _reserved_1: [0; 2],
            record_trie_root: record::Entry::from_bytes(b[96..].try_into().unwrap()),
        }
    }
}
