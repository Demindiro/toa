use chacha20poly1305::{Tag, XNonce};
use core::mem;

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Entry {
    pub poly1305: Tag,
    pub nonce: XNonce,
    pub offset: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
}

impl Entry {
    pub const LEN: usize = 64;

    pub fn into_bytes(self) -> [u8; Self::LEN] {
        let mut buf = [0; Self::LEN];
        buf[0..16].copy_from_slice(self.poly1305.as_slice());
        buf[16..40].copy_from_slice(self.nonce.as_slice());
        buf[40..48].copy_from_slice(&self.offset.to_le_bytes());
        buf[48..52].copy_from_slice(&self.compressed_len.to_le_bytes());
        buf[52..56].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        buf
    }

    pub fn from_bytes(b: &[u8; Self::LEN]) -> Self {
        Self {
            poly1305: *Tag::from_slice(&b[0..16]),
            nonce: *XNonce::from_slice(&b[16..40]),
            offset: u64::from_le_bytes(b[40..48].try_into().unwrap()),
            compressed_len: u32::from_le_bytes(b[48..52].try_into().unwrap()),
            uncompressed_len: u32::from_le_bytes(b[52..56].try_into().unwrap()),
        }
    }
}
