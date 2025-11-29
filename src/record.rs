use crate::Poly1305;
use core::mem;

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Entry {
    pub offset: u64,
    pub compression_info: CompressionInfo,
    pub uncompressed_len: u32,
    pub poly1305: Poly1305,
}

const _: () = assert!(mem::size_of::<Entry>() == 32);

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct CompressionInfo(u32);

#[derive(Clone, Copy)]
pub enum CompressionAlgorithm {
    N1,
    N2,
    N3,
}

impl CompressionInfo {
    pub fn new_uncompressed(len: u32) -> Option<Self> {
        (len < 1 << 30).then(|| Self(len << 2))
    }

    pub fn algorithm(&self) -> Option<CompressionAlgorithm> {
        match self.0 & 3 {
            0 => None,
            1 => Some(CompressionAlgorithm::N1),
            2 => Some(CompressionAlgorithm::N2),
            3 => Some(CompressionAlgorithm::N3),
            _ => unreachable!(),
        }
    }

    pub fn len(&self) -> u32 {
        self.0 >> 2
    }
}

impl Entry {
    pub fn into_bytes(self) -> [u8; 32] {
        let mut buf = [0; 32];
        buf[..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.compression_info.0.to_le_bytes());
        buf[12..16].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        buf[16..].copy_from_slice(self.poly1305.as_slice());
        buf
    }

    pub fn from_bytes(b: &[u8; 32]) -> Self {
        Self {
            offset: u64::from_le_bytes(b[..8].try_into().unwrap()),
            compression_info: CompressionInfo(u32::from_le_bytes(b[8..12].try_into().unwrap())),
            uncompressed_len: u32::from_le_bytes(b[12..16].try_into().unwrap()),
            poly1305: *Poly1305::from_slice(&b[16..]),
        }
    }
}
