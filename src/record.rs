use core::mem;

#[repr(C)]
pub struct Entry {
    pub offset: u64,
    pub compression_info: CompressionInfo,
    pub uncompressed_len: u32,
    pub poly1305: u128,
}

const _: () = assert!(mem::size_of::<Entry>() == 32);

#[repr(transparent)]
pub struct CompressionInfo(u32);

pub enum CompressionAlgorithm {
    N1,
    N2,
    N3,
}

impl CompressionInfo {
    pub fn new_uncompressed(length: u32) -> Option<Self> {
        (length < 1 << 30).then(|| Self(length << 2))
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
        buf[16..].copy_from_slice(&self.poly1305.to_le_bytes());
        buf
    }
}
