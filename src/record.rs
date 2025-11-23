#[repr(C)]
pub struct Entry {
    pub offset: u64,
    pub compression_info: CompressionInfo,
    pub uncompressed_len: u32,
    pub poly1305: u128,
}

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
