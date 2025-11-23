#[repr(C)]
struct Entry {
    offset: u64,
    compression_info: CompressionInfo,
    uncompressed_length: u32,
    poly1305: u128,
}

#[repr(transparent)]
struct CompressionInfo(u32);

enum CompressionAlgorithm {
    N1,
    N2,
    N3,
}

impl CompressionInfo {
    fn algorithm(&self) -> Option<CompressionAlgorithm> {
        match self.0 & 3 {
            0 => None,
            1 => Some(CompressionAlgorithm::N1),
            2 => Some(CompressionAlgorithm::N2),
            3 => Some(CompressionAlgorithm::N3),
            _ => unreachable!(),
        }
    }

    fn len(&self) -> u32 {
        self.0 >> 2
    }
}
