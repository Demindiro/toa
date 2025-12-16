use chacha20poly1305::Tag;

#[derive(Clone, Copy, Debug)]
pub struct Entry {
    pub tag: Tag,
    pub offset: u64,
    pub compression_algorithm: u8,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
}

impl Entry {
    pub const LEN: usize = 32;

    pub fn into_bytes(self) -> [u8; Self::LEN] {
        assert!(self.compressed_len < 1 << 24);
        assert!(self.uncompressed_len < 1 << 24);
        let mut buf = [0; Self::LEN];
        buf[0..16].copy_from_slice(self.tag.as_slice());
        buf[16..24].copy_from_slice(&self.offset.to_le_bytes());
        buf[24] = self.compression_algorithm;
        buf[25..28].copy_from_slice(&self.compressed_len.to_le_bytes()[..3]);
        buf[28] = 0;
        buf[29..32].copy_from_slice(&self.uncompressed_len.to_le_bytes()[..3]);
        buf
    }

    pub fn from_bytes(b: &[u8; Self::LEN]) -> Self {
        Self {
            tag: *Tag::from_slice(&b[0..16]),
            offset: u64::from_le_bytes(b[16..24].try_into().unwrap()),
            compression_algorithm: b[24],
            compressed_len: u32::from_le_bytes(b[24..28].try_into().unwrap()) >> 8,
            uncompressed_len: u32::from_le_bytes(b[28..32].try_into().unwrap()) >> 8,
        }
    }
}
