use chacha20poly1305::Tag;

#[derive(Clone, Copy, Debug)]
pub enum CompressionAlgorithm {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

#[derive(Clone, Debug)]
pub struct UnknownCompressionAlgorithm(pub u32);

#[derive(Clone, Copy, Debug)]
pub(crate) struct Entry {
    pub tag: Tag,
    pub offset: u64,
    pub compression_algorithm: CompressionAlgorithm,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
}

impl Entry {
    pub const LEN: usize = 32;

    pub fn into_bytes(self) -> [u8; Self::LEN] {
        assert!(self.compressed_len <= 1 << 18);
        assert!(self.uncompressed_len <= 1 << 18);
        let mut buf = [0; Self::LEN];
        buf[0..16].copy_from_slice(self.tag.as_slice());
        buf[16..24].copy_from_slice(&self.offset.to_le_bytes());
        let x = self.compressed_len << 14 | self.compression_algorithm as u32;
        let y = self.uncompressed_len << 14;
        buf[24..28].copy_from_slice(&x.to_le_bytes());
        buf[28..32].copy_from_slice(&y.to_le_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8; Self::LEN]) -> Result<Self, UnknownCompressionAlgorithm> {
        let [data @ .., a, b, c, d, e, f, g, h] = *data;
        let [x, y] = [[a, b, c, d], [e, f, g, h]].map(u32::from_le_bytes);
        let [tag @ .., a, b, c, d, e, f, g, h] = data;
        let offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let compression_algorithm = (x & 0x3fff).try_into()?;
        let compressed_len = x >> 14;
        let uncompressed_len = y >> 14;
        Ok(Self {
            tag: *Tag::from_slice(&tag),
            offset,
            compression_algorithm,
            compressed_len,
            uncompressed_len,
        })
    }
}

impl TryFrom<u32> for CompressionAlgorithm {
    type Error = UnknownCompressionAlgorithm;

    fn try_from(x: u32) -> Result<Self, Self::Error> {
        Ok(match x {
            0 => Self::None,
            1 => Self::Lz4,
            2 => Self::Zstd,
            x => return Err(UnknownCompressionAlgorithm(x)),
        })
    }
}
