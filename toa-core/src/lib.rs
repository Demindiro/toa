#![no_std]

use core::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Hash(pub [u8; 32]);

#[derive(Clone, Copy, Debug)]
pub enum CompressionAlgorithm {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

#[derive(Clone, Debug)]
pub struct UnknownCompressionAlgorithm(pub u32);

#[derive(Clone, Debug)]
pub struct CorruptedCompression;

impl Hash {
    pub fn to_hex(&self) -> [u8; 64] {
        let mut b = [0; 64];
        for (w, x) in b.chunks_exact_mut(2).zip(self.0) {
            let f = |i| b"0123456789abcdef"[usize::from(i)];
            w[0] = f(x >> 4);
            w[1] = f(x & 15);
        }
        b
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|x| write!(f, "{x:02x}"))
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
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

impl TryFrom<u8> for CompressionAlgorithm {
    type Error = UnknownCompressionAlgorithm;

    fn try_from(x: u8) -> Result<Self, Self::Error> {
        u32::from(x).try_into()
    }
}

pub fn compress(data: &[u8], buf: &mut [u8]) -> (usize, CompressionAlgorithm) {
    assert!(
        buf.len() >= data.len(),
        "compression buffer should be at least as large as data"
    );
    let len = data.iter().rposition(|x| *x != 0).map_or(0, |i| i + 1);
    let (data, buf) = (&data[..len], &mut buf[..len]);
    match compress_zstd(data, buf) {
        Some(x) => (x, CompressionAlgorithm::Zstd),
        None => {
            buf.copy_from_slice(data);
            (buf.len(), CompressionAlgorithm::None)
        }
    }
}

pub fn decompress(
    data: &[u8],
    buf: &mut [u8],
    algorithm: CompressionAlgorithm,
) -> Result<(), CorruptedCompression> {
    let decompr_len = match algorithm {
        CompressionAlgorithm::None => buf
            .get_mut(..data.len())
            .map(|x| x.copy_from_slice(data))
            .map(|_| data.len())
            .ok_or(CorruptedCompression)?,
        CompressionAlgorithm::Lz4 => todo!("lz4"),
        CompressionAlgorithm::Zstd => decompress_zstd(data, buf)?,
    };
    // because zstd does funny things
    buf[decompr_len..].fill(0);
    Ok(())
}

fn compress_zstd<'a>(data: &[u8], buf: &'a mut [u8]) -> Option<usize> {
    // TODO make compression level configurable
    let len = zstd_safe::compress(buf, data, i32::MAX).unwrap_or(usize::MAX);
    (len < data.len()).then_some(len)
}

fn decompress_zstd(data: &[u8], buf: &mut [u8]) -> Result<usize, CorruptedCompression> {
    zstd_safe::decompress(buf, data).map_err(|_| CorruptedCompression)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn hash_to_hex() {
        assert_eq!(
            Hash([0; 32]).to_hex(),
            *b"0000000000000000000000000000000000000000000000000000000000000000"
        );
        assert_eq!(
            Hash([1; 32]).to_hex(),
            *b"0101010101010101010101010101010101010101010101010101010101010101"
        );
        assert_eq!(
            Hash([0xf7; 32]).to_hex(),
            *b"f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7"
        );
    }
}
