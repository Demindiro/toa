#![no_std]

pub use toa_hash::*;

use core::fmt;

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
