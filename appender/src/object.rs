pub mod builder;
pub mod reader;

use crate::Hash;
use core::{mem, ops};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Byte(u8);
#[derive(Clone, Copy, Debug)]
struct ByteIndex(u8);

#[repr(C)]
struct Leaf2 {
    hash: [u8; 32],
    offset: u64,
    len: u64,
}

const _: () = assert!(mem::size_of::<Leaf2>() == 48);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct U256([usize; 32 / mem::size_of::<usize>()]);

impl Leaf2 {
    fn into_bytes(self) -> [u8; 48] {
        let mut buf = [0; 48];
        buf[..32].copy_from_slice(&self.hash);
        buf[32..40].copy_from_slice(&self.offset.to_le_bytes());
        buf[40..].copy_from_slice(&self.len.to_le_bytes());
        buf
    }

    fn from_bytes(data: &[u8; 48]) -> Self {
        let [data @ .., a, b, c, d, e, f, g, h] = *data;
        let len = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let [hash @ .., a, b, c, d, e, f, g, h] = data;
        let offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        Self { hash, offset, len }
    }
}

impl ByteIndex {
    fn get(&self, key: &Hash) -> Byte {
        Byte(key.0[usize::from(self.0)])
    }

    fn next(&self) -> Self {
        assert!(self.0 < 31, "next byte index out of range for 256-bit key");
        Self(self.0 + 1)
    }
}

impl U256 {
    const ZERO: Self = Self([0; 32 / mem::size_of::<usize>()]);

    fn test_bit(&self, index: u8) -> bool {
        let [i, s] = Self::split_bit_index(index.into());
        self.0[i] & (1 << s) != 0
    }

    fn set_bit(&mut self, index: u8) {
        let [i, s] = Self::split_bit_index(index.into());
        self.0[i] |= 1 << s;
    }

    fn with_bit(&self, index: u8) -> Self {
        let mut x = *self;
        x.set_bit(index);
        x
    }

    fn count_ones(&self) -> u16 {
        self.0.iter().map(|x| x.count_ones() as u16).sum()
    }

    /// Generate a mask with 0 to 255 bits set starting from the lowest bit.
    fn trailing_mask(bits: u8) -> Self {
        let mut rd = [0; 32 / mem::size_of::<usize>() * 2];
        let mut wr = [0; 32 / mem::size_of::<usize>()];
        let i = usize::from(bits) + (mem::size_of::<usize>() * 8 - 1);
        let [i, s] = Self::split_bit_index(i);
        rd[..wr.len()].fill(usize::MAX);
        let x = &mut rd[wr.len() - 1];
        *x = x.wrapping_shr(!s as u32);
        let len = wr.len();
        wr.copy_from_slice(&rd[len - i..][..len]);
        Self(wr)
    }

    fn split_bit_index(index: usize) -> [usize; 2] {
        let w = mem::size_of::<usize>() * 8;
        [index / w, index % w]
    }

    fn to_le_bytes(&self) -> [u8; 32] {
        let mut b = [0; 32];
        b.chunks_exact_mut(mem::size_of::<usize>())
            .zip(self.0)
            .for_each(|(x, y)| x.copy_from_slice(&y.to_le_bytes()));
        b
    }

    fn from_le_bytes(bytes: &[u8; 32]) -> Self {
        let mut s = Self::ZERO;
        bytes
            .chunks_exact(mem::size_of::<usize>())
            .zip(&mut s.0)
            .for_each(|(x, y)| *y = usize::from_le_bytes(x.try_into().unwrap()));
        s
    }
}

impl ops::BitAnd for U256 {
    type Output = Self;

    fn bitand(mut self, rhs: Self) -> Self {
        self.0.iter_mut().zip(rhs.0).for_each(|(x, y)| *x &= y);
        self
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn u256_trailing_mask() {
        match mem::size_of::<usize>() {
            8 => {
                for i in 0..mem::size_of::<usize>() as u8 * 8 {
                    let x = !usize::MAX.checked_shl(i.into()).unwrap_or(0);
                    assert_eq!(U256::trailing_mask(i).0, [x, 0, 0, 0]);
                }
                assert_eq!(U256::trailing_mask(64).0, [usize::MAX, 0, 0, 0]);
                assert_eq!(U256::trailing_mask(65).0, [usize::MAX, 1, 0, 0]);
                assert_eq!(U256::trailing_mask(66).0, [usize::MAX, 3, 0, 0]);
                assert_eq!(
                    U256::trailing_mask(64 * 2 + 2).0,
                    [usize::MAX, usize::MAX, 3, 0]
                );
                assert_eq!(
                    U256::trailing_mask(64 * 3 + 2).0,
                    [usize::MAX, usize::MAX, usize::MAX, 3]
                );
            }
            x => todo!("{x}"),
        }
    }
}
