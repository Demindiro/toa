use super::{ByteIndex, Leaf2, U256};
use crate::{Hash, ObjectRaw, PackOffset};
use core::mem;

const _: () = assert!(mem::size_of::<Leaf2>() == 48);

pub(crate) fn find<E, F>(root: PackOffset, key: &Hash, mut dev: F) -> Result<Option<ObjectRaw>, E>
where
    F: FnMut(PackOffset, &mut [u8]) -> Result<(), E>,
{
    let mut offset = root;
    let mut index = ByteIndex(0);
    loop {
        let (offt, ty) = (offset.0 & !7, offset.0 & 7);
        let mut f = |o, buf: &mut _| (dev)(PackOffset(offt + o), buf);
        if ty & 1 == 0 {
            let byte = index.get(key);
            let buf = &mut [0; 32];
            f(0, buf)?;
            let population = U256::from_le_bytes(buf);
            let i = (population & U256::trailing_mask(byte.0)).count_ones();
            let buf = &mut [0; 8];
            f(32 + 8 * u64::from(i), buf)?;
            offset = PackOffset(u64::from_le_bytes(*buf));
        } else {
            let buf = &mut [0; 48];
            f(0, buf)?;
            let Leaf2 { hash, offset, len } = Leaf2::from_bytes(buf);
            return Ok((hash == *key.as_bytes()).then_some(ObjectRaw {
                offset: PackOffset(offset),
                len,
            }));
        }
        index = index.next();
    }
}

pub(crate) fn iter_with<'a, E, F, G>(root: PackOffset, mut dev: F, mut with: G) -> Result<(), E>
where
    F: FnMut(PackOffset, &mut [u8]) -> Result<(), E>,
    G: FnMut(Hash) -> bool,
{
    iter_with_do(root, &mut dev, &mut with).map(|_| ())
}

fn iter_with_do<E, F, G>(offset: PackOffset, dev: &mut F, with: &mut G) -> Result<bool, E>
where
    F: FnMut(PackOffset, &mut [u8]) -> Result<(), E>,
    G: FnMut(Hash) -> bool,
{
    let (offt, ty) = (offset.0 & !7, offset.0 & 7);
    match ty & 1 == 1 {
        false => iter_with_parent(offt, dev, with),
        true => iter_with_leaf(offt, dev, with),
    }
}

fn iter_with_parent<E, F, G>(offset: u64, dev: &mut F, with: &mut G) -> Result<bool, E>
where
    F: FnMut(PackOffset, &mut [u8]) -> Result<(), E>,
    G: FnMut(Hash) -> bool,
{
    let mut f = |o, buf: &mut _| (dev)(PackOffset(offset + o), buf);
    let buf = &mut [0; 32];
    f(0, buf)?;
    let population = U256::from_le_bytes(buf);
    let mut buf = alloc::vec![[0; 8]; usize::from(population.count_ones())];
    f(32, buf.as_flattened_mut())?;
    for x in buf {
        let offt = u64::from_le_bytes(x);
        if iter_with_do(PackOffset(offt), dev, with)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn iter_with_leaf<E, F, G>(offset: u64, dev: &mut F, with: &mut G) -> Result<bool, E>
where
    F: FnMut(PackOffset, &mut [u8]) -> Result<(), E>,
    G: FnMut(Hash) -> bool,
{
    let buf = &mut [0; 48];
    (dev)(PackOffset(offset), buf)?;
    let Leaf2 { hash, .. } = Leaf2::from_bytes(buf);
    Ok((with)(Hash::from_bytes(hash)))
}
