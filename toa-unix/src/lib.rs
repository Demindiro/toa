#![no_std]

use chrono::prelude::*;
use core::fmt;
use toa::{Hash, Object};

#[derive(Clone, Debug, Default)]
pub struct Dir<T>(T);

pub struct DirIter<T> {
    dir: Dir<T>,
    cur: u64,
}

#[derive(Debug)]
pub struct DirItem {
    pub ty: DirItemType,
    pub permissions: u16,
    pub name: DirData,
    pub uid: u32,
    pub gid: u32,
    pub modified: i64,
}

#[derive(Debug)]
pub enum DirItemType {
    File,
    Dir,
    SymLink(DirData),
    Unknown { ty: u16 },
}

#[derive(Clone, Copy, Debug)]
pub struct DirData {
    offset: u64,
    len: u64,
}

impl<T> Dir<T> {
    pub fn new(object: T) -> Self {
        Self(object)
    }
}

impl<T> Dir<T>
where
    T: Clone,
{
    pub fn iter(&self) -> DirIter<T> {
        DirIter {
            dir: self.clone(),
            cur: 0,
        }
    }
}

impl<T> Dir<Object<T>> {}

impl<T> Dir<Object<&toa::Toa<T>>>
where
    T: toa::ToaStore,
{
    pub fn len(&self) -> u64 {
        self.0.refs().len().try_into().unwrap_or(u64::MAX)
    }

    pub fn inside_bounds(&self, index: u64) -> bool {
        u128::from(index) < self.0.refs().len()
    }

    pub fn get(&self, index: u64) -> Result<Option<DirItem>, toa::ReadExactError<T::Error>> {
        let offset = u128::from(index) * 32;
        self.inside_bounds(index)
            .then(|| {
                let x = self.0.data().read_array::<32>(offset)?;

                let [a, b, name_len, d, e, f, g, h, x @ ..] = x;
                let ty_perms = u16::from_le_bytes([a, b]);
                let len = u64::from_le_bytes([d, e, f, g, h, 0, 0, 0]);
                let [a, b, c, d, e, f, g, h, x @ ..] = x;
                let uid = u32::from_le_bytes([a, b, c, d]);
                let gid = u32::from_le_bytes([e, f, g, h]);
                let [a, b, c, d, e, f, g, h, x @ ..] = x;
                let name_offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
                let modified = i64::from_le_bytes(x);

                let name = DirData {
                    offset: name_offset,
                    len: u64::from(name_len),
                };
                let ty = match ty_perms >> 9 {
                    0 => DirItemType::File,
                    1 => DirItemType::Dir,
                    2 => DirItemType::SymLink(DirData {
                        offset: name_offset + name.len,
                        len,
                    }),
                    ty => DirItemType::Unknown { ty },
                };

                Ok(DirItem {
                    ty,
                    permissions: ty_perms & 0o777,
                    uid,
                    gid,
                    name,
                    modified,
                })
            })
            .transpose()
    }

    pub fn get_ref(&self, index: u64) -> Result<Option<Hash>, toa::ReadExactError<T::Error>> {
        let offset = u128::from(index);
        self.inside_bounds(index)
            .then(|| self.0.refs().read_array(offset).map(|[x]| x))
            .transpose()
    }

    pub fn read_data(
        &self,
        data: DirData,
        out: &mut [u8],
    ) -> Result<(), toa::ReadExactError<T::Error>> {
        self.0.data().read_exact(data.offset.into(), out)
    }
}

impl<T> Iterator for DirIter<Object<&toa::Toa<T>>>
where
    T: toa::ToaStore,
{
    type Item = Result<(u64, DirItem), toa::ReadExactError<T::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.dir
            .get(self.cur)
            .map(|x| x.map(|x| (self.cur, x)).inspect(|_| self.cur += 1))
            .transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        usize::try_from(self.dir.len() - self.cur).map_or((usize::MAX, None), |x| (x, Some(x)))
    }
}

impl DirData {
    pub fn len(&self) -> u64 {
        self.len
    }
}
