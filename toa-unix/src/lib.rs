use std::{fs, io};
use toa::{Blob, Data, Hash, Object, Refs, Toa};

pub struct Dir<'a, T> {
    refs: Refs<'a, T>,
    data: Data<'a, T>,
    len: u64,
}

pub struct DirIter<'a, T> {
    dir: Dir<'a, T>,
    cur: u64,
}

#[derive(Debug)]
pub struct DirItem {
    pub ty: DirItemType,
    pub len: u64,
    pub permissions: u16,
    pub name: DirData,
    pub uid: u32,
    pub gid: u32,
    pub modified: i64,
}

#[derive(Clone, Copy, Debug)]
pub enum DirItemType {
    File,
    Dir,
    SymLink,
    Unknown { ty: u16 },
}

#[derive(Clone, Copy, Debug)]
pub struct DirData {
    offset: u64,
    len: u64,
}

impl<'a> Dir<'a, Blob<fs::File>> {
    pub fn new(toa: &'a Toa<Blob<fs::File>>, refs: &Hash) -> Result<Self, io::Error> {
        let Some(refs) = toa.get(refs)? else { todo!() };
        let Object::Refs(refs) = refs else { todo!() };
        let [data] = refs.read_array(0).unwrap_or_else(|e| todo!("{e:?}"));
        let Some(data) = toa.get(&data)? else { todo!() };
        let Object::Data(data) = data else { todo!() };
        let len = (refs.len()? - 1).try_into().unwrap_or(u64::MAX);
        Ok(Self { refs, data, len })
    }
}

impl<T> Clone for Dir<'_, T> {
    fn clone(&self) -> Self {
        Self {
            refs: self.refs,
            data: self.data,
            len: self.len,
        }
    }
}

impl<'a, T> Dir<'a, T> {
    pub fn iter(&self) -> DirIter<'a, T> {
        DirIter {
            dir: self.clone(),
            cur: 0,
        }
    }
}

impl<'a> Dir<'a, Blob<fs::File>> {
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn inside_bounds(&self, index: u64) -> bool {
        index < self.len
    }

    pub fn get(&self, index: u64) -> Result<Option<DirItem>, toa::ReadExactError<io::Error>> {
        let offset = u128::from(index) * 32;
        self.inside_bounds(index)
            .then(|| {
                let x = self.data.read_array::<32>(offset)?;

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
                    2 => DirItemType::SymLink,
                    ty => DirItemType::Unknown { ty },
                };

                Ok(DirItem {
                    ty,
                    len,
                    permissions: ty_perms & 0o777,
                    uid,
                    gid,
                    name,
                    modified,
                })
            })
            .transpose()
    }

    pub fn get_ref(&self, index: u64) -> Result<Option<Hash>, toa::ReadExactError<io::Error>> {
        let offset = u128::from(index);
        self.inside_bounds(index)
            .then(|| self.refs.read_array(1 + offset).map(|[x]| x))
            .transpose()
    }

    pub fn read_data(
        &self,
        data: DirData,
        out: &mut [u8],
    ) -> Result<(), toa::ReadExactError<io::Error>> {
        self.data.read_exact(data.offset.into(), out)
    }
}

impl Iterator for DirIter<'_, Blob<fs::File>> {
    type Item = Result<(u64, DirItem), toa::ReadExactError<io::Error>>;

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
