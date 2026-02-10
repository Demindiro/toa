use crate::{InnerToa, Result};
use toa::{Hash, Object};

const MAGIC: [u8; 24] = *b"Appender UNIX directory\0";

pub struct Dir<'a> {
    object: Object<&'a InnerToa>,
    total: u64,
}

pub struct DirItem {
    pub ty: DirItemType,
    pub permissions: u16,
    pub name: String,
    pub uid: u32,
    pub gid: u32,
    pub modified: i64,
    pub key: Hash,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DirItemType {
    File,
    Dir,
    SymLink,
}

impl<'a> Dir<'a> {
    pub fn new(object: Object<&'a InnerToa>) -> Result<Self> {
        let mut hdr = [0; 32];
        object
            .data()
            .read_exact(0, &mut hdr)
            .map_err(|e| format!("failed to get directory header: {e:?}"))?;
        let [magic @ .., a, b, c, d, e, f, g, h] = hdr;
        if magic != MAGIC {
            return Err(format!("bad dir magic").into());
        }
        let total = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        Ok(Self { object, total })
    }

    pub fn get(&self, index: u64) -> Result<Option<DirItem>> {
        if index >= self.total {
            return Ok(None);
        }
        let mut x = [0; 64];
        self.object
            .data()
            .read_exact((32 + index * 64).into(), &mut x)
            .map_err(|e| format!("failed to get directory entry: {e:?}"))?;
        let [a, b, x @ ..] = x;
        let ty_perms = u16::from_le_bytes([a, b]);
        let ty = match ty_perms >> 9 {
            0 => DirItemType::File,
            1 => DirItemType::Dir,
            2 => DirItemType::SymLink,
            x => return Err(format!("invalid type {x} for directory entry"))?,
        };
        let [name_len, x @ ..] = x;
        let [_, _, _, _, _, x @ ..] = x;
        let [a, b, c, d, x @ ..] = x;
        let uid = u32::from_le_bytes([a, b, c, d]);
        let [a, b, c, d, x @ ..] = x;
        let gid = u32::from_le_bytes([a, b, c, d]);
        let [a, b, c, d, e, f, g, h, x @ ..] = x;
        let name_offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let [a, b, c, d, e, f, g, h, x @ ..] = x;
        let modified = i64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let key = Hash::from_bytes(x);
        let mut name = vec![0; name_len.into()];
        self.object
            .data()
            .read_exact(name_offset.into(), &mut name)
            .map_err(|e| format!("failed to get name of directory entry: {e:?}"))?;
        // TODO length check
        // also use a pretty-printer like BStr
        let name = String::from_utf8_lossy(&name).to_string();
        Ok(Some(DirItem {
            ty,
            permissions: ty_perms & 0o777,
            uid,
            gid,
            name,
            modified,
            key,
        }))
    }

    pub fn symlink_slice(&self, item: &DirItem) -> Box<[u8]> {
        let [a, b, c, d, e, f, g, h, x @ ..] = *item.key.as_bytes();
        let offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let [a, b, c, d, e, f, g, h, ..] = x;
        let len = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        // FIXME workaround for bug in toa-cli
        let offset = offset + item.name.len() as u64;
        let mut x = vec![0; len as usize];
        self.object
            .data()
            .read_exact(offset.into(), &mut x)
            .expect("invalid symlink key");
        x.into()
    }
}
