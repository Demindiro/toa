use crate::{InnerReader, Result};
use appender::{Hash, Object};

const MAGIC: [u8; 24] = *b"Appender UNIX directory\0";

pub struct Dir<'a> {
    object: Object<'a, InnerReader>,
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
    pub fn new(object: Object<'a, InnerReader>) -> Result<Self> {
        let hdr = object
            .read_exact(0, 32)
            .and_then(|x| x.into_bytes())
            .map_err(|e| format!("failed to get directory header: {e:?}"))?;
        let hdr: [u8; 32] = hdr
            .try_into()
            .map_err(|_| format!("truncated (or invalid) directory header"))?;
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
        let x = self
            .object
            .read_exact(32 + index * 64, 64)
            .and_then(|x| x.into_bytes())
            .map_err(|e| format!("failed to get directory entry: {e:?}"))?;
        let x: [u8; 64] = x.try_into().map_err(|_| "directory entry is truncated")?;
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
        let name = self
            .object
            .read_exact(name_offset, name_len.into())
            .and_then(|x| x.into_bytes())
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

    pub fn symlink_slice(&self, item: &DirItem) -> appender::ObjectRaw {
        let [a, b, c, d, e, f, g, h, x @ ..] = *item.key.as_bytes();
        let offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let [a, b, c, d, e, f, g, h, ..] = x;
        let len = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        // FIXME workaround for bug in appender-cli
        let offset = offset + item.name.len() as u64;
        self.object
            .to_raw()
            .subslice(offset, len)
            .expect("invalid symlink key")
    }
}
