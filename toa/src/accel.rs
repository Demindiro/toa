#[cfg(feature = "accel-sled")]
pub use sled;

use std::{collections::BTreeMap, io};
use toa_hash::Hash;

/// erated index.
///
/// An index allows skipping scanning the entire blob store,
/// speeding up the loading process dramatically.
pub trait Index {
    /// Cookie pointing to the last chunks added to this index.
    fn top_cookie(&self) -> IndexCookie;

    /// Lookup an entry in the index.
    fn get(&self, key: &Hash) -> io::Result<Option<IndexEntry>>;

    /// Add more entries to the index
    fn add(&mut self, key: &Hash, value: IndexEntry) -> io::Result<()>;

    fn set_top(&mut self, new_top: IndexCookie) -> io::Result<()>;
}

#[derive(Clone, Copy, Default, bytemuck::Zeroable, bytemuck::Pod)]
#[repr(C)]
pub struct IndexCookie {
    pub data_offset_full: u64,
    pub data_offset_partial: u64,
    pub data_offset_pairs: u64,
    pub refs_offset_full: u64,
    pub refs_offset_partial: u64,
    pub refs_offset_pairs: u64,
}

#[derive(Clone, Copy, Default, bytemuck::Zeroable, bytemuck::Pod)]
#[repr(transparent)]
pub struct IndexEntry(pub u64);

impl Index for BTreeMap<Hash, IndexEntry> {
    fn top_cookie(&self) -> IndexCookie {
        IndexCookie::default()
    }

    fn get(&self, key: &Hash) -> io::Result<Option<IndexEntry>> {
        Ok(self.get(key).copied())
    }

    fn add(&mut self, &key: &Hash, value: IndexEntry) -> io::Result<()> {
        self.insert(key, value);
        Ok(())
    }

    fn set_top(&mut self, _new_top: IndexCookie) -> io::Result<()> {
        Ok(())
    }
}

impl<T> Index for &mut T
where
    T: Index,
{
    fn top_cookie(&self) -> IndexCookie {
        (**self).top_cookie()
    }
    fn get(&self, key: &Hash) -> io::Result<Option<IndexEntry>> {
        (**self).get(key)
    }
    fn add(&mut self, key: &Hash, value: IndexEntry) -> io::Result<()> {
        (**self).add(key, value)
    }
    fn set_top(&mut self, new_top: IndexCookie) -> io::Result<()> {
        (**self).set_top(new_top)
    }
}

#[cfg(feature = "accel-sled")]
mod imp_sled {
    use super::*;

    const KEY_TOP_COOKIE: &[u8] = b"top-cookie";

    impl Index for sled::Db {
        fn top_cookie(&self) -> IndexCookie {
            sled::Tree::get(self, KEY_TOP_COOKIE)
                .unwrap()
                .map_or_else(Default::default, |x| bytemuck::pod_read_unaligned(&x))
        }

        fn get(&self, key: &Hash) -> io::Result<Option<IndexEntry>> {
            Ok(sled::Tree::get(self, bytemuck::bytes_of(key))
                .unwrap()
                .map(|x| bytemuck::pod_read_unaligned(&x)))
        }

        fn add(&mut self, key: &Hash, value: IndexEntry) -> io::Result<()> {
            self.insert(bytemuck::bytes_of(key), &value.0.to_le_bytes())
                .unwrap();
            Ok(())
        }

        fn set_top(&mut self, new_top: IndexCookie) -> io::Result<()> {
            self.insert(KEY_TOP_COOKIE, bytemuck::bytes_of(&new_top))
                .unwrap();
            Ok(())
        }
    }
}
