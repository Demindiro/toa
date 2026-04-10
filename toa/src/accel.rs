use std::io;
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
}

pub trait IndexMut {
    /// Add more entries to the index
    fn add(
        &self,
        items: &mut dyn ExactSizeIterator<Item = (Hash, IndexEntry)>,
        new_top: IndexCookie,
    ) -> io::Result<()>;
}

#[derive(Clone, Copy)]
pub struct IndexCookie {
    pub offset_full: u64,
    pub offset_partial: u64,
}

#[derive(Clone, Copy, Default)]
pub struct IndexEntry(pub u64);

impl Index for () {
    fn top_cookie(&self) -> IndexCookie {
        IndexCookie {
            offset_full: 0,
            offset_partial: 0,
        }
    }

    fn get(&self, _key: &Hash) -> io::Result<Option<IndexEntry>> {
        Ok(None)
    }
}
