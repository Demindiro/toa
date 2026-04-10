use std::{
    collections::BTreeMap,
    io::{self, Read, Seek, Write},
};
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

/// A very dumb on-disk cache.
///
/// Purely intended to be loaded fast into memory.
pub struct HashCache<T> {
    file: T,
    cookie: IndexCookie,
    map: BTreeMap<Hash, IndexEntry>,
}

#[derive(Clone, Copy, Default, bytemuck::Zeroable, bytemuck::Pod)]
#[repr(C)]
struct HashCacheEntry {
    key: Hash,
    value: IndexEntry,
}

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

impl<T> HashCache<T>
where
    T: Read + Write + Seek,
{
    pub fn load(file: T) -> io::Result<Self> {
        let mut s = Self {
            file,
            cookie: Default::default(),
            map: Default::default(),
        };
        s.file.rewind()?;
        match s.file.read_exact(bytemuck::bytes_of_mut(&mut s.cookie)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => s
                .file
                .write_all(bytemuck::bytes_of(&IndexCookie::default()))?,
            Err(e) => return Err(e),
        }
        let mut reader = io::BufReader::new(&mut s.file);
        let mut entry = HashCacheEntry::default();
        while reader
            .read_exact(bytemuck::bytes_of_mut(&mut entry))
            .is_ok()
        {
            s.map.insert(entry.key, entry.value);
        }
        Ok(s)
    }
}

impl<T> Index for HashCache<T>
where
    T: Read + Write + Seek,
{
    fn top_cookie(&self) -> IndexCookie {
        self.cookie
    }

    fn get(&self, key: &Hash) -> io::Result<Option<IndexEntry>> {
        Ok(self.map.get(key).copied())
    }

    fn add(&mut self, &key: &Hash, value: IndexEntry) -> io::Result<()> {
        let pos = self.file.seek(io::SeekFrom::End(0))?;
        assert!(
            pos >= core::mem::size_of::<IndexCookie>() as u64,
            "file missing header"
        );
        self.map.insert(key, value);
        let entry = HashCacheEntry { key, value };
        self.file.write_all(bytemuck::bytes_of(&entry))?;
        Ok(())
    }

    fn set_top(&mut self, new_top: IndexCookie) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(0))?;
        self.file.write_all(bytemuck::bytes_of(&new_top))?;
        self.file.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::*;

    fn init<A>(accel: A) -> crate::test::Test<A>
    where
        A: Index,
    {
        let store = toa_blob::BlobStore::init(toa_blob::MemZones::new(1 << 20, 20)).unwrap();
        let toa = Toa::init(store, accel, PageSize::K4, Compression::None, 0)
            .expect("toa init failed")
            .expect("duplicate toa store");
        crate::test::Test { toa }
    }

    #[test]
    fn hash_cache() {
        let accel = std::io::Cursor::new(vec![]);
        let accel = HashCache::load(accel).unwrap();
        let mut s = init(accel);
        let f = |x| format!("A number {x}").into_bytes();
        let keys = (0..1 << 12).map(|i| s.add(&f(i))).collect::<Vec<_>>();
        keys.iter()
            .enumerate()
            .for_each(|(i, k)| s.assert_eq(k, &f(i)));
        let (store, accel, res) = s.toa.unmount();
        res.unwrap();
        let toa = Toa::load(store, accel).unwrap().unwrap();
        s = crate::test::Test { toa };
        keys.iter()
            .enumerate()
            .for_each(|(i, k)| s.assert_eq(k, &f(i)));
    }
}
