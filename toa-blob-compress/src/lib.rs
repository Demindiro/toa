pub use toa_blob::DuplicateBlob;

use std::io;

const TABLE_SUFFIX: &[u8] = b".table.compr";
const PAGES_SUFFIX: &[u8] = b".pages.compr";
const TAIL_SUFFIX: &[u8] = b".tail.compr";

pub struct BlobStoreCompress<T> {
    store: T,
}

pub struct BlobRef<T> {
    store: T,
    blobs: BlobSet,
}

#[derive(Clone, Copy, Debug)]
pub struct BlobSet {
    table: toa_blob::BlobId,
    pages: toa_blob::BlobId,
    tail: toa_blob::BlobId,
}

impl<T> BlobStoreCompress<T> {
    pub fn new(store: T) -> Self {
        Self { store }
    }

    pub fn inner(&self) -> &T {
        &self.store
    }

    pub fn into_inner(self) -> T {
        self.store
    }
}

impl<U> BlobStoreCompress<toa_blob::BlobStore<U>>
where
    U: toa_blob::ZoneDev,
{
    pub fn create_blob<'a>(
        &'a self,
        name: &[u8],
    ) -> io::Result<Result<BlobRef<&'a Self>, DuplicateBlob>> {
        let table = concat(name, TABLE_SUFFIX);
        let pages = concat(name, PAGES_SUFFIX);
        let tail = concat(name, TAIL_SUFFIX);
        let f = |x| self.store.create_blob(x).map(|x| x.map(|x| x.id()));
        match (f(&table)?, f(&pages)?, f(&tail)?) {
            (Ok(table), Ok(pages), Ok(tail)) => self.blob(BlobSet { table, pages, tail }).map(Ok),
            (Err(e), Err(_), Err(_)) => Ok(Err(e)),
            _ => todo!("blob missing"),
        }
    }

    pub fn blob<'a>(&'a self, blobs: BlobSet) -> io::Result<BlobRef<&'a Self>> {
        Ok(BlobRef { store: self, blobs })
    }

    pub fn find<'a>(&'a self, name: &[u8]) -> io::Result<Option<BlobRef<&'a Self>>> {
        let table = concat(name, TABLE_SUFFIX);
        let pages = concat(name, PAGES_SUFFIX);
        let tail = concat(name, TAIL_SUFFIX);
        let f = |x| self.store.find(x).map(|x| x.map(|x| x.id()));
        match (f(&table)?, f(&pages)?, f(&tail)?) {
            (Some(table), Some(pages), Some(tail)) => {
                self.blob(BlobSet { table, pages, tail }).map(Some)
            }
            (None, None, None) => Ok(None),
            _ => todo!("blob missing"),
        }
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.store.flush()
    }

    pub fn size_on_disk(&self) -> io::Result<u64> {
        self.store.size_on_disk()
    }
}

impl<T> BlobRef<T> {
    pub fn blob_set(&self) -> BlobSet {
        self.blobs
    }
}

impl<'a, U> BlobRef<&'a BlobStoreCompress<toa_blob::BlobStore<U>>>
where
    U: toa_blob::ZoneDev,
{
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // split into chunks and start reading
        todo!();
    }

    pub fn append(&self, data: &[u8]) -> io::Result<u64> {
        todo!();
    }

    pub fn delete(self) -> io::Result<()> {
        self.table()?.delete()?;
        self.pages()?.delete()?;
        self.tail()?.delete()?;
        Ok(())
    }

    pub fn rename(&mut self, new_name: &[u8]) -> io::Result<()> {
        todo!();
    }

    fn table(&self) -> io::Result<toa_blob::BlobRef<'_, toa_blob::BlobStore<U>>> {
        self.store.store.blob(self.blobs.table)
    }

    fn pages(&self) -> io::Result<toa_blob::BlobRef<'_, toa_blob::BlobStore<U>>> {
        self.store.store.blob(self.blobs.pages)
    }

    fn tail(&self) -> io::Result<toa_blob::BlobRef<'_, toa_blob::BlobStore<U>>> {
        self.store.store.blob(self.blobs.tail)
    }
}

fn concat(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().chain(b).copied().collect::<Vec<u8>>()
}
