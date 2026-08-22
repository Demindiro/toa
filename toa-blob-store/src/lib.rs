use core::{error, fmt};
use std::io;

pub trait BlobStore {
    type BlobHandle;

    fn open_clear(&self, name: &str) -> io::Result<Self::BlobHandle>;
    fn find(&self, name: &str) -> io::Result<Option<Self::BlobHandle>>;
    fn transaction<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce() -> io::Result<R>;
    fn create(&self, name: &str) -> io::Result<Result<Self::BlobHandle, DuplicateBlob>>;
    fn create_unzoned(&self, name: &str) -> io::Result<Result<Self::BlobHandle, DuplicateBlob>>;
    fn rename(&self, old_name: &str, new_name: &str) -> io::Result<()>;
    fn append(&self, blob: &Self::BlobHandle, data: &[u8]) -> io::Result<u64>;
    fn append_many(&self, blob: &Self::BlobHandle, data: &[&[u8]]) -> io::Result<u64>;
    fn read_at(&self, blob: &Self::BlobHandle, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn name(&self, blob: &Self::BlobHandle) -> io::Result<String>;
    fn len(&self, blob: &Self::BlobHandle) -> io::Result<u64>;
    fn clear(&self, blob: &Self::BlobHandle) -> io::Result<()>;
    fn delete(&self, blob: Self::BlobHandle) -> io::Result<()>;
    fn flush(&self) -> io::Result<()>;
    fn size_on_disk(&self) -> io::Result<u64>;
    fn blobs<'a>(&'a self) -> io::Result<impl Iterator<Item = io::Result<Self::BlobHandle>> + 'a>;
}

pub trait BlobStoreExt: BlobStore {
    fn read_at_exact(
        &self,
        blob: &Self::BlobHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<()> {
        match self.read_at(blob, offset, buf) {
            Ok(n) if n == buf.len() => Ok(()),
            Ok(n) => todo!("want {}, got {n}", buf.len()),
            Err(e) => Err(e),
        }
    }
    fn read_at_exact_or_none(
        &self,
        blob: &Self::BlobHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<bool> {
        match self.read_at(blob, offset, buf) {
            Ok(n) if n == buf.len() => Ok(true),
            Ok(0) => Ok(false),
            Ok(n) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "read_at_exact_or_none: partial data (offset {}, wanted {}, got {})",
                    offset,
                    buf.len(),
                    n
                ),
            )),
            Err(e) => Err(e),
        }
    }
    fn read_at_array<const N: usize>(
        &self,
        blob: &Self::BlobHandle,
        offset: u64,
    ) -> io::Result<[u8; N]> {
        let mut buf = [0; N];
        self.read_at_exact(blob, offset, &mut buf)?;
        Ok(buf)
    }
}

#[derive(Debug)]
pub struct DuplicateBlob;

impl<T: BlobStore> BlobStoreExt for T {}

impl<T> BlobStore for &mut T
where
    T: BlobStore,
{
    type BlobHandle = T::BlobHandle;

    fn open_clear(&self, name: &str) -> io::Result<Self::BlobHandle> {
        (**self).open_clear(name)
    }
    fn find(&self, name: &str) -> io::Result<Option<Self::BlobHandle>> {
        (**self).find(name)
    }
    fn transaction<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce() -> io::Result<R>,
    {
        (**self).transaction(f)
    }
    fn create(&self, name: &str) -> io::Result<Result<Self::BlobHandle, DuplicateBlob>> {
        (**self).create(name)
    }
    fn create_unzoned(&self, name: &str) -> io::Result<Result<Self::BlobHandle, DuplicateBlob>> {
        (**self).create_unzoned(name)
    }
    fn rename(&self, old_name: &str, new_name: &str) -> io::Result<()> {
        (**self).rename(old_name, new_name)
    }
    fn append(&self, blob: &Self::BlobHandle, data: &[u8]) -> io::Result<u64> {
        (**self).append(blob, data)
    }
    fn append_many(&self, blob: &Self::BlobHandle, data: &[&[u8]]) -> io::Result<u64> {
        (**self).append_many(blob, data)
    }
    fn read_at(&self, blob: &Self::BlobHandle, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        (**self).read_at(blob, offset, buf)
    }
    fn name(&self, blob: &Self::BlobHandle) -> io::Result<String> {
        (**self).name(blob)
    }
    fn len(&self, blob: &Self::BlobHandle) -> io::Result<u64> {
        (**self).len(blob)
    }
    fn clear(&self, blob: &Self::BlobHandle) -> io::Result<()> {
        (**self).clear(blob)
    }
    fn delete(&self, blob: Self::BlobHandle) -> io::Result<()> {
        (**self).delete(blob)
    }
    fn flush(&self) -> io::Result<()> {
        (**self).flush()
    }
    fn size_on_disk(&self) -> io::Result<u64> {
        (**self).size_on_disk()
    }
    fn blobs<'a>(&'a self) -> io::Result<impl Iterator<Item = io::Result<Self::BlobHandle>> + 'a> {
        (**self).blobs()
    }
}

impl error::Error for DuplicateBlob {}

impl fmt::Display for DuplicateBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "duplicate blob".fmt(f)
    }
}
