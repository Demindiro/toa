use std::io;

pub trait BlobStore {
    type BlobHandle;

    fn open(&self, name: &str) -> io::Result<Self::BlobHandle>;
    fn open_clear(&self, name: &str) -> io::Result<Self::BlobHandle>;
    fn rename(&self, old_name: &str, new_name: &str) -> io::Result<()>;
    fn append(&self, blob: &Self::BlobHandle, data: &[u8]) -> io::Result<u64>;
    fn append_many(&self, blob: &Self::BlobHandle, data: &[&[u8]]) -> io::Result<u64>;
    fn read_at(&self, blob: &Self::BlobHandle, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn flush(&self) -> io::Result<()>;
    fn size_on_disk(&self) -> io::Result<u64>;
}

pub trait BlobStoreExt: BlobStore {
    fn read_at_exact(
        &self,
        blob: &Self::BlobHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<bool> {
        match self.read_at(blob, offset, buf) {
            Ok(n) if n == buf.len() => Ok(true),
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
            Ok(_) => todo!(),
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

    fn open(&self, name: &str) -> io::Result<Self::BlobHandle> {
        (**self).open(name)
    }
    fn open_clear(&self, name: &str) -> io::Result<Self::BlobHandle> {
        (**self).open_clear(name)
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
    fn flush(&self) -> io::Result<()> {
        (**self).flush()
    }
    fn size_on_disk(&self) -> io::Result<u64> {
        (**self).size_on_disk()
    }
}
