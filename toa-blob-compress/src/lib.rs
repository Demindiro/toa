pub use toa_blob::DuplicateBlob;

use nora_endian::u32le;
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
    page_size: PageSize,
    compression: Compression,
    compression_level: u8,
    table: toa_blob::BlobId,
    pages: toa_blob::BlobId,
    tail: toa_blob::BlobId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PageSize {
    K4 = 1 << 12,
    K8 = 1 << 13,
    K16 = 1 << 14,
    K32 = 1 << 15,
    K64 = 1 << 16,
    K128 = 1 << 17,
    K256 = 1 << 18,
    K512 = 1 << 19,
    M1 = 1 << 20,
    M2 = 1 << 21,
    M4 = 1 << 22,
    M8 = 1 << 23,
    M16 = 1 << 24,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Compression {
    None = 0,
    #[cfg(feature = "lz4")]
    Lz4 = 1,
    #[cfg(feature = "zstd")]
    Zstd = 2,
}

#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
struct TableHeader {
    magic: [u8; 8],
    version: u32le,
    page_size: u32le,
    compression: u8,
    compression_level: u8,
    _pad_0: [u8; 14],
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
        page_size: PageSize,
        compression: Compression,
        compression_level: u8,
    ) -> io::Result<Result<BlobRef<&'a Self>, DuplicateBlob>> {
        // TODO transactions (rollbacks!)
        let table = concat(name, TABLE_SUFFIX);
        let pages = concat(name, PAGES_SUFFIX);
        let tail = concat(name, TAIL_SUFFIX);
        match (
            self.store.create_blob(&table)?,
            self.store.create_blob(&pages)?,
            self.store.create_unzoned_blob(&tail)?,
        ) {
            (Ok(table), Ok(pages), Ok(tail)) => {
                let hdr = TableHeader {
                    magic: TableHeader::MAGIC,
                    version: TableHeader::VERSION.into(),
                    page_size: (page_size as u32).into(),
                    compression: compression as u8,
                    compression_level,
                    _pad_0: Default::default(),
                };
                table.append(bytemuck::bytes_of(&hdr))?;
                let [table, pages, tail] = [table, pages, tail].map(|x| x.id());
                self.blob(BlobSet {
                    page_size,
                    compression,
                    compression_level,
                    table,
                    pages,
                    tail,
                })
                .map(Ok)
            }
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
        let f = |x| self.store.find(x);
        match (f(&table)?, f(&pages)?, f(&tail)?) {
            (Some(table), Some(pages), Some(tail)) => {
                if table.len()? < 32 {
                    todo!("table too short");
                }
                let hdr = &mut [0; 32];
                table.read_at(0, hdr)?;
                let hdr = bytemuck::cast_ref::<_, TableHeader>(hdr);
                if hdr.magic != TableHeader::MAGIC {
                    todo!("bad table magic");
                }
                if hdr.version != TableHeader::VERSION {
                    todo!("bad table version");
                }
                let page_size = PageSize::try_from(u32::from(hdr.page_size)).unwrap();
                let compression = Compression::try_from(hdr.compression).unwrap();
                let compression_level = hdr.compression_level.into();
                let [table, pages, tail] = [table, pages, tail].map(|x| x.id());
                self.blob(BlobSet {
                    page_size,
                    compression,
                    compression_level,
                    table,
                    pages,
                    tail,
                })
                .map(Some)
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
        let clen = self.compressed_len()?;
        if let Some(x) = offset.checked_sub(clen) {
            // read from tail only
            return self.tail()?.read_at(x, buf);
        }
        // split into chunks and start reading
        todo!();
    }

    pub fn append(&self, data: &[u8]) -> io::Result<u64> {
        self.tail()?.append(data)
    }

    pub fn delete(self) -> io::Result<()> {
        self.table()?.delete()?;
        self.pages()?.delete()?;
        self.tail()?.delete()?;
        Ok(())
    }

    pub fn rename(&mut self, new_name: &[u8]) -> io::Result<()> {
        // FIXME we need atomic renames
        self.table()?.rename(&concat(new_name, TABLE_SUFFIX))?;
        self.pages()?.rename(&concat(new_name, PAGES_SUFFIX))?;
        self.tail()?.rename(&concat(new_name, TAIL_SUFFIX))?;
        Ok(())
    }

    fn read_compressed(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        todo!();
    }

    /// # Returns
    ///
    /// The total amount of compressed data in bytes.
    fn compressed_len(&self) -> io::Result<u64> {
        let n = self.table()?.len()?;
        let n = n - core::mem::size_of::<TableHeader>() as u64;
        let n = n / 16;
        let n = n * (self.blobs.page_size as u64);
        Ok(n)
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

impl TryFrom<u32> for PageSize {
    type Error = &'static str;

    fn try_from(n: u32) -> Result<Self, Self::Error> {
        use PageSize::*;
        Ok(match n {
            0x1000 => K4,
            0x2000 => K8,
            0x4000 => K16,
            0x8000 => K32,
            0x10000 => K64,
            0x20000 => K128,
            0x40000 => K256,
            0x80000 => K512,
            0x100000 => M1,
            0x200000 => M2,
            0x400000 => M4,
            0x800000 => M8,
            0x1000000 => M16,
            _ => return Err("unsupported page size"),
        })
    }
}

impl TryFrom<u8> for Compression {
    type Error = &'static str;

    fn try_from(n: u8) -> Result<Self, Self::Error> {
        use Compression::*;
        Ok(match n {
            0 => None,
            #[cfg(feature = "lz4")]
            1 => Lz4,
            #[cfg(feature = "zstd")]
            2 => Zstd,
            _ => return Err("unsupported compression algorithm"),
        })
    }
}

impl TableHeader {
    pub const MAGIC: [u8; 8] = *b"Compress";
    pub const VERSION: u32 = 0x20260317;
}

fn concat(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().chain(b).copied().collect::<Vec<u8>>()
}
