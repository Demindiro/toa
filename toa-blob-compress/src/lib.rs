#![forbid(unused_must_use)]

pub use toa_blob_store::DuplicateBlob;

use core::{cell::Cell, fmt};
use nora_endian::{u32le, u64le};
use std::io;
use toa_blob_store::BlobStore;

const DESCRIPTOR_SUFFIX: &str = ".descr.compr";
const TABLE_SUFFIX: &str = ".table.compr";
const PAGES_SUFFIX: &str = ".pages.compr";
const TAIL_SUFFIX: &str = ".tail.compr";

pub struct BlobRef<'a, T>
where
    T: BlobStore,
{
    store: &'a T,
    blobs: BlobSet<T::BlobHandle>,
    cache: Cell<Cache>,
}

#[derive(Clone, Copy, Debug)]
pub struct BlobSet<T> {
    page_size: PageSize,
    compression: Compression,
    compression_level: u8,
    descriptor: T,
    table: T,
    pages: T,
    tail: T,
}

pub struct Cache {
    buf: Vec<u8>,
    offset: u64,
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
struct Descriptor {
    magic: [u8; 8],
    version: u32le,
    page_size: u32le,
    compression: u8,
    compression_level: u8,
    _pad_0: [u8; 14],
}

#[derive(Clone, Copy, Debug, Default, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
struct TableEntry {
    offset: u64le,
    algorithm: u8,
    _pad_0: [u8; 3],
    compressed_len: u32le,
}

impl<'a, T> BlobRef<'a, T>
where
    T: BlobStore,
{
    pub fn blob(store: &'a T, blobs: BlobSet<T::BlobHandle>) -> Self {
        Self::blob_with_cache(store, blobs, Default::default())
    }

    pub fn blob_with_cache(store: &'a T, blobs: BlobSet<T::BlobHandle>, cache: Cache) -> Self {
        Self {
            store,
            blobs,
            cache: cache.into(),
        }
    }

    pub fn blob_set(&self) -> &BlobSet<T::BlobHandle> {
        &self.blobs
    }

    pub fn into_blob_set(self) -> (BlobSet<T::BlobHandle>, Cache) {
        (self.blobs, self.cache.take())
    }

    pub fn create(
        store: &'a T,
        name: &str,
        page_size: PageSize,
        compression: Compression,
        compression_level: u8,
    ) -> io::Result<Result<Self, DuplicateBlob>> {
        // TODO transactions (rollbacks!)
        let descriptor = concat(name, DESCRIPTOR_SUFFIX);
        let table = concat(name, TABLE_SUFFIX);
        let pages = concat(name, PAGES_SUFFIX);
        let tail = concat(name, TAIL_SUFFIX);
        match (
            store.create_unzoned(&descriptor)?,
            store.create(&table)?,
            store.create(&pages)?,
            store.create_unzoned(&tail)?,
        ) {
            (Ok(descriptor), Ok(table), Ok(pages), Ok(tail)) => {
                let hdr = Descriptor {
                    magic: Descriptor::MAGIC,
                    version: Descriptor::VERSION.into(),
                    page_size: (page_size as u32).into(),
                    compression: compression as u8,
                    compression_level,
                    _pad_0: Default::default(),
                };
                store.append(&descriptor, bytemuck::bytes_of(&hdr))?;
                Ok(Ok(Self::blob(
                    store,
                    BlobSet {
                        page_size,
                        compression,
                        compression_level,
                        descriptor,
                        table,
                        pages,
                        tail,
                    },
                )))
            }
            (Err(e), Err(_), Err(_), Err(_)) => Ok(Err(e)),
            _ => todo!("blob missing"),
        }
    }

    pub fn find(store: &'a T, name: &str) -> io::Result<Option<Self>> {
        let descriptor = concat(name, DESCRIPTOR_SUFFIX);
        let table = concat(name, TABLE_SUFFIX);
        let pages = concat(name, PAGES_SUFFIX);
        let tail = concat(name, TAIL_SUFFIX);
        let f = |x| store.find(x);
        match (f(&descriptor)?, f(&table)?, f(&pages)?, f(&tail)?) {
            (Some(descriptor), Some(table), Some(pages), Some(tail)) => {
                let hdr = &mut [0; 32];
                let n = store.read_at(&descriptor, 0, hdr)?;
                if n < 32 {
                    todo!("descriptor too short");
                }
                let hdr = bytemuck::cast_ref::<_, Descriptor>(hdr);
                if hdr.magic != Descriptor::MAGIC {
                    todo!("bad descriptor magic");
                }
                if hdr.version != Descriptor::VERSION {
                    todo!("bad descriptor version");
                }
                let page_size = PageSize::try_from(u32::from(hdr.page_size)).unwrap();
                let compression = Compression::try_from(hdr.compression).unwrap();
                let compression_level = hdr.compression_level.into();
                Ok(Some(Self::blob(
                    store,
                    BlobSet {
                        page_size,
                        compression,
                        compression_level,
                        descriptor,
                        table,
                        pages,
                        tail,
                    },
                )))
            }
            (None, None, None, None) => Ok(None),
            _ => todo!("blob missing"),
        }
    }

    pub fn flush(&self) -> io::Result<()> {
        self.store.flush()
    }

    pub fn size_on_disk(&self) -> io::Result<u64> {
        self.store.size_on_disk()
    }

    pub fn clear(&self) -> io::Result<()> {
        self.apply_all(|x| self.store.clear(x))
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let clen = self.compressed_len()?;
        if let Some(x) = offset.checked_sub(clen) {
            // read from tail only
            return self.store.read_at(&self.blobs.tail, x, buf);
        }
        // split into chunks and start reading
        let og_len = buf.len();
        let n = self.read_compressed_partial(offset, buf)?;
        let (offset, buf) = (offset + n as u64, &mut buf[n..]);
        if buf.is_empty() {
            return Ok(og_len);
        }
        let n = self.read_compressed_whole(offset, buf)?;
        let (offset, buf) = (offset + n as u64, &mut buf[n..]);
        let n = if offset < self.compressed_len()? {
            self.read_compressed_partial(offset, buf)?
        } else {
            self.store.read_at(&self.blobs.tail, 0, buf)?
        };
        Ok(og_len - (buf.len() - n))
    }

    pub fn read_at_exact(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        match self.read_at(offset, buf) {
            Ok(n) if n == buf.len() => Ok(()),
            Ok(n) => todo!("want {}, got {n}", buf.len()),
            Err(e) => Err(e),
        }
    }

    pub fn read_at_exact_or_none(&self, offset: u64, buf: &mut [u8]) -> io::Result<bool> {
        match self.read_at(offset, buf) {
            Ok(n) if n == buf.len() => Ok(true),
            Ok(0) => Ok(false),
            Ok(_) => todo!(),
            Err(e) => Err(e),
        }
    }

    pub fn read_at_array<const N: usize>(&self, offset: u64) -> io::Result<[u8; N]> {
        let mut buf = [0; N];
        self.read_at_exact(offset, &mut buf)?;
        Ok(buf)
    }

    pub fn append(&self, data: &[u8]) -> io::Result<u64> {
        // split into (start, middle, end)
        // add tail with start to fill a page
        // add middle directly as pages
        // add remainder to "cleared" tail

        let offset = self.len()?;

        let page_size = self.blobs.page_size as u64;
        let page_mask = page_size - 1;

        let tail = &self.blobs.tail;
        let n = self.store.len(tail)?.wrapping_neg() & page_mask;
        let n = usize::try_from(n).expect("u32 <= usize");
        let n = n.min(data.len());
        let (start, data) = data.split_at(n);
        self.store.append(tail, start)?;

        if self.store.len(tail)? >= page_size {
            assert!(self.store.len(tail)? == page_size, "tail too large");
            let buf = &mut vec![0; page_size as usize];
            let n = self.store.read_at(tail, 0, buf)?;
            assert_eq!(n, buf.len());
            self.append_page(buf)?;
            self.store.clear(tail)?;
        }

        let mut it = data.chunks_exact(page_size as usize);
        for page in &mut it {
            self.append_page(page)?;
        }

        self.store.append(tail, it.remainder())?;
        assert!(self.store.len(tail)? < page_size, "tail is full");

        Ok(offset)
    }

    pub fn append_many(&self, data: &[&[u8]]) -> io::Result<u64> {
        let n = self.len()?;
        for x in data {
            self.append(x)?;
        }
        Ok(n)
    }

    pub fn delete(self) -> io::Result<()> {
        [
            self.blobs.descriptor,
            self.blobs.table,
            self.blobs.pages,
            self.blobs.tail,
        ]
        .into_iter()
        .try_for_each(|x| self.store.delete(x))
    }

    fn read_compressed_partial(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let page_size = self.blobs.page_size as u64;
        let page_mask = page_size - 1;
        let offt = offset & !page_mask;

        let mut cache = self.cache.take();

        if offt.wrapping_sub(cache.offset) != 0 {
            cache.buf.resize(page_size as usize, 0);
            // don't restore cache on error, the cached data might be partially modified
            self.read_compressed_whole(offt, &mut cache.buf)?;
            cache.offset = offt;
        }

        let start = (offset & page_mask) as usize;
        let end = start + buf.len();
        let start = start.min(cache.buf.len());
        let end = end.min(cache.buf.len());
        let buf = &mut buf[..end - start];
        buf.copy_from_slice(&cache.buf[start..end]);

        self.cache.set(cache);

        Ok(buf.len())
    }

    fn read_compressed_whole(&self, mut offset: u64, mut buf: &mut [u8]) -> io::Result<usize> {
        let page_size = self.blobs.page_size as u64;
        let page_mask = page_size - 1;
        assert_eq!(offset & page_mask, 0);
        let og_len = buf.len();

        while buf.len() >= page_size as usize {
            let entry = &mut TableEntry::default();
            let entry_offt = (offset / page_size) * core::mem::size_of_val(entry) as u64;
            let n =
                self.store
                    .read_at(&self.blobs.table, entry_offt, bytemuck::bytes_of_mut(entry))?;
            if n == 0 {
                break;
            }
            assert_eq!(n, core::mem::size_of_val(entry));
            let compression = Compression::try_from(entry.algorithm).unwrap();
            let clen = u32::from(entry.compressed_len) as usize;
            let cbuf = &mut vec![0; clen];
            let part;
            (part, buf) = buf.split_at_mut(page_size as usize);
            self.store
                .read_at(&self.blobs.pages, entry.offset.into(), cbuf)?;
            decompress(compression, part, cbuf);
            offset += page_size;
        }
        Ok(og_len - buf.len())
    }

    fn append_page(&self, page: &[u8]) -> io::Result<()> {
        assert_eq!(page.len(), self.blobs.page_size as usize, "not page sized");
        let buf = &mut Vec::new();
        let (algorithm, clen) = self.blobs.compress(page, buf);
        let clen32 = u32::try_from(clen).expect("compressed len exceeds page size");
        let offset = self.store.append(&self.blobs.pages, &buf[..clen])?;
        let entry = TableEntry {
            offset: offset.into(),
            algorithm: algorithm as u8,
            _pad_0: [0; 3],
            compressed_len: clen32.into(),
        };
        self.store
            .append(&self.blobs.table, bytemuck::bytes_of(&entry))?;
        Ok(())
    }

    /// # Returns
    ///
    /// The total amount of compressed data in bytes.
    fn compressed_len(&self) -> io::Result<u64> {
        let n = self.store.len(&self.blobs.table)?;
        let n = n / 16;
        let n = n * (self.blobs.page_size as u64);
        Ok(n)
    }

    pub fn len(&self) -> io::Result<u64> {
        Ok(self.compressed_len()? + self.store.len(&self.blobs.tail)?)
    }

    fn apply_all<F, R>(&self, f: F) -> io::Result<()>
    where
        F: Fn(&T::BlobHandle) -> io::Result<R>,
    {
        // TODO transactions!
        for x in [
            &self.blobs.descriptor,
            &self.blobs.table,
            &self.blobs.pages,
            &self.blobs.tail,
        ] {
            (f)(x)?;
        }
        Ok(())
    }
}

impl<T> BlobSet<T> {
    fn compress(&self, page: &[u8], out: &mut Vec<u8>) -> (Compression, usize) {
        let f = |out: &mut Vec<u8>| {
            let n = page.len().max(out.len());
            out.resize(n, 0);
            out[..page.len()].copy_from_slice(page);
            (Compression::None, page.len())
        };
        let _n = match self.compression {
            Compression::None => return f(out),
            #[cfg(feature = "lz4")]
            Compression::Lz4 => self.compress_lz4(page, out),
            #[cfg(feature = "zstd")]
            Compression::Zstd => self.compress_zstd(page, out),
        };
        #[allow(unreachable_code)] // if all features are disabled
        if _n < page.len() {
            (self.compression, _n)
        } else {
            f(out)
        }
    }

    #[cfg(feature = "lz4")]
    fn compress_lz4(&self, page: &[u8], out: &mut Vec<u8>) -> usize {
        let n = out
            .len()
            .max(lz4_flex::block::get_maximum_output_size(page.len()));
        out.resize(n, 0);
        lz4_flex::compress_into(page, out).unwrap()
    }

    #[cfg(feature = "zstd")]
    fn compress_zstd(&self, page: &[u8], out: &mut Vec<u8>) -> usize {
        let n = out.len().max(zstd_safe::compress_bound(page.len()));
        out.resize(n, 0);
        zstd_safe::compress(&mut **out, page, self.compression_level.into()).unwrap()
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            buf: Default::default(),
            offset: u64::MAX,
        }
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

impl fmt::Display for PageSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PageSize as P;
        let s = match self {
            P::K4 => "4KiB",
            P::K8 => "8KiB",
            P::K16 => "16KiB",
            P::K32 => "32KiB",
            P::K64 => "64KiB",
            P::K128 => "128KiB",
            P::K256 => "256KiB",
            P::K512 => "512KiB",
            P::M1 => "1MiB",
            P::M2 => "2MiB",
            P::M4 => "4MiB",
            P::M8 => "8MiB",
            P::M16 => "16MiB",
        };
        f.write_str(s)
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

impl Descriptor {
    pub const MAGIC: [u8; 8] = *b"Compress";
    pub const VERSION: u32 = 0x20260317;
}

fn concat(a: &str, b: &str) -> String {
    a.to_string() + b
}

fn decompress(compression: Compression, out: &mut [u8], data: &[u8]) {
    match compression {
        Compression::None => out.copy_from_slice(data),
        #[cfg(feature = "lz4")]
        Compression::Lz4 => {
            let n = lz4_flex::decompress_into(data, out).unwrap();
            assert_eq!(n, out.len());
        }
        #[cfg(feature = "zstd")]
        Compression::Zstd => {
            let n = zstd_safe::decompress(out, data).unwrap();
            assert_eq!(n, out.len());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use toa_blob::{BlobStore, BlockShift, MemBlocks};

    fn init() -> BlobStore<MemBlocks> {
        BlobStore::init(MemBlocks::new(BlockShift::N9, 42, 10)).unwrap()
    }

    #[test]
    fn read_large() {
        let s = init();
        let b = BlobRef::create(&s, "", PageSize::K4, Compression::None, 0)
            .unwrap()
            .unwrap();
        let x = &[1; 20000];
        let y = &mut [0; 20000];
        b.append(x).unwrap();
        let n = b.read_at(1000, y).unwrap();
        assert_eq!(x.len() - 1000, n);
        assert_eq!(&x[..x.len() - 1000], &y[..n]);
    }

    #[test]
    fn read_small() {
        let s = init();
        let b = BlobRef::create(&s, "", PageSize::K4, Compression::None, 0)
            .unwrap()
            .unwrap();
        let x = &[1; 20000];
        let y = &mut [0; 100];
        b.append(x).unwrap();
        let n = b.read_at(100, y).unwrap();
        assert_eq!(n, 100);
        assert_eq!(&x[..n], &y[..n]);
    }
}
