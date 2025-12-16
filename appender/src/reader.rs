pub mod cache;

use crate::{
    DEPTH, Hash, Key, ObjectPointer, PITCH, PackOffset, PackRef, device, object, pack, record,
    record::{CompressionAlgorithm, UnknownCompressionAlgorithm},
};
use alloc::boxed::Box;
use alloc::vec::Vec;
use cache::{Cache, MicroLru};
use chacha20poly1305::{AeadInPlace, ChaCha12Poly1305, KeyInit, Tag};

pub struct Reader<D, C> {
    pack: pack::Pack,
    device: D,
    cache: C,
}

pub type Read = Vec<u8>;

pub struct IterRead<'a, D, C> {
    object: &'a mut Object<'a, D, C>,
    offset: u64,
    remaining: usize,
}

pub struct Object<'a, D, C> {
    reader: &'a Reader<D, C>,
    ptr: ObjectPointer,
}

#[derive(Clone, Debug)]
pub enum Error<D> {
    Device(D),
    Crypto(chacha20poly1305::Error),
    UnknownCompressionAlgorithm,
    CorruptedCompression,
}

struct CorruptedCompression;

impl<D, C> Reader<D, C> {
    pub fn into_device(self) -> D {
        self.device
    }
}

impl<D, C> Reader<D, C>
where
    D: device::Read,
    C: Cache<Box<[u8]>>,
{
    pub fn new(device: D, cache: C, pack: PackRef) -> Result<Self, Error<D::Error>> {
        let pack = pack::Pack::from_bytes(pack.0);
        Ok(Self {
            pack,
            cache,
            device,
        })
    }

    pub fn get(&self, key: &Hash) -> Result<Option<Object<'_, D, C>>, Error<D::Error>> {
        let x = object::reader::find(self.pack.object_trie_root, key, self.reader())?;
        Ok(x.map(|ptr| Object { reader: self, ptr }))
    }

    pub fn contains_key(&self, key: &Hash) -> Result<bool, Error<D::Error>> {
        self.get(key).map(|x| x.is_some())
    }

    pub fn iter_with<F>(&self, with: F) -> Result<(), Error<D::Error>>
    where
        F: FnMut(Hash) -> bool,
    {
        object::reader::iter_with(self.pack.object_trie_root, self.reader(), with)
    }

    fn read(&self, offset: PackOffset, len: usize) -> Result<Read, Error<D::Error>> {
        let mut cur = self.pack.record_trie_root;
        const MASK: u64 = (1 << PITCH) - 1;
        const RECORD_MASK: u64 = MASK / record::Entry::LEN as u64;
        const RLEN_P2: u8 = record::Entry::LEN.trailing_zeros() as u8;
        let index = |d| offset.0 >> (PITCH + d * (PITCH - RLEN_P2));
        for d in (1..=DEPTH).rev() {
            let data = self.read_record(d, index(d), &cur)?;
            let i = usize::try_from(index(d - 1) & RECORD_MASK).expect("1<<PITCH < usize");
            cur = record::Entry::from_bytes(
                &data[record::Entry::LEN * i..][..record::Entry::LEN]
                    .try_into()
                    .expect("exact bytes"),
            )?;
        }
        let offset = usize::try_from(offset.0 & MASK).expect("1<<PITCH < usize");
        let x = self.read_record(0, index(0), &cur)?;
        let x = &x[offset..];
        let x = &x[..len.min(x.len())];
        Ok(x.into())
    }

    fn read_record<'s>(
        &'s self,
        depth: u8,
        index: u64,
        entry: &record::Entry,
    ) -> Result<C::Get<'s>, Error<D::Error>> {
        let key = cache::Key::from_depth_index(depth, index);
        if let Some(x) = self.cache.get(key) {
            Ok(x)
        } else {
            let x = self.read_record_nocache(depth.into(), index, entry)?;
            Ok(self.cache.insert(key, x.into()))
        }
    }

    fn read_record_nocache(
        &self,
        depth: u32,
        index: u64,
        entry: &record::Entry,
    ) -> Result<Vec<u8>, Error<D::Error>> {
        let len = usize::try_from(entry.compressed_len).expect("u32 <= usize");
        let mut data = alloc::vec![0; len];
        self.device
            .read(entry.offset, &mut data)
            .map_err(Error::Device)?;
        let nonce = crate::record_nonce(depth, index);
        ChaCha12Poly1305::new(&self.pack.key)
            .decrypt_in_place_detached(&nonce, &[], &mut data, &entry.tag)
            .map_err(Error::Crypto)?;
        let len = usize::try_from(entry.uncompressed_len).expect("u32 <= usize");
        Ok(decompress(data, len, entry.compression_algorithm)?)
    }

    fn reader(&self) -> impl FnMut(PackOffset, &mut [u8]) -> Result<(), Error<D::Error>> + '_ {
        move |offset, out: &mut [_]| {
            self.read(offset, out.len())
                .map(|x| out.copy_from_slice(x.as_ref()))
        }
    }
}

impl<'a, D, C> Object<'a, D, C>
where
    D: device::Read,
    C: Cache<Box<[u8]>>,
{
    pub fn read(&mut self, offset: u64, len: usize) -> Result<Read, Error<D::Error>> {
        if self.ptr.len <= offset {
            return Ok([].into());
        }
        let len = usize::try_from(self.ptr.len - offset)
            .unwrap_or(usize::MAX)
            .min(len);
        let offset = PackOffset(self.ptr.offset.0 + offset);
        self.reader.read(offset, len)
    }

    // TODO len shouldn't be usize but u64
    pub fn read_exact(
        &'a mut self,
        offset: u64,
        len: usize,
    ) -> Result<IterRead<'a, D, C>, Error<D::Error>> {
        Ok(IterRead {
            object: self,
            offset,
            remaining: len,
        })
    }
}

impl<'a, D, C> IterRead<'a, D, C>
where
    D: device::Read,
    C: Cache<Box<[u8]>>,
{
    pub fn into_bytes(self) -> Result<Vec<u8>, Error<D::Error>> {
        let mut v = Vec::new();
        for x in self {
            v.extend_from_slice(&x?);
        }
        Ok(v)
    }
}

impl<'a, D, C> Iterator for IterRead<'a, D, C>
where
    D: device::Read,
    C: Cache<Box<[u8]>>,
{
    type Item = Result<Read, Error<D::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let r = match self.object.read(self.offset, self.remaining) {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };
        if r.is_empty() {
            self.remaining = 0;
            return None;
        }
        self.offset += u64::try_from(r.len()).expect("usize <= u64");
        self.remaining -= r.len();
        Some(Ok(r))
    }
}

impl<'a, D, C> core::iter::FusedIterator for IterRead<'a, D, C>
where
    D: device::Read,
    C: Cache<Box<[u8]>>,
{
}

impl<D> From<UnknownCompressionAlgorithm> for Error<D> {
    fn from(_: UnknownCompressionAlgorithm) -> Self {
        Self::UnknownCompressionAlgorithm
    }
}

impl<D> From<CorruptedCompression> for Error<D> {
    fn from(_: CorruptedCompression) -> Self {
        Self::CorruptedCompression
    }
}

fn decompress<'a>(
    data: Vec<u8>,
    uncompressed_len: usize,
    algorithm: CompressionAlgorithm,
) -> Result<Vec<u8>, CorruptedCompression> {
    match algorithm {
        // TODO should we allow trimming trailing zeros?
        CompressionAlgorithm::None if data.len() != uncompressed_len => Err(CorruptedCompression),
        CompressionAlgorithm::None => Ok(data),
        CompressionAlgorithm::Lz4 => todo!("lz4"),
        CompressionAlgorithm::Zstd => decompress_zstd(data, uncompressed_len),
    }
}

fn decompress_zstd(
    data: Vec<u8>,
    uncompressed_len: usize,
) -> Result<Vec<u8>, CorruptedCompression> {
    let mut b = vec![0; uncompressed_len];
    let real_len = zstd_safe::decompress(&mut *b, &data).map_err(|_| CorruptedCompression)?;
    (real_len == b.len())
        .then_some(b)
        .ok_or(CorruptedCompression)
}
