#![forbid(unsafe_code, unused_must_use, mismatched_lifetime_syntaxes)]

pub use toa_blob_compress::{BlobRef, Compression, PageSize};
pub use toa_blob_store::{BlobStore, BlobStoreExt, DuplicateBlob};
pub use toa_hash::Hash;

use ::core::{fmt, mem, ops};
use std::{
    cell::Cell,
    collections::btree_map::{BTreeMap, Entry},
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};
use toa_blob_compress::BlobSet;
use toa_hash::Domain;

const CHUNK_SIZE: u128 = 1 << 13;

pub struct Dir(pub Box<Path>);

pub struct Toa<T>
where
    T: BlobStore,
{
    store: T,
    data: BlobsTyped<T::BlobHandle, AbstractBlob<T::BlobHandle>>,
    refs: BlobsTyped<T::BlobHandle, AbstractBlob<T::BlobHandle>>,
    map: Map,
    root: Hash,
}

pub struct Blob<T> {
    file: T,
    len: Cell<u64>,
}

pub enum Object<'a, T>
where
    T: BlobStore,
{
    Data(Data<'a, T>),
    Refs(Refs<'a, T>),
}

pub struct Data<'a, T>(Typed<'a, T, AbstractBlob<T::BlobHandle>>)
where
    T: BlobStore;
pub struct Refs<'a, T>(Typed<'a, T, AbstractBlob<T::BlobHandle>>)
where
    T: BlobStore;

type Map = BTreeMap<Hash, FileRef>;

struct Typed<'a, T, CT>
where
    T: BlobStore,
{
    blobs: &'a BlobsTyped<T::BlobHandle, CT>,
    map: &'a Map,
    store: &'a T,
    location: FileRef,
}

struct BlobsTyped<T, CT> {
    chunks_full: CT,
    chunks_partial: CT,
    pairs: T,
}

// lol@names
#[derive(Clone, Copy)]
enum AbstractBlob<T> {
    Plain(T),
    Compressed(BlobSet<T>),
}

#[derive(Clone, Copy)]
struct FileRef(u64);

#[derive(Debug)]
pub enum ReadError<S> {
    MissingChunk,
    MissingPair,
    Io(S),
}

#[derive(Debug)]
pub enum ReadExactError<S> {
    MissingChunk,
    MissingPair,
    Truncated,
    Io(S),
}

impl<T> Toa<T>
where
    T: BlobStore,
    T::BlobHandle: Copy, // TODO
{
    pub fn init(
        store: T,
        page_size: PageSize,
        compression: Compression,
        compression_level: u8,
    ) -> io::Result<Result<Self, DuplicateBlob>> {
        let data = BlobsTyped::init_at(&store, "data", page_size, compression, compression_level)?;
        let refs = BlobsTyped::init_at_plain(&store, "refs")?;
        let [Ok(data), Ok(refs)] = [data, refs] else {
            return Ok(Err(DuplicateBlob));
        };
        Ok(Ok(Self {
            store,
            data,
            refs,
            map: Default::default(),
            root: Default::default(),
        }))
    }

    pub fn load(store: T) -> io::Result<Option<Self>> {
        let mut map = Map::default();
        let data = BlobsTyped::load_at(&store, "data", &mut map, Domain::Data)?;
        let refs = BlobsTyped::load_at_plain(&store, "refs", &mut map, Domain::Refs)?;
        let [Some(data), Some(refs)] = [data, refs] else {
            return Ok(None);
        };
        let mut root = [0; 32];
        if let Some(x) = store.find("root.bin")? {
            let n = store.read_at(&x, 0, &mut root)?;
            if n != 32 && n != 0 {
                todo!()
            }
        }
        let root = Hash::from_bytes(root);
        Ok(Some(Self {
            store,
            data,
            refs,
            map,
            root,
        }))
    }

    pub fn contains_key(&self, key: &Hash) -> io::Result<bool> {
        Ok(self.map.contains_key(key))
    }

    pub fn get<'a>(&'a self, key: &Hash) -> io::Result<Option<Object<'a, T>>> {
        let Some(x) = Typed::new(self, *key) else {
            return Ok(None);
        };
        let x = match x.location.ty().1 {
            Domain::Data => Object::Data(Data(x)),
            Domain::Refs => Object::Refs(Refs(x)),
        };
        Ok(Some(x))
    }

    pub fn iter_with<F>(&self, mut f: F) -> io::Result<()>
    where
        F: FnMut(Hash) -> bool,
    {
        self.map.keys().for_each(|x| {
            f(*x);
        });
        Ok(())
    }

    pub fn add_data(&mut self, data: &[u8]) -> io::Result<Hash> {
        self.data
            .add(&mut self.store, Domain::Data, data, &mut self.map)
    }

    pub fn add_refs(&mut self, refs: &[Hash]) -> io::Result<Hash> {
        self.refs.add(
            &mut self.store,
            Domain::Refs,
            bytemuck::cast_slice(refs),
            &mut self.map,
        )
    }

    pub fn size_on_disk(&self) -> io::Result<u64> {
        self.store.size_on_disk()
    }

    pub fn root(&self) -> Hash {
        self.root
    }

    pub fn set_root(&mut self, new_root: Hash) -> io::Result<()> {
        let mut x = self.store.open_clear("new_root.bin")?;
        self.store.append(&mut x, new_root.as_bytes())?;
        self.store.rename("new_root.bin", "root.bin")?;
        self.root = new_root;
        Ok(())
    }

    pub fn unmount(self) -> (T, io::Result<()>) {
        (self.store, Ok(()))
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.store.flush()
    }
}

impl Blob<fs::File> {
    /// # Returns
    ///
    /// Offset.
    fn append(&self, data: &[u8]) -> io::Result<u64> {
        self.append_many(&[data])
    }

    /// # Returns
    ///
    /// Offset.
    fn append_many(&self, data: &[&[u8]]) -> io::Result<u64> {
        let o = self.len.get();
        for x in data {
            (&self.file).write_all(x)?;
            self.len.update(|y| y + x.len() as u64);
        }
        Ok(o)
    }

    fn read_at(&self, offset: u64, mut buf: &mut [u8]) -> io::Result<usize> {
        let mut o = offset;
        while !buf.is_empty() {
            let m = std::os::unix::fs::FileExt::read_at(&self.file, buf, o)?;
            if m == 0 {
                break;
            }
            o += m as u64;
            buf = &mut buf[m..];
        }
        Ok((o - offset) as usize)
    }
}

impl<T> BlobsTyped<T, AbstractBlob<T>>
where
    T: Copy, // TODO do this properly
{
    fn init_at<S>(
        store: &S,
        dir: &str,
        page_size: PageSize,
        compression: Compression,
        compression_level: u8,
    ) -> io::Result<Result<Self, DuplicateBlob>>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let g = |name: &str| store.create(&format!("{dir}_{name}"));
        let f = |name: &str| {
            BlobRef::create(
                store,
                &format!("{dir}_{name}"),
                page_size,
                compression,
                compression_level,
            )
        };
        match (
            f("chunks_full.bin")?,
            f("chunks_partial.bin")?,
            g("pairs.bin")?,
        ) {
            (Ok(chunks_full), Ok(chunks_partial), Ok(pairs)) => Ok(Ok(Self {
                chunks_full: AbstractBlob::Compressed(*chunks_full.blob_set()),
                chunks_partial: AbstractBlob::Compressed(*chunks_partial.blob_set()),
                pairs,
            })),
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => Ok(Err(e)),
        }
    }

    fn init_at_plain<S>(store: &S, dir: &str) -> io::Result<Result<Self, DuplicateBlob>>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let f = |name: &str| store.create(&format!("{dir}_{name}"));
        match (
            f("chunks_full.bin")?,
            f("chunks_partial.bin")?,
            f("pairs.bin")?,
        ) {
            (Ok(chunks_full), Ok(chunks_partial), Ok(pairs)) => Ok(Ok(Self {
                chunks_full: AbstractBlob::Plain(chunks_full),
                chunks_partial: AbstractBlob::Plain(chunks_partial),
                pairs,
            })),
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => Ok(Err(e)),
        }
    }

    fn load_at<S>(store: &S, dir: &str, map: &mut Map, domain: Domain) -> io::Result<Option<Self>>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let g = |name: &str| store.find(&format!("{dir}_{name}"));
        let f = |name: &str| BlobRef::find(store, &format!("{dir}_{name}"));
        match (
            f("chunks_full.bin")?,
            f("chunks_partial.bin")?,
            g("pairs.bin")?,
        ) {
            (Some(chunks_full), Some(chunks_partial), Some(pairs)) => {
                let mut s = Self {
                    chunks_full: AbstractBlob::Compressed(*chunks_full.blob_set()),
                    chunks_partial: AbstractBlob::Compressed(*chunks_partial.blob_set()),
                    pairs,
                };
                s.load(store, map, domain)?;
                Ok(Some(s))
            }
            (None, _, _) | (_, None, _) | (_, _, None) => Ok(None),
        }
    }

    fn load_at_plain<S>(
        store: &S,
        dir: &str,
        map: &mut Map,
        domain: Domain,
    ) -> io::Result<Option<Self>>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let f = |name: &str| store.find(&format!("{dir}_{name}"));
        match (
            f("chunks_full.bin")?,
            f("chunks_partial.bin")?,
            f("pairs.bin")?,
        ) {
            (Some(chunks_full), Some(chunks_partial), Some(pairs)) => {
                let mut s = Self {
                    chunks_full: AbstractBlob::Plain(chunks_full),
                    chunks_partial: AbstractBlob::Plain(chunks_partial),
                    pairs,
                };
                s.load(store, map, domain)?;
                Ok(Some(s))
            }
            (None, _, _) | (_, None, _) | (_, _, None) => Ok(None),
        }
    }

    fn add<S>(
        &mut self,
        store: &mut S,
        domain: Domain,
        data: &[u8],
        map: &mut Map,
    ) -> io::Result<Hash>
    where
        S: BlobStore<BlobHandle = T>,
    {
        if data.len() <= CHUNK_SIZE as usize {
            self.add_chunk(store, domain, data, map)
        } else {
            let mut stack = arrayvec::ArrayVec::<Hash, { 128 - 13 }>::new();
            let split_n = ((data.len() - 1) & 0x1fff) + 1;
            let (perfect, tail) = data.split_at(data.len() - split_n);
            for (i, y) in perfect.chunks_exact(CHUNK_SIZE as usize).enumerate() {
                let mut y = self.add_chunk(store, domain, y, map)?;
                let mut len = 1 << 16;
                while stack.len() >= (i + 1).count_ones() as usize {
                    let x = stack.pop().expect("at least one element");
                    len <<= 1;
                    y = self.add_pair(store, domain, &x, &y, len, map)?;
                }
                stack.push(y);
            }

            let len = (data.len() as u128) << 3;
            let mut y = self.add_chunk(store, domain, tail, map)?;
            let mut mask = 0xffff;
            let top_i = len.wrapping_sub(1); // special-case for len=0
            while let Some(x) = stack.pop() {
                debug_assert_eq!(
                    (top_i & !mask).count_ones(),
                    1 + stack.len() as u32,
                    "length bits should correlate to stack depth"
                );
                let bits = (top_i & !mask).trailing_zeros();
                mask = (1 << (bits + 1)) - 1;
                let pair_len = (top_i & mask) + 1;
                y = self.add_pair(store, domain, &x, &y, pair_len, map)?;
            }
            Ok(y)
        }
    }

    fn add_chunk<S>(
        &mut self,
        store: &mut S,
        domain: Domain,
        chunk: &[u8],
        map: &mut Map,
    ) -> io::Result<Hash>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let key = toa_hash::hash_chunk(domain, chunk);
        if let Entry::Vacant(e) = map.entry(key) {
            e.insert(self.store_chunk(store, domain, chunk)?);
        }
        Ok(key)
    }

    fn add_pair<S>(
        &mut self,
        store: &mut S,
        domain: Domain,
        x: &Hash,
        y: &Hash,
        len: u128,
        map: &mut Map,
    ) -> io::Result<Hash>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let key = toa_hash::hash_pair(*x, *y, len);
        if let Entry::Vacant(e) = map.entry(key) {
            e.insert(self.store_pair(store, domain, x, y, len)?);
        }
        Ok(key)
    }

    fn store_chunk<S>(&mut self, store: &mut S, domain: Domain, bytes: &[u8]) -> io::Result<FileRef>
    where
        S: BlobStore<BlobHandle = T>,
    {
        if let Ok(bytes) = bytes.try_into() {
            self.store_chunk_full(store, domain, bytes)
        } else {
            self.store_chunk_partial(store, domain, bytes)
        }
    }

    fn store_chunk_full<S>(
        &mut self,
        store: &mut S,
        domain: Domain,
        bytes: &[u8; CHUNK_SIZE as usize],
    ) -> io::Result<FileRef>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let offt = self.chunks_full.append(store, bytes)?;
        Ok(FileRef::new_chunk_full(domain, offt))
    }

    fn store_chunk_partial<S>(
        &mut self,
        store: &mut S,
        domain: Domain,
        bytes: &[u8],
    ) -> io::Result<FileRef>
    where
        S: BlobStore<BlobHandle = T>,
    {
        assert!(bytes.len() < CHUNK_SIZE as usize, "partial chunk too large");
        let hdr = u16::try_from(bytes.len() << 3)
            .expect("less than CHUNK_SIZE as usize bytes / 65536 bits");
        let pad = (!(2 + bytes.len()) + 1) & 7;
        let pad = &[0; 8][..pad];
        let offt = self
            .chunks_partial
            .append_many(store, &[&hdr.to_le_bytes(), bytes, pad])?;
        Ok(FileRef::new_chunk_partial(domain, offt))
    }

    fn store_pair<S>(
        &mut self,
        store: &mut S,
        domain: Domain,
        x: &Hash,
        y: &Hash,
        len: u128,
    ) -> io::Result<FileRef>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let mut buf = [0; 80];
        buf[00..32].copy_from_slice(x.as_bytes());
        buf[32..64].copy_from_slice(y.as_bytes());
        buf[64..].copy_from_slice(&len.to_le_bytes());
        let offt = store.append(&mut self.pairs, &buf)?;
        Ok(FileRef::new_pair(domain, offt))
    }

    fn load<S>(&mut self, store: &S, map: &mut Map, domain: Domain) -> io::Result<()>
    where
        S: BlobStore<BlobHandle = T>,
    {
        self.load_chunks_full(store, map, domain)?;
        self.load_chunks_partial(store, map, domain)?;
        self.load_pairs(store, map, domain)?;
        Ok(())
    }

    fn load_chunks_full<S>(&mut self, store: &S, map: &mut Map, domain: Domain) -> io::Result<()>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let mut buf = vec![0; CHUNK_SIZE as usize];
        let mut offt = 0;
        while self
            .chunks_full
            .read_at_exact_or_none(store, offt, &mut buf)?
        {
            let key = toa_hash::hash_chunk(domain, &buf);
            map.insert(key, FileRef::new_chunk_full(domain, offt));
            offt += buf.len() as u64;
        }
        Ok(())
    }

    fn load_chunks_partial<S>(&mut self, store: &S, map: &mut Map, domain: Domain) -> io::Result<()>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let mut buf = vec![0; CHUNK_SIZE as usize];
        let len = &mut [0; 2];
        let mut offt = 0;
        while self
            .chunks_partial
            .read_at_exact_or_none(store, offt, len)?
        {
            let len = u16::from_le_bytes(*len) >> 3;
            let buf = &mut buf[..usize::from(len)];
            self.chunks_partial.read_at_exact(store, offt + 2, buf)?;
            let key = toa_hash::hash_chunk(domain, buf);
            map.insert(key, FileRef::new_chunk_partial(domain, offt));
            offt += align8(2 + u64::from(len));
        }
        Ok(())
    }

    fn load_pairs<S>(&mut self, store: &S, map: &mut Map, domain: Domain) -> io::Result<()>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let mut buf = [0; 80];
        let mut offt = 0;
        while store.read_at_exact_or_none(&self.pairs, offt, &mut buf)? {
            let ([x, y], len) = bytes_to_pair(buf);
            let key = toa_hash::hash_pair(x, y, len);
            map.insert(key, FileRef::new_pair(domain, offt));
            offt += buf.len() as u64;
        }
        Ok(())
    }
}

impl FileRef {
    const TY_CHUNK_FULL: u64 = 2;
    const TY_CHUNK_PARTIAL: u64 = 4;
    const TY_PAIR: u64 = 6;

    fn new(offset: u64, ty: u64, domain: Domain) -> Self {
        assert!(ty < 8);
        assert!(offset % 8 == 0);
        Self(offset | ty | u64::from(domain == Domain::Refs))
    }

    fn new_pair(domain: Domain, offset: u64) -> Self {
        Self::new(offset, Self::TY_PAIR, domain)
    }

    fn new_chunk_full(domain: Domain, offset: u64) -> Self {
        Self::new(offset, Self::TY_CHUNK_FULL, domain)
    }

    fn new_chunk_partial(domain: Domain, offset: u64) -> Self {
        Self::new(offset, Self::TY_CHUNK_PARTIAL, domain)
    }

    fn ty(&self) -> (u64, Domain) {
        let domain = if self.0 & 1 == 0 {
            Domain::Data
        } else {
            Domain::Refs
        };
        (self.0 & 6, domain)
    }

    fn offset(&self) -> u64 {
        self.0 & !7
    }
}

impl<'a, T> Object<'a, T>
where
    T: BlobStore,
{
    pub fn into_data(self) -> Option<Data<'a, T>> {
        let Self::Data(x) = self else { return None };
        Some(x)
    }

    pub fn into_refs(self) -> Option<Refs<'a, T>> {
        let Self::Refs(x) = self else { return None };
        Some(x)
    }
}

impl<'a, T> Typed<'a, T, AbstractBlob<T::BlobHandle>>
where
    T: BlobStore,
{
    fn new(toa: &'a Toa<T>, key: Hash) -> Option<Self> {
        let location = *toa.map.get(&key)?;
        let blobs = match location.ty().1 {
            Domain::Data => &toa.data,
            Domain::Refs => &toa.refs,
        };
        Some(Self {
            store: &toa.store,
            blobs,
            map: &toa.map,
            location,
        })
    }

    fn with_key(&self, key: Hash) -> Option<Self> {
        Some(Self {
            location: *self.map.get(&key)?,
            ..*self
        })
    }
}

impl<'a, T> Data<'a, T>
where
    T: BlobStore,
    T::BlobHandle: Copy, // TODO
{
    /// # Note
    ///
    /// Offset is in *bytes*.
    pub fn read(&self, offset: u128, buf: &mut [u8]) -> Result<usize, ReadError<io::Error>> {
        self.0.read(offset, buf)
    }

    /// # Note
    ///
    /// Offset is in *bytes*.
    pub fn read_exact(
        &self,
        offset: u128,
        buf: &mut [u8],
    ) -> Result<(), ReadExactError<io::Error>> {
        self.0.read_exact(offset, buf)
    }

    /// # Note
    ///
    /// Offset is in *bytes*.
    pub fn read_array<const N: usize>(
        &self,
        offset: u128,
    ) -> Result<[u8; N], ReadExactError<io::Error>> {
        self.0.read_array(offset)
    }

    pub fn len(&self) -> io::Result<u128> {
        self.0.len_bits().map(|x| x >> 3)
    }
}

impl<'a, T> Refs<'a, T>
where
    T: BlobStore,
    T::BlobHandle: Copy, // TODO
{
    /// # Note
    ///
    /// Offset is in *hashes*.
    pub fn read(&self, offset: u128, buf: &mut [Hash]) -> Result<usize, ReadError<io::Error>> {
        let offset = offset.saturating_mul(mem::size_of::<Hash>() as u128);
        self.0.read(offset, bytemuck::cast_slice_mut(buf))
    }

    /// # Note
    ///
    /// Offset is in *hashes*.
    pub fn read_exact(
        &self,
        offset: u128,
        buf: &mut [Hash],
    ) -> Result<(), ReadExactError<io::Error>> {
        let offset = offset.saturating_mul(mem::size_of::<Hash>() as u128);
        self.0.read_exact(offset, bytemuck::cast_slice_mut(buf))
    }

    /// # Note
    ///
    /// Offset is in *hashes*.
    pub fn read_array<const N: usize>(
        &self,
        offset: u128,
    ) -> Result<[Hash; N], ReadExactError<io::Error>> {
        // bytemuck is being annoying, so reimplement using read_exact
        let mut buf = [Hash::default(); N];
        self.read_exact(offset, &mut buf)?;
        Ok(buf)
    }

    pub fn len(&self) -> io::Result<u128> {
        self.0.len_bits().map(|x| x >> 8)
    }
}

impl<'a, T> Typed<'a, T, AbstractBlob<T::BlobHandle>>
where
    T: BlobStore,
    T::BlobHandle: Copy, // TODO
{
    pub fn read(&self, offset: u128, buf: &mut [u8]) -> Result<usize, ReadError<io::Error>> {
        match self.location.ty().0 {
            FileRef::TY_CHUNK_FULL => self.read_chunk_full(offset, buf),
            FileRef::TY_CHUNK_PARTIAL => self.read_chunk_partial(offset, buf),
            FileRef::TY_PAIR => self.read_pair(offset, buf),
            _ => unreachable!("invalid FileRef type"),
        }
    }

    pub fn read_exact(
        &self,
        offset: u128,
        buf: &mut [u8],
    ) -> Result<(), ReadExactError<io::Error>> {
        let n = self.read(offset, buf)?;
        if n != buf.len() {
            return Err(ReadExactError::Truncated);
        }
        Ok(())
    }

    pub fn read_array<const N: usize>(
        &self,
        offset: u128,
    ) -> Result<[u8; N], ReadExactError<io::Error>> {
        let mut buf = [0; N];
        self.read_exact(offset, &mut buf)?;
        Ok(buf)
    }

    pub fn len_bits(&self) -> io::Result<u128> {
        match self.location.ty().0 {
            FileRef::TY_CHUNK_FULL => Ok(CHUNK_SIZE << 3),
            FileRef::TY_CHUNK_PARTIAL => self
                .blobs
                .chunks_partial
                .read_at_array(self.store, self.location.offset())
                .map(u16::from_le_bytes)
                .map(u128::from),
            FileRef::TY_PAIR => self
                .store
                .read_at_array(&self.blobs.pairs, self.location.offset() + 64)
                .map(u128::from_le_bytes),
            _ => unreachable!("invalid FileRef type"),
        }
    }

    fn read_pair(&self, offset: u128, buf: &mut [u8]) -> Result<usize, ReadError<io::Error>> {
        if buf.is_empty() {
            return Ok(0);
        }

        let ([x, y], len) = self
            .store
            .read_at_array(&self.blobs.pairs, self.location.offset())
            .map(bytes_to_pair)
            .map_err(ReadError::Io)?;

        let len = align8(len) >> 3;
        if offset >= len {
            return Ok(0);
        }

        let x = self.with_key(x).unwrap();
        let y = self.with_key(y).unwrap();
        let xl = len.next_power_of_two() >> 1;
        let n = xl.saturating_sub(offset).min(buf.len() as u128) as usize;
        let (xb, yb) = buf.split_at_mut(n);
        Ok(x.read(offset, xb)? + y.read(offset.saturating_sub(xl), yb)?)
    }

    fn read_chunk_full(&self, offset: u128, buf: &mut [u8]) -> Result<usize, ReadError<io::Error>> {
        let len = buf.len().min(CHUNK_SIZE.saturating_sub(offset) as usize);
        let buf = &mut buf[..len];
        if buf.is_empty() {
            return Ok(0);
        }
        self.blobs
            .chunks_full
            .read_at(self.store, self.location.offset() + offset as u64, buf)
            .map_err(ReadError::Io)?;
        Ok(len)
    }

    fn read_chunk_partial(
        &self,
        offset: u128,
        buf: &mut [u8],
    ) -> Result<usize, ReadError<io::Error>> {
        // FIXME it is possible that the tail of the blob is not flushed
        // if I/O is suddenly interrupted.
        // Due to this a partial chunk might get torn, leading to an error
        // here if reloaded.
        // The best fix would be to implement transactions. Transactions will
        // make consistency guarantees a lot easier in many cases, as well as
        // allow fsck to completely restore consistency at the lowest layer.
        let nb = self
            .blobs
            .chunks_partial
            .read_at_array(self.store, self.location.offset())
            .map(u16::from_le_bytes)
            .map_err(ReadError::Io)?;
        let n = align8(nb) >> 3;
        let n = buf.len().min(u128::from(n).saturating_sub(offset) as usize);
        let buf = &mut buf[..n];
        if buf.is_empty() {
            return Ok(0);
        }
        self.blobs
            .chunks_partial
            .read_at(self.store, self.location.offset() + 2 + offset as u64, buf)
            .map_err(ReadError::Io)?;
        Ok(n)
    }

    #[cfg(test)]
    fn dump_tree(&self, depth: usize) {
        print!("{:>depth$}    ", "");
        match self.location.ty().0 {
            FileRef::TY_CHUNK_FULL => println!("F"),
            FileRef::TY_CHUNK_PARTIAL => {
                let nb = self
                    .blobs
                    .chunks_partial
                    .read_at_array(self.store, self.location.offset())
                    .map(u16::from_le_bytes)
                    .unwrap();
                println!("{}", nb);
            }
            FileRef::TY_PAIR => {
                let ([x, y], len) = self
                    .store
                    .read_at_array(&self.blobs.pairs, self.location.offset())
                    .map(bytes_to_pair)
                    .unwrap();
                println!("{}", len);
                self.with_key(x).unwrap().dump_tree(depth + 2);
                self.with_key(y).unwrap().dump_tree(depth + 2);
            }
            _ => unreachable!("invalid FileRef type"),
        }
    }
}

impl<T> From<ReadError<T>> for ReadExactError<T> {
    fn from(x: ReadError<T>) -> Self {
        match x {
            ReadError::MissingChunk => Self::MissingChunk,
            ReadError::MissingPair => Self::MissingPair,
            ReadError::Io(x) => Self::Io(x),
        }
    }
}

impl<T: BlobStore> Clone for Data<'_, T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: BlobStore> Clone for Refs<'_, T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: BlobStore> Clone for Typed<'_, T, AbstractBlob<T::BlobHandle>> {
    fn clone(&self) -> Self {
        Self {
            store: self.store,
            blobs: self.blobs,
            map: self.map,
            location: self.location,
        }
    }
}

impl<T: BlobStore> Copy for Data<'_, T> {}
impl<T: BlobStore> Copy for Refs<'_, T> {}
impl<T: BlobStore> Copy for Typed<'_, T, AbstractBlob<T::BlobHandle>> {}

macro_rules! abstract_blob_imp {
    ($(fn $fn:ident<S>(&self, store: &S, $($param:ident: $ty:ty),*) -> $ret:ty;)*) => {
        $(
            fn $fn<S>(&self, store: &S, $($param: $ty,)*) -> $ret
            where
                S: BlobStore<BlobHandle = T>,
            {
                match self {
                    Self::Plain(x) => store.$fn(x, $($param,)*),
                    Self::Compressed(x) => BlobRef::blob(store, *x).$fn($($param,)*),
                }
            }
        )*
    }
}

impl<T> AbstractBlob<T>
where
    T: Copy,
{
    abstract_blob_imp! {
        fn append<S>(&self, store: &S, data: &[u8]) -> io::Result<u64>;
        fn append_many<S>(&self, store: &S, data: &[&[u8]]) -> io::Result<u64>;
        fn read_at<S>(&self, store: &S, offt: u64, buf: &mut [u8]) -> io::Result<usize>;
        fn read_at_exact_or_none<S>(&self, store: &S, offt: u64, buf: &mut [u8]) -> io::Result<bool>;
        fn read_at_exact<S>(&self, store: &S, offt: u64, buf: &mut [u8]) -> io::Result<()>;
    }
    fn read_at_array<const N: usize, S>(&self, store: &S, offt: u64) -> io::Result<[u8; N]>
    where
        S: BlobStore<BlobHandle = T>,
    {
        let mut buf = [0; N];
        self.read_at_exact(store, offt, &mut buf)?;
        Ok(buf)
    }
}

impl fmt::Debug for FileRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (ty, domain) = self.ty();
        let ty = match ty {
            Self::TY_CHUNK_FULL => "full",
            Self::TY_CHUNK_PARTIAL => "part",
            Self::TY_PAIR => "pair",
            _ => "??",
        };
        write!(f, "{ty}:{domain:?}:{}", self.offset())
    }
}

impl Dir {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&path)?;
        Ok(Self(path.into()))
    }

    fn open_or_create(
        &self,
        name: &str,
        create: bool,
        truncate: bool,
    ) -> io::Result<Blob<fs::File>> {
        let mut path = PathBuf::from(&*self.0);
        path.push(name);
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(create)
            .truncate(truncate)
            .open(path)
            .and_then(|file| {
                let len = file.metadata()?.len().into();
                Ok(Blob { file, len })
            })
    }

    fn path(&self, name: &str) -> PathBuf {
        let mut x = PathBuf::from(&*self.0);
        x.push(name);
        x
    }
}

impl BlobStore for Dir {
    type BlobHandle = Blob<fs::File>;

    fn open_clear(&self, name: &str) -> io::Result<Self::BlobHandle> {
        self.open_or_create(name, true, true)
    }
    fn rename(&self, old_name: &str, new_name: &str) -> io::Result<()> {
        fs::rename(self.path(old_name), self.path(new_name))
    }
    fn append(&self, blob: &Self::BlobHandle, data: &[u8]) -> io::Result<u64> {
        blob.append(data)
    }
    fn append_many(&self, blob: &Self::BlobHandle, data: &[&[u8]]) -> io::Result<u64> {
        blob.append_many(data)
    }
    fn read_at(&self, blob: &Self::BlobHandle, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        blob.read_at(offset, buf)
    }
    fn flush(&self) -> io::Result<()> {
        todo!("Dir flush")
    }
    fn size_on_disk(&self) -> io::Result<u64> {
        std::fs::read_dir(&self.0)?.try_fold(0, |s, x| Ok(s + x?.metadata()?.len()))
    }

    fn find(&self, _name: &str) -> io::Result<Option<Self::BlobHandle>> {
        todo!()
    }
    fn create(&self, _name: &str) -> io::Result<Result<Self::BlobHandle, toa_blob::DuplicateBlob>> {
        todo!()
    }
    fn create_unzoned(
        &self,
        _name: &str,
    ) -> io::Result<Result<Self::BlobHandle, toa_blob::DuplicateBlob>> {
        todo!()
    }
    fn len(&self, _blob: &Self::BlobHandle) -> io::Result<u64> {
        todo!()
    }
    fn clear(&self, _blob: &Self::BlobHandle) -> io::Result<()> {
        todo!()
    }
    fn delete(&self, _blob: Self::BlobHandle) -> io::Result<()> {
        todo!()
    }
}

fn align8<T>(x: T) -> T
where
    T: ops::Add<Output = T> + ops::Not<Output = T> + ops::BitAnd<Output = T> + From<u8>,
{
    (x + T::from(7)) & !T::from(7)
}

fn bytes_to_pair(bytes: [u8; 80]) -> ([Hash; 2], u128) {
    let x = Hash::from_slice(&bytes[0..32]);
    let y = Hash::from_slice(&bytes[32..64]);
    let len = u128::from_le_bytes(bytes[64..].try_into().expect("16 bytes"));
    ([x, y], len)
}

#[cfg(test)]
mod test {
    use super::*;
    use toa_blob::{BlobStore, MemZones};

    type Toa = super::Toa<BlobStore<MemZones<512>>>;

    struct Test {
        toa: Toa,
    }

    impl Test {
        fn add(&mut self, data: &[u8]) -> Hash {
            let key = self.toa.add_data(data).expect("add_data failed");
            assert_eq!(key, toa_hash::hash(Domain::Data, data));
            key
        }

        fn assert_eq(&self, key: &Hash, value: &[u8]) {
            let o = self
                .toa
                .get(&key)
                .expect("get failed")
                .expect("object does not exist");
            let o = match o {
                Object::Data(o) => o,
                Object::Refs(_) => panic!("expected data, got refs"),
            };
            o.0.dump_tree(0);
            assert_eq!(
                o.0.len_bits().unwrap(),
                (value.len() as u128) << 3,
                "lengths do not match"
            );
            let x = &mut *vec![0; value.len()];
            let n = o.0.read(0, x).expect("read failed");
            assert_eq!(n, value.len(), "read unexpectedly truncated");
            let f = String::from_utf8_lossy;
            assert!(x == value, "{:?} <> {:?}", f(&x), f(value));
        }
    }

    fn init() -> Test {
        let store = BlobStore::init(MemZones::new(1 << 20, 20)).unwrap();
        let toa = Toa::init(store, PageSize::K4, Compression::None, 0)
            .expect("toa init failed")
            .expect("duplicate toa store");
        Test { toa }
    }

    #[test]
    fn insert_one_empty() {
        let mut s = init();
        let key = s.add(b"");
        s.assert_eq(&key, &[]);
    }

    #[test]
    fn insert_one() {
        let mut s = init();
        let key = s.add(b"Hello, world!");
        s.assert_eq(&key, b"Hello, world!");
    }

    #[test]
    fn insert_two() {
        let mut s = init();
        let a = s.add(b"Hello, world!");
        let b = s.add(b"Greetings!");
        s.assert_eq(&a, b"Hello, world!");
        s.assert_eq(&b, b"Greetings!");
    }

    #[test]
    fn insert_many() {
        let mut s = init();
        let f = |x| format!("A number {x}").into_bytes();
        let keys = (0..1 << 12).map(|i| s.add(&f(i))).collect::<Vec<_>>();
        keys.iter()
            .enumerate()
            .for_each(|(i, k)| s.assert_eq(k, &f(i)));
    }

    #[test]
    fn insert_one_3div2_chunks() {
        let mut s = init();
        let v = (0..CHUNK_SIZE as usize * 3 / 2)
            .fold(String::new(), |s, _| s + "x")
            .into_bytes();
        let k = s.add(&v);
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_one_2_chunks() {
        let mut s = init();
        let v = (0..CHUNK_SIZE as usize * 2)
            .fold(String::new(), |s, _| s + "x")
            .into_bytes();
        let k = s.add(&v);
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_one_large() {
        let mut s = init();
        let v = (0..1 << 19)
            .fold(String::new(), |s, _| s + "x")
            .into_bytes();
        let k = s.add(&v);
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_one_large_zeros() {
        let mut s = init();
        let v = vec![0; 1 << 20];
        let k = s.add(&v);
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_many_large() {
        let n = 1 << 21;
        let mut s = init();
        let keys = (0..=255)
            .map(|x| (x, s.add(&vec![x; n])))
            .collect::<Vec<_>>();
        keys.iter().for_each(|(x, k)| s.assert_eq(k, &vec![*x; n]));
    }

    #[test]
    fn reload() {
        let mut s = init();
        let a = s.add(b"Hello, world!");
        let b = s.add(b"Hello, planet!");
        let c = s.add(&vec![b'x'; 1 << 15]);
        let Test { toa } = s;
        let (store, res) = toa.unmount();
        res.unwrap();
        let toa = Toa::load(store)
            .expect("reload")
            .expect("toa store missing");
        let s = Test { toa };
        s.assert_eq(&a, b"Hello, world!");
        s.assert_eq(&b, b"Hello, planet!");
        s.assert_eq(&c, &vec![b'x'; 1 << 15]);
    }

    /// Tests for bugs found with fuzzing.
    ///
    /// Might be manually reduced to simplify the test case.
    mod fuzz {
        use super::*;

        #[test]
        fn read_partial_chunk_truncated() {
            let bytes = vec![0; 11 * 8192 + 1];
            let mut s = init();
            let k = s.add(&bytes);
            s.assert_eq(&k, &bytes);
        }
    }
}
