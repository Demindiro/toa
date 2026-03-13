#![forbid(unsafe_code, unused_must_use, mismatched_lifetime_syntaxes)]

pub use toa_hash::Hash;

use ::core::{fmt, mem, ops};
use std::{
    collections::btree_map::{BTreeMap, Entry},
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};
use toa_hash::Domain;

const CHUNK_SIZE: u128 = 1 << 13;

pub trait BlobStore {
    type BlobHandle;

    fn open(&mut self, name: &str) -> io::Result<Self::BlobHandle>;
    fn open_clear(&mut self, name: &str) -> io::Result<Self::BlobHandle>;
    fn rename(&mut self, old_name: &str, new_name: &str) -> io::Result<()>;
    fn append(&mut self, blob: &mut Self::BlobHandle, data: &[u8]) -> io::Result<u64>;
    fn append_many(&mut self, blob: &mut Self::BlobHandle, data: &[&[u8]]) -> io::Result<u64>;
    fn read_at(&self, blob: &Self::BlobHandle, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn size_on_disk(&self) -> io::Result<u64>;
}

trait BlobStoreExt: BlobStore {
    fn read_at_exact(
        &self,
        blob: &Self::BlobHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<bool> {
        match self.read_at(blob, offset, buf) {
            Ok(n) if n == buf.len() => Ok(true),
            Ok(_) => todo!(),
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

impl<T: BlobStore> BlobStoreExt for T {}

pub struct Dir(pub Box<Path>);

pub struct Toa<T>
where
    T: BlobStore,
{
    store: T,
    data: BlobsTyped<T>,
    refs: BlobsTyped<T>,
    map: Map,
    root: Hash,
}

pub struct Blob<T> {
    file: T,
    len: u64,
}

pub enum Object<'a, T>
where
    T: BlobStore,
{
    Data(Data<'a, T>),
    Refs(Refs<'a, T>),
}

pub struct Data<'a, T>(Typed<'a, T>)
where
    T: BlobStore;
pub struct Refs<'a, T>(Typed<'a, T>)
where
    T: BlobStore;

type Map = BTreeMap<Hash, FileRef>;

struct Typed<'a, T>
where
    T: BlobStore,
{
    blobs: &'a BlobsTyped<T>,
    map: &'a Map,
    store: &'a T,
    location: FileRef,
}

struct BlobsTyped<T>
where
    T: BlobStore,
{
    chunks_full: T::BlobHandle,
    chunks_partial: T::BlobHandle,
    pairs: T::BlobHandle,
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
{
    pub fn open(mut store: T) -> io::Result<Self> {
        let mut map = Map::default();
        let data = BlobsTyped::open_at(&mut store, "data", &mut map, Domain::Data)?;
        let refs = BlobsTyped::open_at(&mut store, "refs", &mut map, Domain::Refs)?;
        let mut root = [0; 32];
        let x = store.open("root.bin")?;
        let n = store.read_at(&x, 0, &mut root)?;
        if n != 32 && n != 0 {
            todo!()
        };
        let root = Hash::from_bytes(root);
        Ok(Self {
            store,
            data,
            refs,
            map,
            root,
        })
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
}

impl Blob<fs::File> {
    /// # Returns
    ///
    /// Offset.
    fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        self.append_many(&[data])
    }

    /// # Returns
    ///
    /// Offset.
    fn append_many(&mut self, data: &[&[u8]]) -> io::Result<u64> {
        let o = self.len;
        for x in data {
            self.file.write_all(x)?;
            self.len += x.len() as u64;
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

impl<T> BlobsTyped<T>
where
    T: BlobStore,
{
    fn open_at(store: &mut T, dir: &str, map: &mut Map, domain: Domain) -> io::Result<Self> {
        let mut f = |name: &str| store.open(&format!("{dir}_{name}"));
        let mut s = Self {
            chunks_full: f("chunks_full.bin")?,
            chunks_partial: f("chunks_partial.bin")?,
            pairs: f("pairs.bin")?,
        };
        s.load(store, map, domain)?;
        Ok(s)
    }

    fn add(
        &mut self,
        store: &mut T,
        domain: Domain,
        data: &[u8],
        map: &mut Map,
    ) -> io::Result<Hash> {
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

    fn add_chunk(
        &mut self,
        store: &mut T,
        domain: Domain,
        chunk: &[u8],
        map: &mut Map,
    ) -> io::Result<Hash> {
        let key = toa_hash::hash_chunk(domain, chunk);
        if let Entry::Vacant(e) = map.entry(key) {
            e.insert(self.store_chunk(store, domain, chunk)?);
        }
        Ok(key)
    }

    fn add_pair(
        &mut self,
        store: &mut T,
        domain: Domain,
        x: &Hash,
        y: &Hash,
        len: u128,
        map: &mut Map,
    ) -> io::Result<Hash> {
        let key = toa_hash::hash_pair(*x, *y, len);
        if let Entry::Vacant(e) = map.entry(key) {
            e.insert(self.store_pair(store, domain, x, y, len)?);
        }
        Ok(key)
    }

    fn store_chunk(&mut self, store: &mut T, domain: Domain, bytes: &[u8]) -> io::Result<FileRef> {
        if let Ok(bytes) = bytes.try_into() {
            self.store_chunk_full(store, domain, bytes)
        } else {
            self.store_chunk_partial(store, domain, bytes)
        }
    }

    fn store_chunk_full(
        &mut self,
        store: &mut T,
        domain: Domain,
        bytes: &[u8; CHUNK_SIZE as usize],
    ) -> io::Result<FileRef> {
        let offt = store.append(&mut self.chunks_full, bytes)?;
        Ok(FileRef::new_chunk_full(domain, offt))
    }

    fn store_chunk_partial(
        &mut self,
        store: &mut T,
        domain: Domain,
        bytes: &[u8],
    ) -> io::Result<FileRef> {
        assert!(bytes.len() < CHUNK_SIZE as usize, "partial chunk too large");
        let hdr = u16::try_from(bytes.len() << 3)
            .expect("less than CHUNK_SIZE as usize bytes / 65536 bits");
        let pad = (!(2 + bytes.len()) + 1) & 7;
        let pad = &[0; 8][..pad];
        let offt =
            store.append_many(&mut self.chunks_partial, &[&hdr.to_le_bytes(), bytes, pad])?;
        Ok(FileRef::new_chunk_partial(domain, offt))
    }

    fn store_pair(
        &mut self,
        store: &mut T,
        domain: Domain,
        x: &Hash,
        y: &Hash,
        len: u128,
    ) -> io::Result<FileRef> {
        let mut buf = [0; 80];
        buf[00..32].copy_from_slice(x.as_bytes());
        buf[32..64].copy_from_slice(y.as_bytes());
        buf[64..].copy_from_slice(&len.to_le_bytes());
        let offt = store.append(&mut self.pairs, &buf)?;
        Ok(FileRef::new_pair(domain, offt))
    }

    fn load(&mut self, store: &T, map: &mut Map, domain: Domain) -> io::Result<()> {
        self.load_chunks_full(store, map, domain)?;
        self.load_chunks_partial(store, map, domain)?;
        self.load_pairs(store, map, domain)?;
        Ok(())
    }

    fn load_chunks_full(&mut self, store: &T, map: &mut Map, domain: Domain) -> io::Result<()> {
        let mut buf = vec![0; CHUNK_SIZE as usize];
        let mut offt = 0;
        while store.read_at_exact_or_none(&self.chunks_full, offt, &mut buf)? {
            let key = toa_hash::hash_chunk(domain, &buf);
            map.insert(key, FileRef::new_chunk_full(domain, offt));
            offt += buf.len() as u64;
        }
        Ok(())
    }

    fn load_chunks_partial(&mut self, store: &T, map: &mut Map, domain: Domain) -> io::Result<()> {
        let mut buf = vec![0; CHUNK_SIZE as usize];
        let len = &mut [0; 2];
        let mut offt = 0;
        while store.read_at_exact_or_none(&self.chunks_partial, offt, len)? {
            let len = u16::from_le_bytes(*len) >> 3;
            let buf = &mut buf[..usize::from(len)];
            store.read_at_exact(&self.chunks_partial, offt + 2, buf)?;
            let key = toa_hash::hash_chunk(domain, buf);
            map.insert(key, FileRef::new_chunk_partial(domain, offt));
            offt += align8(2 + u64::from(len));
        }
        Ok(())
    }

    fn load_pairs(&mut self, store: &T, map: &mut Map, domain: Domain) -> io::Result<()> {
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

impl<'a, T> Typed<'a, T>
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

impl<'a, T> Typed<'a, T>
where
    T: BlobStore,
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
                .store
                .read_at_array(&self.blobs.chunks_partial, self.location.offset())
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
        self.store
            .read_at(
                &self.blobs.chunks_full,
                self.location.offset() + offset as u64,
                buf,
            )
            .map_err(ReadError::Io)?;
        Ok(len)
    }

    fn read_chunk_partial(
        &self,
        offset: u128,
        buf: &mut [u8],
    ) -> Result<usize, ReadError<io::Error>> {
        let nb = self
            .store
            .read_at_array(&self.blobs.chunks_partial, self.location.offset())
            .map(u16::from_le_bytes)
            .map_err(ReadError::Io)?;
        let n = align8(nb) >> 3;
        let n = buf.len().min(u128::from(n).saturating_sub(offset) as usize);
        let buf = &mut buf[..n];
        if buf.is_empty() {
            return Ok(0);
        }
        self.store
            .read_at(
                &self.blobs.chunks_partial,
                self.location.offset() + 2 + offset as u64,
                buf,
            )
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
                    .store
                    .read_at_array(&self.blobs.chunks_partial, self.location.offset())
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

impl<T: BlobStore> Clone for Typed<'_, T> {
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
impl<T: BlobStore> Copy for Typed<'_, T> {}

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
                let len = file.metadata()?.len();
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

    fn open(&mut self, name: &str) -> io::Result<Self::BlobHandle> {
        self.open_or_create(name, true, false)
    }
    fn open_clear(&mut self, name: &str) -> io::Result<Self::BlobHandle> {
        self.open_or_create(name, true, true)
    }
    fn rename(&mut self, old_name: &str, new_name: &str) -> io::Result<()> {
        fs::rename(self.path(old_name), self.path(new_name))
    }
    fn append(&mut self, blob: &mut Self::BlobHandle, data: &[u8]) -> io::Result<u64> {
        blob.append(data)
    }
    fn append_many(&mut self, blob: &mut Self::BlobHandle, data: &[&[u8]]) -> io::Result<u64> {
        blob.append_many(data)
    }
    fn read_at(&self, blob: &Self::BlobHandle, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        blob.read_at(offset, buf)
    }
    fn size_on_disk(&self) -> io::Result<u64> {
        std::fs::read_dir(&self.0)?.try_fold(0, |s, x| Ok(s + x?.metadata()?.len()))
    }
}

impl<U> BlobStore for toa_blob::BlobStore<U>
where
    U: toa_blob::ZoneDev,
{
    type BlobHandle = std::rc::Rc<[u8]>;

    fn open(&mut self, name: &str) -> io::Result<Self::BlobHandle> {
        let name = std::rc::Rc::from(name.as_bytes());
        match self.create_blob(&name)? {
            Ok(x) => x,
            Err(_) => self.blob(&name)?.unwrap(),
        };
        Ok(name)
    }
    fn open_clear(&mut self, name: &str) -> io::Result<Self::BlobHandle> {
        let name = std::rc::Rc::from(name.as_bytes());
        if let Some(x) = self.blob(&name)? {
            x.delete()?;
        }
        self.create_blob(&name)?.unwrap();
        Ok(name)
    }
    fn rename(&mut self, old_name: &str, new_name: &str) -> io::Result<()> {
        self.blob(old_name.as_bytes())?
            .unwrap()
            .rename(new_name.as_bytes())
    }
    fn append(&mut self, blob: &mut Self::BlobHandle, data: &[u8]) -> io::Result<u64> {
        self.blob(blob)?.unwrap().append(data)
    }
    fn append_many(&mut self, blob: &mut Self::BlobHandle, data: &[&[u8]]) -> io::Result<u64> {
        self.blob(blob)?.unwrap().append_many(data)
    }
    fn read_at(&self, blob: &Self::BlobHandle, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.blob(blob)?.unwrap().read_at(offset, buf)
    }
    fn size_on_disk(&self) -> io::Result<u64> {
        self.size_on_disk()
    }
}

impl<T> BlobStore for &mut T
where
    T: BlobStore,
{
    type BlobHandle = T::BlobHandle;

    fn open(&mut self, name: &str) -> io::Result<Self::BlobHandle> {
        (**self).open(name)
    }
    fn open_clear(&mut self, name: &str) -> io::Result<Self::BlobHandle> {
        (**self).open_clear(name)
    }
    fn rename(&mut self, old_name: &str, new_name: &str) -> io::Result<()> {
        (**self).rename(old_name, new_name)
    }
    fn append(&mut self, blob: &mut Self::BlobHandle, data: &[u8]) -> io::Result<u64> {
        (**self).append(blob, data)
    }
    fn append_many(&mut self, blob: &mut Self::BlobHandle, data: &[&[u8]]) -> io::Result<u64> {
        (**self).append_many(blob, data)
    }
    fn read_at(&self, blob: &Self::BlobHandle, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        (**self).read_at(blob, offset, buf)
    }
    fn size_on_disk(&self) -> io::Result<u64> {
        (**self).size_on_disk()
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
        let toa = Toa::open(store).expect("toa init failed");
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
        let toa = Toa::open(store).expect("reload");
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
