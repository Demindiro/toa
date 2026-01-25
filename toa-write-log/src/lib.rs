#![cfg_attr(not(test), no_std)]
#![forbid(unused_must_use)]

extern crate alloc;

pub use toa_core::{CompressionAlgorithm, Hash, UnknownCompressionAlgorithm};

use alloc::collections::BTreeMap;
use arrayvec::ArrayVec;
use blake3::hazmat::{ChainingValue, HasherExt};
use core::convert::Infallible;

pub const CHUNK_SIZE: usize = 1 << 16;
pub const ENTRY_SIZE: usize = 64;

const CHUNK_SIZE_64: u64 = CHUNK_SIZE as u64;

pub trait Store {
    type Error;
    type Write<'a>: StoreWrite<Error = Self::Error>
    where
        Self: 'a;
    type Read<'a>: AsRef<[u8]>
    where
        Self: 'a;

    fn write_chunk<'a>(&'a mut self, max_len: usize) -> Result<Self::Write<'a>, Self::Error>;
    fn write_entry(&mut self, data: &[u8; ENTRY_SIZE]) -> Result<(), Self::Error>;
    fn read_chunk<'a>(&'a mut self, offset: u64, len: usize)
    -> Result<Self::Read<'a>, Self::Error>;
    fn read_entry(&mut self, index: u32) -> Result<[u8; ENTRY_SIZE], Self::Error>;
    fn len(&mut self) -> Result<u32, Self::Error>;
}

pub trait StoreWrite {
    type Error;

    fn buf(&mut self) -> &mut [u8];
    fn finish(self, len: usize) -> Result<u64, Self::Error>;
}

pub struct WriteLog<S> {
    store: S,
    len: u32,
    // TODO we should use a custom hashmap with an implicit key
    lut: BTreeMap<Hash, u32>,
}

#[derive(Clone, Debug)]
pub enum GetError<A> {
    Store(A),
    UnknownCompressionAlgorithm,
}

#[derive(Clone, Debug)]
pub enum ReadError<A> {
    Store(A),
    CorruptedCompression,
}

#[derive(Clone, Debug)]
pub enum AddStreamError<A, B> {
    Store(A),
    Stream(B),
}

#[derive(Debug)]
pub struct Entry {
    object_len: u64,
    offset: u64,
    compressed_len: u32,
    uncompressed_len: u32,
    compression: CompressionAlgorithm,
}

pub enum Read<'a> {
    Parent(&'a [Hash; 2]),
    Leaf { len: usize },
}

#[cfg(any(feature = "alloc", test))]
#[derive(Default)]
pub struct MemStore {
    pub chunks: alloc::vec::Vec<u8>,
    pub entries: alloc::vec::Vec<[u8; ENTRY_SIZE]>,
}

#[cfg(any(feature = "alloc", test))]
pub struct MemStoreWrite<'a> {
    buf: &'a mut alloc::vec::Vec<u8>,
    offset: usize,
}

#[cfg(any(feature = "std", test))]
pub struct FileStore {
    pub chunks: std::fs::File,
    pub entries: std::fs::File,
    pub buffer: Vec<u8>,
}

#[cfg(any(feature = "std", test))]
pub struct FileStoreWrite<'a> {
    store: &'a mut FileStore,
}

impl<S: Store> WriteLog<S> {
    pub fn load<'a>(mut store: S) -> Result<Self, S::Error> {
        let mut lut = BTreeMap::new();
        let len = store.len()?;
        for i in 0..len {
            let buf = store.read_entry(i)?;
            let key = buf[..32].try_into().expect("32 bytes");
            lut.insert(Hash(key), i);
        }
        Ok(Self { store, len, lut })
    }

    pub fn get<'a>(&mut self, key: &Hash) -> Result<Option<Entry>, GetError<S::Error>> {
        let Some(&index) = self.lut.get(key) else {
            return Ok(None);
        };
        let buf = self.store.read_entry(index).map_err(GetError::Store)?;
        let entry = buf[32..].try_into().expect("32 bytes");
        let entry = Entry::from_bytes(entry).map_err(|_| GetError::UnknownCompressionAlgorithm)?;
        Ok(Some(entry))
    }

    pub fn read<'a>(
        &mut self,
        entry: &Entry,
        buf: &'a mut [u8; CHUNK_SIZE],
    ) -> Result<Read<'a>, ReadError<S::Error>> {
        let clen = usize::try_from(entry.compressed_len).expect("u32 <= usize");
        let uclen = usize::try_from(entry.uncompressed_len).expect("u32 <= usize");
        let data = self
            .store
            .read_chunk(entry.offset, clen)
            .map_err(ReadError::Store)?;
        let buf = &mut buf[..uclen];
        toa_core::decompress(data.as_ref(), buf, entry.compression)
            .map_err(|_| ReadError::CorruptedCompression)?;
        if entry.object_len > CHUNK_SIZE_64 {
            assert!(
                buf.len() == core::mem::size_of::<[Hash; 2]>(),
                "parent node must be exactly 64 bytes"
            );
            // SAFETY: Hash has an alignment of 1 and the buffer fits
            let parent = unsafe { &*buf.as_ptr().cast::<[Hash; 2]>() };
            Ok(Read::Parent(parent))
        } else {
            Ok(Read::Leaf { len: uclen })
        }
    }

    pub fn add(&mut self, data: &[u8]) -> Result<Hash, S::Error> {
        let len = u64::try_from(data.len()).expect("usize <= u64");
        let mut it = data.chunks(CHUNK_SIZE);
        // there is a special case where data == [], i.e. empty
        let it = || Ok::<_, Infallible>(it.next().unwrap_or(&[]));
        match self.add_stream(len, it) {
            Ok(x) => Ok(x),
            Err(AddStreamError::Store(x)) => Err(x),
        }
    }

    /// Every item **must** return a slice of [`CHUNK_SIZE`] bytes until EOS,
    /// except for the last chunk.
    pub fn add_stream<F, E, B>(
        &mut self,
        len: u64,
        mut data: F,
    ) -> Result<Hash, AddStreamError<S::Error, E>>
    where
        F: FnMut() -> Result<B, E>,
        B: AsRef<[u8]>,
    {
        if len <= CHUNK_SIZE_64 {
            let b = (data)().map_err(AddStreamError::Stream)?;
            let key = Hash(*blake3::hash(b.as_ref()).as_bytes());
            self.add_chunk(&key, len, b.as_ref())
                .map_err(AddStreamError::Store)?;
            return Ok(key);
        }
        // +1 as we lazily derive root
        let mut stack = ArrayVec::<ChainingValue, { 64 - 10 + 1 }>::new();
        for i in 0..(len + CHUNK_SIZE_64 - 1) / CHUNK_SIZE_64 {
            let byte_offset = i * CHUNK_SIZE_64;
            let b = (data)().map_err(AddStreamError::Stream)?;
            let cv = blake3::Hasher::new()
                .set_input_offset(byte_offset)
                .update(b.as_ref())
                .finalize_non_root();
            let obj_len = (len - byte_offset).min(CHUNK_SIZE_64);
            self.add_chunk(&Hash(cv), obj_len, b.as_ref())
                .map_err(AddStreamError::Store)?;
            self.merge(&mut stack, i.count_ones() as usize, len)
                .map_err(AddStreamError::Store)?;
            stack.push(cv);
        }
        self.merge(&mut stack, 2, len)
            .map_err(AddStreamError::Store)?;
        let [y, x] = [stack.pop(), stack.pop()];
        let xy @ [x, y] = x.and_then(|x| y.map(|y| [x, y])).expect("at least 2 CV");
        assert!(stack.is_empty(), "all CVs popped");
        let z = blake3::hazmat::merge_subtrees_root(&x, &y, blake3::hazmat::Mode::Hash);
        let xy = xy.as_flattened();
        self.add_chunk(&Hash(*z.as_bytes()), len, xy)
            .map_err(AddStreamError::Store)?;
        Ok(Hash(*z.as_bytes()))
    }

    fn merge(
        &mut self,
        stack: &mut ArrayVec<ChainingValue, { 64 - 10 + 1 }>,
        max: usize,
        len: u64,
    ) -> Result<(), S::Error> {
        let mut obj_len = CHUNK_SIZE_64;
        while stack.len() > max {
            obj_len <<= 1;
            self.merge_one(stack, obj_len.min(len))?;
        }
        Ok(())
    }

    fn merge_one(
        &mut self,
        stack: &mut ArrayVec<ChainingValue, { 64 - 10 + 1 }>,
        obj_len: u64,
    ) -> Result<(), S::Error> {
        // derived using brain + "borrowing" from blake3::Hasher::merge_cv_stacks
        let [y, x] = [stack.pop(), stack.pop()];
        let xy @ [x, y] = x.and_then(|x| y.map(|y| [x, y])).expect("at least 2 CV");
        let z = blake3::hazmat::merge_subtrees_non_root(&x, &y, blake3::hazmat::Mode::Hash);
        let xy = xy.as_flattened();
        self.add_chunk(&Hash(z), obj_len, xy)?;
        stack.push(z);
        Ok(())
    }

    fn add_chunk(&mut self, key: &Hash, object_len: u64, data: &[u8]) -> Result<(), S::Error> {
        let len = data.len();
        assert!(len <= CHUNK_SIZE, "chunk too large");
        let compressed_len @ uncompressed_len = u32::try_from(len).expect("chunk too large");
        let mut wr = self.store.write_chunk(len)?;
        wr.buf().copy_from_slice(data);
        let offset = wr.finish(len)?;
        let entry = Entry {
            object_len,
            offset,
            compressed_len,
            uncompressed_len,
            compression: CompressionAlgorithm::None,
        };
        let mut b = [0; 64];
        b[..32].copy_from_slice(&key.0);
        b[32..].copy_from_slice(&entry.into_bytes());
        self.store.write_entry(&b)?;
        self.lut.insert(*key, self.len);
        self.len += 1;
        Ok(())
    }

    pub fn into_store(self) -> S {
        self.store
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }
}

impl Entry {
    fn into_bytes(self) -> [u8; 32] {
        assert!(self.compressed_len < 1 << 24);
        assert!(self.uncompressed_len < 1 << 24);
        let mut b = [0; 32];
        b[8..16].copy_from_slice(&self.object_len.to_le_bytes());
        b[16..24].copy_from_slice(&self.offset.to_le_bytes());
        b[24] = self.compression as u8;
        b[25..28].copy_from_slice(&self.compressed_len.to_le_bytes()[..3]);
        b[29..32].copy_from_slice(&self.uncompressed_len.to_le_bytes()[..3]);
        b
    }

    fn from_bytes(x: [u8; 32]) -> Result<Self, UnknownCompressionAlgorithm> {
        let [_, _, _, _, _, _, _, _, x @ ..] = x;
        let [a, b, c, d, e, f, g, h, x @ ..] = x;
        let object_len = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let [a, b, c, d, e, f, g, h, x @ ..] = x;
        let offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let [a, b, c, d, _, f, g, h] = x;
        let compression = CompressionAlgorithm::try_from(a)?;
        let compressed_len = u32::from_le_bytes([b, c, d, 0]);
        let uncompressed_len = u32::from_le_bytes([f, g, h, 0]);
        Ok(Self {
            object_len,
            offset,
            compressed_len,
            uncompressed_len,
            compression,
        })
    }
}

#[cfg(any(feature = "alloc", test))]
impl Store for MemStore {
    type Error = Infallible;
    type Write<'a>
        = MemStoreWrite<'a>
    where
        Self: 'a;
    type Read<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn write_chunk<'a>(&'a mut self, max_len: usize) -> Result<Self::Write<'a>, Self::Error> {
        // TODO can technically fail if max_len is some stupid value
        let offset = self.chunks.len();
        self.chunks.resize(offset + max_len, 0);
        Ok(MemStoreWrite {
            buf: &mut self.chunks,
            offset,
        })
    }

    fn write_entry(&mut self, data: &[u8; ENTRY_SIZE]) -> Result<(), Self::Error> {
        self.entries.push(*data);
        Ok(())
    }

    fn read_chunk<'a>(
        &'a mut self,
        offset: u64,
        len: usize,
    ) -> Result<Self::Read<'a>, Self::Error> {
        // TODO can also fail...
        let start = usize::try_from(offset).ok().unwrap();
        let end = start.checked_add(len).unwrap();
        let x = self.chunks.get(start..end).unwrap();
        Ok(x)
    }

    fn read_entry(&mut self, index: u32) -> Result<[u8; ENTRY_SIZE], Self::Error> {
        // TODO ditto
        let i = usize::try_from(index).unwrap();
        let x = self.entries.get(i).unwrap();
        Ok(*x)
    }

    fn len(&mut self) -> Result<u32, Self::Error> {
        Ok(u32::try_from(self.entries.len()).unwrap())
    }
}

#[cfg(any(feature = "alloc", test))]
impl<'a> StoreWrite for MemStoreWrite<'a> {
    type Error = Infallible;

    fn buf(&mut self) -> &mut [u8] {
        &mut self.buf[self.offset..]
    }

    fn finish(self, len: usize) -> Result<u64, Self::Error> {
        self.buf.resize_with(self.offset.saturating_add(len), || {
            panic!("len exceeds capacity")
        });
        Ok(u64::try_from(self.offset).expect("usize <= u64"))
    }
}

#[cfg(any(feature = "std", test))]
impl Store for FileStore {
    type Error = std::io::Error;
    type Write<'a>
        = FileStoreWrite<'a>
    where
        Self: 'a;
    type Read<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn write_chunk<'a>(&'a mut self, max_len: usize) -> Result<Self::Write<'a>, Self::Error> {
        self.buffer.resize(max_len, 0);
        Ok(FileStoreWrite { store: self })
    }

    fn write_entry(&mut self, data: &[u8; ENTRY_SIZE]) -> Result<(), Self::Error> {
        use std::io::{self, Seek, Write};
        self.entries.seek(io::SeekFrom::End(0))?;
        self.entries.write_all(data)
    }

    fn read_chunk<'a>(
        &'a mut self,
        offset: u64,
        len: usize,
    ) -> Result<Self::Read<'a>, Self::Error> {
        use std::io::{self, Read, Seek};
        self.buffer.resize(len, 0);
        self.chunks.seek(io::SeekFrom::Start(offset))?;
        self.chunks.read_exact(&mut self.buffer)?;
        Ok(&self.buffer)
    }

    fn read_entry(&mut self, index: u32) -> Result<[u8; ENTRY_SIZE], Self::Error> {
        use std::io::{self, Read, Seek};
        let mut buf = [0; 64];
        self.entries
            .seek(io::SeekFrom::Start(u64::from(index) * ENTRY_SIZE as u64))?;
        self.entries.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn len(&mut self) -> Result<u32, Self::Error> {
        use std::io::{self, Seek};
        let pos = self.entries.seek(io::SeekFrom::End(0))?;
        u32::try_from(pos / 64)
            .map_err(|_| io::Error::new(io::ErrorKind::FileTooLarge, "too many entries"))
    }
}

#[cfg(any(feature = "std", test))]
impl<'a> StoreWrite for FileStoreWrite<'a> {
    type Error = std::io::Error;

    fn buf(&mut self) -> &mut [u8] {
        &mut self.store.buffer
    }

    fn finish(self, len: usize) -> Result<u64, Self::Error> {
        use std::io::{self, Seek, Write};
        self.store
            .buffer
            .resize_with(len, || panic!("len exceeds capacity"));
        let pos = self.store.chunks.seek(io::SeekFrom::End(0))?;
        self.store.chunks.write_all(&self.store.buffer)?;
        Ok(pos)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct Test<S> {
        log: WriteLog<S>,
        buf: Box<[u8; CHUNK_SIZE]>,
    }

    impl<S> Test<S>
    where
        S: Store,
        S::Error: core::fmt::Debug,
    {
        fn new(store: S) -> Self {
            let log = WriteLog::load(store).expect("load");
            let buf = Box::new([0; CHUNK_SIZE]);
            Self { log, buf }
        }

        fn add(&mut self, data: &[u8]) -> Hash {
            let expect = Hash(*blake3::hash(data).as_bytes());
            let key = self.log.add(data).expect("add");
            assert_eq!(expect, key);
            key
        }

        fn assert_eq(&mut self, key: &Hash, data: &[u8]) {
            let entry = self.log.get(key).expect("get").expect("present");
            match self.log.read(&entry, &mut self.buf).expect("read") {
                Read::Parent(&[kl, kr]) => {
                    let split = (data.len() / 2).next_power_of_two();
                    let split = split.max(CHUNK_SIZE);
                    let (dl, dr) = data.split_at(split);
                    self.assert_eq(&kl, dl);
                    self.assert_eq(&kr, dr);
                }
                // don't use assert_eq to avoid flooding output
                Read::Leaf { len } => assert!(&self.buf[..len] == data, "{len} <> {}", data.len()),
            }
        }

        fn reload(self) -> Self {
            let Self { log, buf } = self;
            let log = log.into_store();
            let log = WriteLog::load(log).expect("reload");
            Test { log, buf }
        }
    }

    fn init() -> Test<MemStore> {
        Test::new(MemStore::default())
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
    fn insert_chunk_plus_one() {
        let mut s = init();
        let v = vec![0; CHUNK_SIZE + 1];
        let k = s.add(&v);
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_3_chunk() {
        let mut s = init();
        let v = vec![0; 3 * CHUNK_SIZE];
        let k = s.add(&v);
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_4_chunk() {
        let mut s = init();
        let v = vec![0; 4 * CHUNK_SIZE];
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
        let f = |x| format!("A number {x}").into_bytes();
        let keys = (0..1 << 12).map(|i| s.add(&f(i))).collect::<Vec<_>>();
        let mut s = s.reload();
        keys.iter()
            .enumerate()
            .for_each(|(i, k)| s.assert_eq(k, &f(i)));
    }

    #[test]
    fn file_store() {
        let mut s = Test::new(FileStore {
            chunks: tempfile::tempfile().expect("tempfile chunks"),
            entries: tempfile::tempfile().expect("tempfile entries"),
            buffer: Default::default(),
        });
        let f = |x| format!("A number {x}").into_bytes();
        let keys = (0..1 << 12).map(|i| s.add(&f(i))).collect::<Vec<_>>();
        let mut s = s.reload();
        keys.iter()
            .enumerate()
            .for_each(|(i, k)| s.assert_eq(k, &f(i)));
    }
}
