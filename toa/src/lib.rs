#![forbid(unsafe_code, unused_must_use, mismatched_lifetime_syntaxes)]

pub use toa_core::{self as core, Hash};

use ::core::{fmt, mem, ops};
use std::{
    collections::btree_map::{BTreeMap, Entry},
    fs,
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
};
use toa_core::Domain;

const CHUNK_SIZE: u128 = 1 << 13;

pub struct Toa<T> {
    data: BlobsTyped<T>,
    refs: BlobsTyped<T>,
    map: Map,
    root: Hash,
    dir: Box<Path>,
}

pub struct Blob<T> {
    file: T,
    len: u64,
}

#[derive(Clone)]
pub enum Object<'a, T> {
    Data(Data<'a, T>),
    Refs(Refs<'a, T>),
}

#[derive(Clone)]
pub struct Data<'a, T>(Typed<'a, T>);
#[derive(Clone)]
pub struct Refs<'a, T>(Typed<'a, T>);

type Map = BTreeMap<Hash, FileRef>;

#[derive(Clone, Copy)]
struct Typed<'a, T> {
    blobs: &'a BlobsTyped<T>,
    map: &'a Map,
    location: FileRef,
}

struct BlobsTyped<T> {
    chunks_full: T,
    chunks_partial: T,
    pairs: T,
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

impl Toa<Blob<fs::File>> {
    pub fn open(path: &Path) -> io::Result<Self> {
        let dir = PathBuf::from(path).into_boxed_path();
        let mut map = Map::default();
        let f = |x| {
            let mut p = PathBuf::from(dir.clone());
            p.push(x);
            p
        };
        let data = BlobsTyped::open_at(&f("data"), &mut map, Domain::Data)?;
        let refs = BlobsTyped::open_at(&f("refs"), &mut map, Domain::Refs)?;
        let mut root = [0; 32];
        match fs::OpenOptions::new().read(true).open(&f("root.bin")) {
            Ok(mut x) => x.read_exact(&mut root)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let root = Hash::from_bytes(root);
        Ok(Self {
            data,
            refs,
            map,
            root,
            dir,
        })
    }

    pub fn contains_key(&self, key: &Hash) -> io::Result<bool> {
        Ok(self.map.contains_key(key))
    }

    pub fn get<'a>(&'a self, key: &Hash) -> io::Result<Option<Object<'a, Blob<fs::File>>>> {
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
        self.data.add(Domain::Data, data, &mut self.map)
    }

    pub fn add_refs(&mut self, refs: &[Hash]) -> io::Result<Hash> {
        self.refs
            .add(Domain::Refs, bytemuck::cast_slice(refs), &mut self.map)
    }

    pub fn size_on_disk(&self) -> u64 {
        self.data.size_on_disk() + self.refs.size_on_disk()
    }

    pub fn root(&self) -> Hash {
        self.root
    }

    pub fn set_root(&mut self, new_root: Hash) -> io::Result<()> {
        let (f, nf) = (self.file_path("root.bin"), self.file_path("new_root.bin"));
        fs::write(&nf, new_root.as_bytes())?;
        fs::rename(&nf, &f)?;
        self.root = new_root;
        Ok(())
    }

    fn file_path(&self, name: &str) -> PathBuf {
        let mut x = PathBuf::from(&*self.dir);
        x.push(name);
        x
    }
}

impl<T> Blob<T> {
    pub fn size_on_disk(&self) -> u64 {
        self.len
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
        let mut len = 0;
        for x in data {
            self.file.write_all(x)?;
            len += x.len() as u64;
        }
        let pad = 0u64.wrapping_sub(len) & 7;
        self.file.write_all(&[0; 8][..pad as usize])?;
        self.len += len + pad;
        debug_assert!(self.len % 8 == 0, "{}", self.len);
        Ok(o)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(&self.file, buf, offset)
    }
}

impl BlobsTyped<Blob<fs::File>> {
    fn open_at(path: &Path, map: &mut Map, domain: Domain) -> io::Result<Self> {
        let f = |name: &str| -> io::Result<_> {
            let mut path = PathBuf::from(path);
            path.push(name);
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .map(|file| Blob { file, len: 0 })
        };
        fs::create_dir_all(path)?;
        let mut s = Self {
            chunks_full: f("chunks_full.bin")?,
            chunks_partial: f("chunks_partial.bin")?,
            pairs: f("pairs.bin")?,
        };
        s.load(map, domain)?;
        Ok(s)
    }

    fn add(&mut self, domain: Domain, data: &[u8], map: &mut Map) -> io::Result<Hash> {
        if data.len() <= CHUNK_SIZE as usize {
            self.add_chunk(domain, data, map)
        } else {
            let mut stack = arrayvec::ArrayVec::<Hash, { 128 - 13 }>::new();
            let mut it = data.chunks_exact(CHUNK_SIZE as usize);
            for (i, y) in (&mut it).enumerate() {
                let mut y = self.add_chunk(domain, y, map)?;
                let mut len = 1 << 16;
                while stack.len() >= (i + 1).count_ones() as usize {
                    let x = stack.pop().expect("at least one element");
                    len <<= 1;
                    y = self.add_pair(domain, &x, &y, len, map)?;
                }
                stack.push(y);
            }

            let mut y = if !it.remainder().is_empty() {
                self.add_chunk(domain, it.remainder(), map)?
            } else {
                stack.pop().expect("at least one element")
            };
            let len = (data.len() as u128) * 8;
            let d = len.next_power_of_two().trailing_zeros();
            let d = d as usize - stack.len();
            let mut mask = !((1 << d) - 1);
            while let Some(x) = stack.pop() {
                mask <<= 1;
                y = self.add_pair(domain, &x, &y, len & !mask, map)?;
            }
            Ok(y)
        }
    }

    fn add_chunk(&mut self, domain: Domain, chunk: &[u8], map: &mut Map) -> io::Result<Hash> {
        let key = toa_core::hash_chunk(domain, chunk);
        if let Entry::Vacant(e) = map.entry(key) {
            e.insert(self.store_chunk(domain, chunk)?);
        }
        Ok(key)
    }

    fn add_pair(
        &mut self,
        domain: Domain,
        x: &Hash,
        y: &Hash,
        len: u128,
        map: &mut Map,
    ) -> io::Result<Hash> {
        let key = toa_core::hash_pair(*x, *y, len);
        if let Entry::Vacant(e) = map.entry(key) {
            e.insert(self.store_pair(domain, x, y, len)?);
        }
        Ok(key)
    }

    fn store_chunk(&mut self, domain: Domain, bytes: &[u8]) -> io::Result<FileRef> {
        if let Ok(bytes) = bytes.try_into() {
            self.store_chunk_full(domain, bytes)
        } else {
            self.store_chunk_partial(domain, bytes)
        }
    }

    fn store_chunk_full(
        &mut self,
        domain: Domain,
        bytes: &[u8; CHUNK_SIZE as usize],
    ) -> io::Result<FileRef> {
        let offt = self.chunks_full.append(bytes)?;
        Ok(FileRef::new_chunk_full(domain, offt))
    }

    fn store_chunk_partial(&mut self, domain: Domain, bytes: &[u8]) -> io::Result<FileRef> {
        assert!(bytes.len() < CHUNK_SIZE as usize, "partial chunk too large");
        let hdr = u16::try_from(bytes.len() << 3)
            .expect("less than CHUNK_SIZE as usize bytes / 65536 bits");
        let offt = self
            .chunks_partial
            .append_many(&[&hdr.to_le_bytes(), bytes])?;
        Ok(FileRef::new_chunk_partial(domain, offt))
    }

    fn store_pair(&mut self, domain: Domain, x: &Hash, y: &Hash, len: u128) -> io::Result<FileRef> {
        let mut buf = [0; 80];
        buf[00..32].copy_from_slice(x.as_bytes());
        buf[32..64].copy_from_slice(y.as_bytes());
        buf[64..].copy_from_slice(&len.to_le_bytes());
        let offt = self.pairs.append(&buf)?;
        Ok(FileRef::new_pair(domain, offt))
    }

    fn load(&mut self, map: &mut Map, domain: Domain) -> io::Result<()> {
        self.load_chunks_full(map, domain)?;
        self.load_chunks_partial(map, domain)?;
        self.load_pairs(map, domain)?;
        Ok(())
    }

    fn load_chunks_full(&mut self, map: &mut Map, domain: Domain) -> io::Result<()> {
        let mut buf = vec![0; CHUNK_SIZE as usize];
        while read_exact_or_none(&mut self.chunks_full.file, &mut buf)? {
            let key = toa_core::hash_chunk(domain, &buf);
            map.insert(key, FileRef::new_chunk_full(domain, self.chunks_full.len));
            self.chunks_full.len += buf.len() as u64;
        }
        Ok(())
    }

    fn load_chunks_partial(&mut self, map: &mut Map, domain: Domain) -> io::Result<()> {
        let mut reader = io::BufReader::new(&mut self.chunks_partial.file);
        let mut buf = vec![0; CHUNK_SIZE as usize];
        let mut len = &mut [0; 2];
        while read_exact_or_none(&mut reader, len)? {
            let len = u16::from_le_bytes(*len) >> 3;
            reader.read_exact(&mut buf[..usize::from(len)])?;
            let key = toa_core::hash_chunk(domain, &buf[..usize::from(len)]);
            map.insert(
                key,
                FileRef::new_chunk_partial(domain, self.chunks_partial.len),
            );
            let pad = -(2 + len as i64) & 7;
            reader.seek(io::SeekFrom::Current(pad))?;
            self.chunks_partial.len += align8(2 + u64::from(len));
        }
        Ok(())
    }

    fn load_pairs(&mut self, map: &mut Map, domain: Domain) -> io::Result<()> {
        let mut reader = io::BufReader::new(&mut self.pairs.file);
        let mut buf = [0; 80];
        while read_exact_or_none(&mut reader, &mut buf)? {
            let ([x, y], len) = bytes_to_pair(buf);
            let key = toa_core::hash_pair(x, y, len);
            map.insert(key, FileRef::new_pair(domain, self.pairs.len));
            self.pairs.len += buf.len() as u64;
        }
        Ok(())
    }

    pub fn size_on_disk(&self) -> u64 {
        self.chunks_full.size_on_disk()
            + self.chunks_partial.size_on_disk()
            + self.pairs.size_on_disk()
    }
}

impl FileRef {
    const TY_CHUNK_FULL: u64 = 2;
    const TY_CHUNK_PARTIAL: u64 = 4;
    const TY_PAIR: u64 = 6;

    const TY_DATA: u64 = 0;
    const TY_REFS: u64 = 1;

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

impl<'a, T> Typed<'a, T> {
    fn new(toa: &'a Toa<T>, key: Hash) -> Option<Self> {
        let location = *toa.map.get(&key)?;
        let blobs = match location.ty().1 {
            Domain::Data => &toa.data,
            Domain::Refs => &toa.refs,
        };
        Some(Self {
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

impl<'a> Typed<'a, Blob<fs::File>> {
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
            FileRef::TY_CHUNK_PARTIAL => {
                let len = &mut [0; 2];
                self.blobs
                    .chunks_partial
                    .read_at(self.location.offset(), len)?;
                Ok(u128::from(u16::from_le_bytes(*len)))
            }
            FileRef::TY_PAIR => {
                let len = &mut [0; 16];
                self.blobs.pairs.read_at(self.location.offset() + 64, len)?;
                Ok(u128::from_le_bytes(*len))
            }
            _ => unreachable!("invalid FileRef type"),
        }
    }

    fn read_pair(&self, offset: u128, buf: &mut [u8]) -> Result<usize, ReadError<io::Error>> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut pair = [0; 80];
        self.blobs
            .pairs
            .read_at(self.location.offset(), &mut pair)
            .map_err(ReadError::Io)?;
        let ([x, y], len) = bytes_to_pair(pair);

        let len = align8(len) >> 3;
        if offset >= len {
            return Ok(0);
        }

        let x = self.with_key(x).unwrap();
        let y = self.with_key(y).unwrap();
        let xl = len.next_power_of_two() >> 1;
        let yl = len - xl;
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
        let offt = (offset % CHUNK_SIZE) as u64;
        self.blobs
            .chunks_full
            .read_at(self.location.offset() + offt, buf)
            .map_err(ReadError::Io)?;
        Ok(len)
    }

    fn read_chunk_partial(
        &self,
        offset: u128,
        buf: &mut [u8],
    ) -> Result<usize, ReadError<io::Error>> {
        let nb = &mut [0; 2];
        self.blobs
            .chunks_partial
            .read_at(self.location.offset(), nb)
            .map_err(ReadError::Io)?;
        let n = usize::from(u16::from_le_bytes(*nb));
        let n = (align8(n) >> 3).min(buf.len());
        let buf = &mut buf[..n];
        self.blobs
            .chunks_partial
            .read_at(self.location.offset() + 2, buf)
            .map_err(ReadError::Io)?;
        Ok(n)
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

impl fmt::Debug for FileRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (ty, domain) = self.ty();
        write!(f, "{ty:?}:{domain:?}:{}", self.offset())
    }
}

fn align8<T>(x: T) -> T
where
    T: ops::Add<Output = T> + ops::Not<Output = T> + ops::BitAnd<Output = T> + From<u8>,
{
    (x + T::from(7)) & !T::from(7)
}

fn read_exact_or_none<T>(io: &mut T, buf: &mut [u8]) -> io::Result<bool>
where
    T: Read,
{
    match io.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => return Err(e),
    }
}

fn advance8(x: &mut u64, y: u64) -> u64 {
    let y = align8(y);
    let z = *x;
    *x += y;
    z
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
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

    type Toa = super::Toa<Blob<fs::File>>;

    struct Test {
        toa: Toa,
        tempdir: tempfile::TempDir,
    }

    impl Test {
        fn add(&mut self, data: &[u8]) -> Hash {
            let key = self.toa.add_data(data).expect("add_data failed");
            assert_eq!(key, toa_core::hash(Domain::Data, data));
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
            assert_eq!(o.0.len_bits().unwrap(), (value.len() as u128) << 3);
            let x = &mut *vec![0; value.len()];
            let n = o.0.read(0, x).expect("read failed");
            assert_eq!(n, value.len());
            let f = String::from_utf8_lossy;
            assert!(x == value, "{:?} <> {:?}", f(&x), f(value));
        }
    }

    fn init() -> Test {
        let tempdir = tempfile::tempdir().expect("failed to create tempdir");
        let toa = Toa::open(tempdir.path()).expect("toa init failed");
        Test { toa, tempdir }
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
        let Test { toa, tempdir } = s;
        dbg!(&toa.map);
        let _ = toa;
        let toa = Toa::open(tempdir.path()).expect("reload");
        dbg!(&toa.map);
        let s = Test { toa, tempdir };
        s.assert_eq(&a, b"Hello, world!");
        s.assert_eq(&b, b"Hello, planet!");
        s.assert_eq(&c, &vec![b'x'; 1 << 15]);
    }
}
