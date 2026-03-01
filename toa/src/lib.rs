#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![forbid(unsafe_code, unused_must_use, mismatched_lifetime_syntaxes)]

pub use toa_core::{self as core, Hash};

use ::core::mem;
use toa_core::{DataCv, RefsCv};

const NAMESPACE_ROOTS: u8 = 1;
const NAMESPACE_PAIRS: u8 = 2;
const NAMESPACE_CHUNKS: u8 = 3;

const CHUNK_SIZE: u128 = 1 << 13;

pub trait ToaStore {
    type Error;
    type Chunk<'a>: AsRef<[u8]>
    where
        Self: 'a;

    fn add_chunk(&self, key: &[u8; 32], value: &[u8]) -> Result<(), Self::Error>;
    fn add_pair(&self, key: &[u8; 32], value: &[u8; 64]) -> Result<(), Self::Error>;
    fn add_root(&self, key: &[u8; 32], value: &[u8; 96]) -> Result<(), Self::Error>;

    fn get_chunk<'a>(&'a self, key: &[u8; 32]) -> Result<Option<Self::Chunk<'a>>, Self::Error>;
    fn get_pair(&self, key: &[u8; 32]) -> Result<Option<[u8; 64]>, Self::Error>;
    fn get_root(&self, key: &[u8; 32]) -> Result<Option<[u8; 96]>, Self::Error>;

    fn iter_roots_with(&self, f: &mut dyn FnMut(&[u8; 32])) -> Result<(), Self::Error>;

    fn size_on_disk(&self) -> Result<u128, Self::Error>;

    fn has_chunk(&self, key: &[u8; 32]) -> Result<bool, Self::Error> {
        self.get_chunk(key).map(|x| x.is_some())
    }
    fn has_pair(&self, key: &[u8; 32]) -> Result<bool, Self::Error> {
        self.get_pair(key).map(|x| x.is_some())
    }
    fn has_root(&self, key: &[u8; 32]) -> Result<bool, Self::Error> {
        self.get_root(key).map(|x| x.is_some())
    }
}

pub struct Toa<S> {
    store: S,
}

#[derive(Clone, Debug, Default)]
pub struct ToaKvStore<T>(pub T);

#[derive(Clone, Debug)]
pub enum ToaKvStoreError<T> {
    InvalidPair,
    InvalidRoot,
    Kv(T),
}

#[derive(Clone)]
pub struct Object<S> {
    toa: S,
    root: Root,
}

#[derive(Debug)]
pub enum ReadError<S> {
    MissingChunk,
    MissingPair,
    Store(S),
}

#[derive(Debug)]
pub enum ReadExactError<S> {
    MissingChunk,
    MissingPair,
    Truncated,
    Store(S),
}

#[derive(Clone, Copy, bytemuck::Zeroable, bytemuck::Pod)]
#[repr(C)]
struct Root {
    data: DataCv,
    refs: RefsCv,
    data_len: u128,
    refs_len: u128,
}

impl<S> Toa<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }
}

macro_rules! impl_add {
    ($cv:ident $add:ident $add_pair:ident $add_chunk:ident $arg:ident $elem:ident $toa_pair:ident $toa_chunk:ident) => {
        fn $add(&self, $arg: &[$elem]) -> Result<$cv, S::Error> {
            if $arg.len() <= CHUNK_SIZE as usize / mem::size_of::<$elem>() {
                self.$add_chunk($arg)
            } else {
                let mut stack = arrayvec::ArrayVec::<$cv, { 128 - 13 }>::new();
                let mut it = $arg.chunks_exact(CHUNK_SIZE as usize / mem::size_of::<$elem>());
                for (i, y) in (&mut it).enumerate() {
                    let mut y = self.$add_chunk(y)?;
                    let mut len = 1 << 16;
                    while stack.len() >= (i + 1).count_ones() as usize {
                        let x = stack.pop().expect("at least one element");
                        len <<= 1;
                        y = self.$add_pair(&x, &y, len)?;
                    }
                    stack.push(y);
                }

                let mut y = if !it.remainder().is_empty() {
                    self.$add_chunk(it.remainder())?
                } else {
                    stack.pop().expect("at least one element")
                };
                let len = ($arg.len() as u128) * 8;
                let d = len.next_power_of_two().trailing_zeros();
                let d = d as usize - stack.len();
                let mut mask = !((1 << d) - 1);
                while let Some(x) = stack.pop() {
                    mask <<= 1;
                    y = self.$add_pair(&x, &y, len & !mask)?;
                }
                Ok(y)
            }
        }

        fn $add_pair(&self, x: &$cv, y: &$cv, len: u128) -> Result<$cv, S::Error> {
            let cv = toa_core::$toa_pair(*x, *y, len);
            let xy = [*x.as_bytes(), *y.as_bytes()];
            self.store.add_pair(cv.as_bytes(), &bytemuck::cast(xy))?;
            Ok(cv)
        }
    };
}

impl<S> Toa<S>
where
    S: ToaStore,
{
    pub fn add(&self, data: &[u8], refs: &[Hash]) -> Result<Hash, S::Error> {
        let root = Root {
            data: self.add_data(data)?,
            refs: self.add_refs(refs)?,
            data_len: (data.len() as u128) << 3,
            refs_len: (refs.len() as u128) << 8,
        };
        let hash = toa_core::root_hash(root.data, root.refs);
        self.store
            .add_root(hash.as_bytes(), bytemuck::cast_ref(&root))?;
        Ok(hash)
    }

    pub fn contains_key(&self, key: &Hash) -> Result<bool, S::Error> {
        self.store.has_root(key.as_bytes())
    }

    pub fn get<'a>(&'a self, key: &Hash) -> Result<Option<Object<&'a Self>>, S::Error> {
        let root = self.store.get_root(key.as_bytes())?;
        let Some(root) = root else {
            return Ok(None);
        };
        let root = bytemuck::cast(root);
        Ok(Some(Object { toa: self, root }))
    }

    pub fn iter_with<F>(&self, mut f: F) -> Result<(), S::Error>
    where
        F: FnMut(Hash) -> bool,
    {
        self.store.iter_roots_with(&mut |x| {
            (f)(Hash::from_bytes(*x));
        })
    }

    impl_add!(DataCv add_data add_data_pair add_data_chunk data u8   data_pair_cv data_chunk_cv);
    impl_add!(RefsCv add_refs add_refs_pair add_refs_chunk refs Hash refs_pair_cv refs_chunk_cv);

    fn add_data_chunk(&self, chunk: &[u8]) -> Result<DataCv, S::Error> {
        let cv = toa_core::data_chunk_cv(chunk);
        self.store.add_chunk(cv.as_bytes(), chunk)?;
        Ok(cv)
    }

    fn add_refs_chunk(&self, chunk: &[Hash]) -> Result<RefsCv, S::Error> {
        let refs = Hash::slice_as_bytes(chunk).as_flattened();
        let cv = toa_core::refs_chunk_cv(chunk);
        self.store.add_chunk(cv.as_bytes(), refs)?;
        Ok(cv)
    }
}

impl<'t, S> Object<&'t Toa<S>>
where
    S: ToaStore,
{
    /// Size of data blob **in bits**.
    pub fn data_len(&self) -> u128 {
        self.root.data_len
    }

    /// Size of references blob **in bits**.
    pub fn refs_len(&self) -> u128 {
        self.root.refs_len
    }

    pub fn data(&self) -> Data<'t, S> {
        Data::new(self.toa, self.root.data, (self.root.data_len + 7) >> 3)
    }

    pub fn refs(&self) -> Refs<'t, S> {
        Refs::new(self.toa, self.root.refs, (self.root.refs_len + 255) >> 8)
    }
}

macro_rules! impl_cv {
    ($cv:ident $root:ident $pair:ident $chunk:ident $ty:ident $docname:literal) => {
        pub enum $root<'a, S> {
            Pair($pair<'a, S>),
            Chunk($chunk<'a, S>),
        }

        pub struct $pair<'a, S> {
            toa: &'a Toa<S>,
            root: $cv,
            len: u128,
        }

        pub struct $chunk<'a, S> {
            toa: &'a Toa<S>,
            root: $cv,
        }

        impl<'t, S> $root<'t, S>
        where
            S: ToaStore,
        {
            pub fn read(
                &self,
                offset: u128,
                buf: &mut [$ty],
            ) -> Result<usize, ReadError<S::Error>> {
                match self {
                    Self::Pair(x) => x.read(offset, buf),
                    Self::Chunk(x) => x.read(offset, buf),
                }
            }

            pub fn read_exact(
                &self,
                offset: u128,
                buf: &mut [$ty],
            ) -> Result<(), ReadExactError<S::Error>> {
                if self.len().saturating_sub(offset) < buf.len() as u128 {
                    return Err(ReadExactError::Truncated);
                }
                self.read(offset, buf).map(|_| ()).map_err(|x| x.into())
            }

            pub fn read_array<const N: usize>(
                &self,
                offset: u128,
            ) -> Result<[$ty; N], ReadExactError<S::Error>> {
                let mut buf = [<$ty>::default(); N];
                self.read_exact(offset, &mut buf)?;
                Ok(buf)
            }

            #[doc = "Size of data blob **in "]
            #[doc = $docname]
            #[doc = "** (rounded up)."]
            pub fn len(&self) -> u128 {
                match self {
                    Self::Chunk(x) => x.len(),
                    Self::Pair(x) => x.len(),
                }
            }

            fn new(toa: &'t Toa<S>, root: $cv, len: u128) -> Self {
                if len <= (CHUNK_SIZE / mem::size_of::<$ty>() as u128) {
                    Self::Chunk($chunk { toa, root })
                } else {
                    Self::Pair($pair { toa, root, len })
                }
            }
        }

        impl<'t, S> $pair<'t, S>
        where
            S: ToaStore,
        {
            pub fn read(
                &self,
                offset: u128,
                buf: &mut [$ty],
            ) -> Result<usize, ReadError<S::Error>> {
                if buf.is_empty() || offset >= self.len {
                    return Ok(0);
                }
                let x = self
                    .toa
                    .store
                    .get_pair(self.root.as_bytes())
                    .map_err(ReadError::Store)?
                    .ok_or(ReadError::MissingPair)?;
                let y = $cv::from_bytes(x[32..].try_into().unwrap());
                let x = $cv::from_bytes(x[..32].try_into().unwrap());
                let xl = self.len.next_power_of_two() >> 1;
                let yl = self.len - xl;
                let x = $root::new(self.toa, x, xl);
                let y = $root::new(self.toa, y, yl);
                let n = xl.saturating_sub(offset).min(buf.len() as u128) as usize;
                let (xb, yb) = buf.split_at_mut(n);
                Ok(x.read(offset, xb)? + y.read(offset.saturating_sub(xl), yb)?)
            }

            fn len(&self) -> u128 {
                self.len
            }
        }

        impl<'t, S> $chunk<'t, S>
        where
            S: ToaStore,
        {
            pub fn read(
                &self,
                offset: u128,
                buf: &mut [$ty],
            ) -> Result<usize, ReadError<S::Error>> {
                let offset = offset * mem::size_of::<$ty>() as u128;
                if offset >= CHUNK_SIZE {
                    return Ok(0);
                }
                let buf = bytemuck::cast_slice_mut(buf);
                let x = self
                    .toa
                    .store
                    .get_chunk(self.root.as_bytes())
                    .map_err(ReadError::Store)?
                    .ok_or(ReadError::MissingChunk)?;
                let x = &x.as_ref()[offset as usize..];
                let n = x.len().min(buf.len());
                buf[..n].copy_from_slice(&x[..n]);
                Ok(n / mem::size_of::<$ty>())
            }

            fn len(&self) -> u128 {
                self.toa
                    .store
                    .get_chunk(self.root.as_bytes())
                    .unwrap_or(None)
                    .map_or(0, |x| (x.as_ref().len() / mem::size_of::<$ty>()) as u128)
            }
        }
    };
}

impl_cv!(DataCv Data DataPair DataChunk u8   "bytes");
impl_cv!(RefsCv Refs RefsPair RefsChunk Hash "hashes");

impl<T> ToaKvStore<T>
where
    T: toa_kv::ToaKv,
{
    fn get<'a>(&'a self, namespace: u8, key: &[u8; 32]) -> Result<Option<T::Get<'a>>, T::Error> {
        let mut k = [0; 33];
        k[0] = namespace;
        k[1..].copy_from_slice(key);
        self.0.get(&k)
    }

    fn set<'a>(&'a self, namespace: u8, key: &[u8; 32], value: &[u8]) -> Result<(), T::Error> {
        let mut k = [0; 33];
        k[0] = namespace;
        k[1..].copy_from_slice(key);
        if self.0.has(key)? {
            Ok(())
        } else {
            self.0.set(&k, value)
        }
    }
}

impl<T> ToaStore for ToaKvStore<T>
where
    T: toa_kv::ToaKv,
{
    type Error = ToaKvStoreError<T::Error>;
    type Chunk<'a>
        = T::Get<'a>
    where
        Self: 'a;

    fn add_chunk(&self, key: &[u8; 32], value: &[u8]) -> Result<(), Self::Error> {
        self.set(NAMESPACE_CHUNKS, key, value)
            .map_err(ToaKvStoreError::Kv)
    }
    fn add_pair(&self, key: &[u8; 32], value: &[u8; 64]) -> Result<(), Self::Error> {
        self.set(NAMESPACE_PAIRS, key, value)
            .map_err(ToaKvStoreError::Kv)
    }
    fn add_root(&self, key: &[u8; 32], value: &[u8; 96]) -> Result<(), Self::Error> {
        self.set(NAMESPACE_ROOTS, key, value)
            .map_err(ToaKvStoreError::Kv)
    }

    fn get_chunk<'a>(&'a self, key: &[u8; 32]) -> Result<Option<Self::Chunk<'a>>, Self::Error> {
        self.get(NAMESPACE_CHUNKS, key).map_err(ToaKvStoreError::Kv)
    }
    fn get_pair(&self, key: &[u8; 32]) -> Result<Option<[u8; 64]>, Self::Error> {
        self.get(NAMESPACE_PAIRS, key)
            .map_err(ToaKvStoreError::Kv)?
            .map(|x| <[u8; 64]>::try_from(x.as_ref()))
            .transpose()
            .map_err(|_| ToaKvStoreError::InvalidPair)
    }
    fn get_root(&self, key: &[u8; 32]) -> Result<Option<[u8; 96]>, Self::Error> {
        self.get(NAMESPACE_ROOTS, key)
            .map_err(ToaKvStoreError::Kv)?
            .map(|x| <[u8; 96]>::try_from(x.as_ref()))
            .transpose()
            .map_err(|_| ToaKvStoreError::InvalidRoot)
    }

    fn iter_roots_with(&self, f: &mut dyn FnMut(&[u8; 32])) -> Result<(), Self::Error> {
        self.0
            .iter_prefix_with(&[NAMESPACE_ROOTS], &mut |x| {
                let x: &[u8; 32] = x.as_ref()[1..].try_into().unwrap();
                (f)(x)
            })
            .map_err(ToaKvStoreError::Kv)
    }

    fn size_on_disk(&self) -> Result<u128, Self::Error> {
        self.0.size_on_disk().map_err(ToaKvStoreError::Kv)
    }
}

impl<T> From<ReadError<T>> for ReadExactError<T> {
    fn from(x: ReadError<T>) -> Self {
        match x {
            ReadError::MissingChunk => Self::MissingChunk,
            ReadError::MissingPair => Self::MissingPair,
            ReadError::Store(x) => Self::Store(x),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{cell::RefCell, collections::BTreeMap};

    type Toa = super::Toa<ToaKvStore<RefCell<BTreeMap<Box<[u8]>, Box<[u8]>>>>>;

    struct TestBuild {
        builder: Toa,
    }

    struct TestRead {
        reader: Toa,
    }

    impl TestBuild {
        fn finish(self) -> TestRead {
            TestRead {
                reader: self.builder,
            }
        }

        fn add(&mut self, data: &[u8]) -> Hash {
            let key = self.builder.add(data, &[]).expect("add failed");
            assert_eq!(key, toa_core::hash(data, &[]));
            key
        }
    }

    impl TestRead {
        fn assert_eq(&self, key: &Hash, value: &[u8]) {
            let o = self
                .reader
                .get(&key)
                .expect("get failed")
                .expect("object does not exist");
            assert_eq!(o.data_len(), (value.len() as u128) << 3);
            let x = &mut *vec![0; value.len()];
            let n = o.data().read(0, x).expect("read failed");
            assert_eq!(n, value.len());
            let f = String::from_utf8_lossy;
            assert!(x == value, "{:?} <> {:?}", f(&x), f(value));
        }
    }

    fn init() -> TestBuild {
        let builder = Toa::new(Default::default());
        TestBuild { builder }
    }

    #[test]
    fn insert_one_empty() {
        let mut s = init();
        let key = s.add(b"");
        let s = s.finish();
        s.assert_eq(&key, &[]);
    }

    #[test]
    fn insert_one() {
        let mut s = init();
        let key = s.add(b"Hello, world!");
        let s = s.finish();
        s.assert_eq(&key, b"Hello, world!");
    }

    #[test]
    fn insert_two() {
        let mut s = init();
        let a = s.add(b"Hello, world!");
        let b = s.add(b"Greetings!");
        let s = s.finish();
        s.assert_eq(&a, b"Hello, world!");
        s.assert_eq(&b, b"Greetings!");
    }

    #[test]
    fn insert_many() {
        let mut s = init();
        let f = |x| format!("A number {x}").into_bytes();
        let keys = (0..1 << 12).map(|i| s.add(&f(i))).collect::<Vec<_>>();
        let s = s.finish();
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
        let s = s.finish();
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_one_2_chunks() {
        let mut s = init();
        let v = (0..CHUNK_SIZE as usize * 2)
            .fold(String::new(), |s, _| s + "x")
            .into_bytes();
        let k = s.add(&v);
        let s = s.finish();
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_one_large() {
        let mut s = init();
        let v = (0..1 << 19)
            .fold(String::new(), |s, _| s + "x")
            .into_bytes();
        let k = s.add(&v);
        let s = s.finish();
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_one_large_zeros() {
        let mut s = init();
        let v = vec![0; 1 << 20];
        let k = s.add(&v);
        let s = s.finish();
        s.assert_eq(&k, &v);
    }

    #[test]
    fn insert_many_large() {
        let n = 1 << 21;
        let mut s = init();
        let keys = (0..=255)
            .map(|x| (x, s.add(&vec![x; n])))
            .collect::<Vec<_>>();
        let s = s.finish();
        keys.iter().for_each(|(x, k)| s.assert_eq(k, &vec![*x; n]));
    }
}
