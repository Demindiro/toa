#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![forbid(unsafe_code, unused_must_use, elided_named_lifetimes)]

extern crate alloc;

pub mod device;
pub mod object;
pub mod record;
pub mod snapshot;

use alloc::vec::Vec;
use chacha20poly1305::{
    AeadCore, AeadInPlace, Key, KeyInit, Tag, XChaCha12Poly1305, XNonce,
    aead::rand_core::{CryptoRng, RngCore},
};
use core::{fmt, mem};

pub struct Appender<D> {
    records: RecordCache<D>,
    objects: object::ObjectTrie,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Hash([u8; 32]);

pub type Read = Vec<u8>;

pub struct IterRead<'a, D> {
    object: &'a mut Object<'a, D>,
    offset: u64,
    remaining: usize,
}

pub struct Object<'a, D> {
    appender: &'a mut Appender<D>,
    snapshot: SnapshotRoot,
    ptr: ObjectPointer,
}

#[derive(Clone, Debug)]
pub enum Error<D> {
    Device(D),
    Crypto(chacha20poly1305::Error),
}

#[derive(Clone, Debug)]
pub struct Unmount {
    pub root: SnapshotRoot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotRoot(u64);

struct RecordCache<D> {
    device: D,
    key: Key,
    record_pitch: u8,
    next_record: Vec<u8>,
    snapshot_len: SnapshotOffset,
    record_stack: Vec<record::Entry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SnapshotOffset(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ObjectPointer {
    offset: SnapshotOffset,
    len: u64,
}

impl<D> Appender<D> {
    pub fn init<R>(device: D, record_pitch: u8, rng: R) -> Self
    where
        R: CryptoRng + RngCore,
    {
        // always initialize with an empty object
        //
        // Having at least one object means we never have to check for
        // "no objects", which will be the absolutely most common case,
        // so worth saving instructions in all cases.
        let key = Hash(blake3::hash(&[]).into());
        let objects = object::ObjectTrie::with_leaf(
            &key,
            ObjectPointer {
                offset: SnapshotOffset(0),
                len: 0,
            },
        );
        Self {
            records: RecordCache::new(device, XChaCha12Poly1305::generate_key(rng), record_pitch),
            objects,
        }
    }

    pub fn into_device_key(self) -> (D, Key) {
        (self.records.device, self.records.key)
    }
}

impl<D> Appender<D>
where
    D: device::Device,
{
    pub fn mount(
        device: D,
        key: Key,
        root: SnapshotRoot,
        record_pitch: u8,
    ) -> Result<Self, Error<D::Error>> {
        let read = device
            .read(root.0, snapshot::Snapshot::ENCRYPTED_LEN)
            .map_err(Error::Device)?;
        let snapshot =
            snapshot::Snapshot::decrypt(read.as_ref().try_into().expect("exact bytes"), &key)
                .unwrap();
        drop(read);
        Ok(Self {
            records: RecordCache::new(device, key, record_pitch),
            objects: object::ObjectTrie::with_external_root(
                root,
                SnapshotOffset(snapshot.object_trie_root),
            ),
        })
    }

    pub fn add<R>(&mut self, rng: &mut R, data: &[u8]) -> Result<Hash, Error<D::Error>>
    where
        R: CryptoRng + RngCore,
    {
        let key = Hash(blake3::hash(data).into());
        let len = u64::try_from(data.len()).expect("usize <= u64");
        let dev = |_, _: SnapshotOffset, _: &mut [u8]| {
            todo!();
        };
        let insert = match self.objects.find(&key, dev)? {
            object::Find::Object(_, _) => return Ok(key),
            object::Find::None(x) => x,
        };
        let offset = self.records.write(rng, data)?;
        insert.insert(ObjectPointer { offset, len }, dev)?;
        Ok(key)
    }

    pub fn get(&mut self, key: &Hash) -> Result<Option<Object<'_, D>>, Error<D::Error>> {
        let dev = |snapshot, offset, out: &mut [_]| {
            self.records
                .read(snapshot, offset, out.len())
                .map(|x| out.copy_from_slice(x.as_ref()))
        };
        self.objects
            .find(key, dev)
            .map(|x| x.into_object())
            .map(|x| {
                x.map(|(snapshot, ptr)| Object {
                    appender: self,
                    snapshot,
                    ptr,
                })
            })
    }

    pub fn contains_key(&mut self, key: &Hash) -> Result<bool, Error<D::Error>> {
        self.objects
            .find(key, |_, _, _| todo!())
            .map(|x| x.is_none())
    }

    fn data(
        &mut self,
        snapshot: SnapshotRoot,
        offset: SnapshotOffset,
        rdlen: usize,
    ) -> Result<Read, Error<D::Error>> {
        self.records.read(snapshot, offset, rdlen)
    }

    pub fn commit<R>(&mut self, rng: &mut R) -> Result<Option<SnapshotRoot>, Error<D::Error>>
    where
        R: CryptoRng + RngCore,
    {
        if !self.objects.dirty() {
            return Ok(None);
        }
        let object_trie_root = self.commit_object_trie(rng)?;
        self.records.commit(object_trie_root, rng).map(Some)
    }

    fn commit_object_trie<R>(&mut self, rng: &mut R) -> Result<SnapshotOffset, Error<D::Error>>
    where
        R: CryptoRng + RngCore,
    {
        self.records.flush(rng)?;
        self.objects
            .serialize(|data| self.records.write_inside_bounds(rng, data))
    }
}

impl<D> RecordCache<D> {
    fn new(device: D, key: Key, record_pitch: u8) -> Self {
        Self {
            device,
            key,
            record_pitch,
            next_record: Default::default(),
            snapshot_len: SnapshotOffset(0),
            record_stack: Default::default(),
        }
    }

    fn record_pitch(&self) -> usize {
        1 << self.record_pitch
    }

    fn next_record_offset(&self) -> SnapshotOffset {
        let mut n = self.snapshot_len;
        n.0 -= u64::try_from(self.next_record.len()).expect("usize <= u64");
        n
    }

    fn next_record_remaining(&self) -> usize {
        self.record_pitch() - self.next_record.len()
    }

    fn next_record_is_full(&self) -> bool {
        self.next_record_remaining() == 0
    }

    fn offset_to_record(&self, x: SnapshotOffset) -> (record::Entry, usize) {
        let i = x.0 >> self.record_pitch;
        let offt = usize::try_from(x.0 - (i << self.record_pitch)).unwrap();
        let entry = self.record_stack[usize::try_from(i).unwrap()];
        (entry, offt)
    }

    fn record_finalize(&mut self) -> Vec<u8> {
        let remaining = (1 << self.record_pitch) - self.next_record.len();
        self.snapshot_len.0 += remaining as u64;
        mem::take(&mut self.next_record)
    }
}

impl<D> RecordCache<D>
where
    D: device::Device,
{
    fn current_data(
        &mut self,
        offset: SnapshotOffset,
        len: usize,
    ) -> Result<Read, Error<D::Error>> {
        if self.next_record_offset() <= offset {
            let start =
                usize::try_from(offset.0 - self.next_record_offset().0).expect("inside record");
            return Ok(self.next_record[start..start + len].into());
        }

        let (x, offt) = self.offset_to_record(offset);
        let x = self.read_record_nocache(&x)?;
        let x = &x[offt..];
        let x = &x[..len.min(x.len())];
        Ok(x.into())
    }

    fn read(
        &mut self,
        snapshot: SnapshotRoot,
        offset: SnapshotOffset,
        rdlen: usize,
    ) -> Result<Read, Error<D::Error>> {
        if snapshot == SnapshotRoot(u64::MAX) {
            return self.current_data(offset, rdlen);
        }

        let rd = |o, l| self.device.read(o, l).map_err(Error::Device);
        let snapshot = snapshot::Snapshot::decrypt(
            rd(snapshot.0, snapshot::Snapshot::ENCRYPTED_LEN)?
                .as_ref()
                .try_into()
                .expect("exact bytes"),
            &self.key,
        )
        .unwrap();
        let len = snapshot.len;
        let mut cur = snapshot.record_trie_root;
        let mask = (1u64 << self.record_pitch) - 1;
        let mut depth = self.record_pitch;
        const RLEN: u64 = record::Entry::LEN as u64;
        while (len - 1) >> depth >= 1 {
            depth += (mask / RLEN).trailing_ones() as u8;
        }
        while depth > self.record_pitch {
            depth -= (mask / RLEN).trailing_ones() as u8;
            let i = (offset.0 >> depth) & (mask / RLEN);
            let data = self.read_record_nocache(&cur)?;
            let stride = record::Entry::LEN;
            let i = usize::try_from(i).unwrap();
            cur = record::Entry::from_bytes(
                &data[stride * i..][..stride]
                    .try_into()
                    .expect("exact bytes"),
            );
        }
        let rdlen =
            (rdlen as u64).min(u64::from(cur.uncompressed_len) - (offset.0 & mask)) as usize;
        let offt = usize::try_from(offset.0 & mask).unwrap();
        self.read_record_nocache(&cur)
            .map(|x| x[offt..][..rdlen].into())
    }

    fn write<R>(&mut self, rng: &mut R, data: &[u8]) -> Result<SnapshotOffset, Error<D::Error>>
    where
        R: CryptoRng + RngCore,
    {
        let mut data = data;
        let offset = self.snapshot_len;
        while !data.is_empty() {
            if self.next_record_is_full() {
                self.flush(rng)?;
            }
            let n = data.len().min(self.next_record_remaining());
            let x;
            (x, data) = data.split_at(n);
            self.next_record.extend_from_slice(x);
            self.snapshot_len.0 += u64::try_from(x.len()).expect("usize <= u64");
        }
        Ok(offset)
    }

    fn write_inside_bounds<R>(
        &mut self,
        rng: &mut R,
        data: &[u8],
    ) -> Result<SnapshotOffset, Error<D::Error>>
    where
        R: CryptoRng + RngCore,
    {
        assert!(data.len() <= self.record_pitch());
        if data.len() > self.next_record_remaining() {
            self.flush(rng)?;
        }
        self.write(rng, data)
    }

    fn flush<R>(&mut self, rng: &mut R) -> Result<(), Error<D::Error>>
    where
        R: CryptoRng + RngCore,
    {
        if self.next_record.is_empty() {
            return Ok(());
        }

        let record = self.record_finalize();
        let uncompressed_len = u32::try_from(record.len()).unwrap();

        let (nonce, tag, record) = pack_record(&self.key, rng, &record);
        let compressed_len = u32::try_from(record.len()).unwrap();

        let offset = self.device.write(&record).map_err(Error::Device)?;

        self.record_stack.push(record::Entry {
            poly1305: tag,
            nonce,
            offset,
            compressed_len,
            uncompressed_len,
        });
        Ok(())
    }

    fn commit<R>(
        &mut self,
        object_trie_root: SnapshotOffset,
        rng: &mut R,
    ) -> Result<SnapshotRoot, Error<D::Error>>
    where
        R: CryptoRng + RngCore,
    {
        self.flush(rng)?;
        let len = self.snapshot_len.0;
        while self.record_stack.len() > 1 {
            for r in core::mem::take(&mut self.record_stack) {
                if self.next_record_is_full() {
                    self.flush(rng)?;
                }
                self.next_record.extend_from_slice(&r.into_bytes());
            }
            self.flush(rng)?;
        }
        let snap = snapshot::Snapshot {
            len,
            object_trie_root: object_trie_root.0,
            record_trie_root: self.record_stack.pop().expect("at least one record"),
        };
        let snap = snap.encrypt(&self.key, rng);
        let offset = self
            .device
            .write(&snap)
            .and_then(|x| self.device.sync().map(|()| x))
            .map(SnapshotRoot)
            .map_err(Error::Device)?;
        self.snapshot_len = SnapshotOffset(0);
        Ok(offset)
    }

    fn read_record_nocache(&mut self, entry: &record::Entry) -> Result<Vec<u8>, Error<D::Error>> {
        let len = usize::try_from(entry.compressed_len).expect("u32 <= usize");
        let data = self.device.read(entry.offset, len).map_err(Error::Device)?;
        // TODO avoid copy maybe?
        let mut data = data.as_ref().to_vec();
        unpack_record(&self.key, &entry.nonce, &entry.poly1305, &mut data).map_err(Error::Crypto)
    }
}

impl<'a, D> Object<'a, D>
where
    D: device::Device,
{
    pub fn read(&mut self, offset: u64, len: usize) -> Result<Read, Error<D::Error>> {
        if self.ptr.len <= offset {
            return Ok([].into());
        }
        let len = usize::try_from(self.ptr.len - offset)
            .unwrap_or(usize::MAX)
            .min(len);
        let offset = SnapshotOffset(self.ptr.offset.0 + offset);

        self.appender.data(self.snapshot, offset, len)
    }

    pub fn read_exact(
        &'a mut self,
        offset: u64,
        len: usize,
    ) -> Result<IterRead<'a, D>, Error<D::Error>> {
        Ok(IterRead {
            object: self,
            offset,
            remaining: len,
        })
    }
}

impl<'a, D> IterRead<'a, D>
where
    D: device::Device,
{
    pub fn into_bytes(self) -> Result<Vec<u8>, Error<D::Error>> {
        let mut v = Vec::new();
        for x in self {
            v.extend_from_slice(&x?);
        }
        Ok(v)
    }
}

impl<'a, D> Iterator for IterRead<'a, D>
where
    D: device::Device,
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

impl<'a, D> core::iter::FusedIterator for IterRead<'a, D> where D: device::Device {}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|x| write!(f, "{x:02x}"))
    }
}

#[must_use]
fn encrypt<R>(key: &Key, rng: R, data: &mut [u8]) -> (XNonce, Tag)
where
    R: CryptoRng + RngCore,
{
    let cipher = XChaCha12Poly1305::new(key);
    let nonce = XChaCha12Poly1305::generate_nonce(rng);
    let tag = cipher
        .encrypt_in_place_detached(&nonce, &[], data)
        .expect("failed to encrypt snapshot");
    (nonce, tag)
}

fn pack_record<R>(key: &Key, rng: R, data: &[u8]) -> (XNonce, Tag, Vec<u8>)
where
    R: CryptoRng + RngCore,
{
    let mut data = data.to_vec();
    let (nonce, tag) = encrypt(key, rng, &mut data);
    (nonce, tag, data)
}

fn unpack_record(
    key: &Key,
    nonce: &XNonce,
    tag: &Tag,
    data: &mut [u8],
) -> Result<Vec<u8>, chacha20poly1305::Error> {
    XChaCha12Poly1305::new(key).decrypt_in_place_detached(nonce, &[], data, tag)?;
    Ok(data.into())
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{SeedableRng, rngs::StdRng};

    const DEPTH: u8 = 12;

    struct Test {
        last_root: SnapshotRoot,
        appender: Appender<std::cell::RefCell<Vec<u8>>>,
        rng: StdRng,
    }

    impl Test {
        fn assert_eq(&mut self, key: &Hash, value: &[u8]) {
            let mut x = self.get(&key).unwrap().unwrap();
            let x = x.read_exact(0, usize::MAX).unwrap();
            let x = x.into_bytes().unwrap();
            assert_eq!(&x, value);
        }

        fn commit(&mut self) {
            self.appender
                .commit(&mut self.rng)
                .unwrap()
                .map(|x| self.last_root = x);
        }

        fn remount(mut self) -> Self {
            self.commit();
            let (dev, key) = self.appender.into_device_key();
            Self {
                last_root: self.last_root,
                appender: Appender::mount(dev, key, self.last_root, DEPTH).unwrap(),
                rng: self.rng,
            }
        }

        fn add(&mut self, data: &[u8]) -> Hash {
            self.appender.add(&mut self.rng, data).unwrap()
        }
    }

    impl core::ops::Deref for Test {
        type Target = Appender<core::cell::RefCell<Vec<u8>>>;

        fn deref(&self) -> &Self::Target {
            &self.appender
        }
    }

    impl core::ops::DerefMut for Test {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.appender
        }
    }

    fn init() -> Test {
        let mut rng = StdRng::from_seed([0; 32]);
        Test {
            last_root: SnapshotRoot(u64::MAX),
            appender: Appender::init(Default::default(), DEPTH, &mut rng),
            rng,
        }
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
    fn remount_noop() {
        let _ = init().remount();
    }

    #[test]
    fn remount_empty() {
        let mut s = init();
        let a = s.add(b"");
        let mut s = s.remount();
        s.assert_eq(&a, b"");
    }

    #[test]
    fn remount_one() {
        let mut s = init();
        let a = s.add(b"Hello, world!");
        let mut s = s.remount();
        s.assert_eq(&a, b"Hello, world!");
    }

    #[test]
    fn remount_many() {
        let mut s = init();
        let f = |x| format!("A number {x}").into_bytes();
        let keys = (0..1 << 12).map(|i| s.add(&f(i))).collect::<Vec<_>>();
        let mut s = s.remount();
        keys.iter()
            .enumerate()
            .for_each(|(i, k)| s.assert_eq(k, &f(i)));
    }

    // TODO we need tests to ensure crypto works!
}
