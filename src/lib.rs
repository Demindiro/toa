pub mod device;
pub mod object;
pub mod record;
pub mod snapshot;

use core::fmt;
use device::Write;
use std::collections::HashMap;

pub struct Appender<D> {
    device: D,
    next_record: Vec<u8>,
    objects: object::ObjectTrie,
    snapshot_len: SnapshotOffset,
    record_pitch: u8,
    record_stack: Vec<record::Entry>,
    snapshots: HashMap<SnapshotRoot, Snapshot>,
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
}

#[derive(Clone, Debug)]
pub struct Unmount {
    pub root: SnapshotRoot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotRoot(u64);

struct Snapshot {
    object_trie_root: SnapshotOffset,
    record_trie_root: record::Entry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SnapshotOffset(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ObjectPointer {
    offset: SnapshotOffset,
    len: u64,
}

impl<D> Appender<D> {
    pub fn init(device: D, record_pitch: u8) -> Self {
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
            device,
            next_record: Default::default(),
            objects,
            snapshot_len: SnapshotOffset(0),
            record_pitch,
            record_stack: Default::default(),
            snapshots: Default::default(),
        }
    }

    pub fn into_device(self) -> D {
        self.device
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

    fn current_offset(&self) -> SnapshotOffset {
        self.snapshot_len
    }

    fn offset_to_record(&self, x: SnapshotOffset) -> (u64, usize) {
        let i = x.0 >> self.record_pitch;
        let e = &self.record_stack[usize::try_from(i).unwrap()];
        (
            e.offset,
            usize::try_from(x.0 - (i << self.record_pitch)).unwrap(),
        )
    }
}

impl<D> Appender<D>
where
    D: device::Device,
{
    pub fn mount(device: D, root: SnapshotRoot, record_pitch: u8) -> Result<Self, Error<D::Error>> {
        let read = device.read(root.0, 64).map_err(Error::Device)?;
        let snapshot = snapshot::Snapshot::from_bytes(read.as_ref().try_into().expect("64 bytes"));
        drop(read);
        Ok(Self {
            device,
            next_record: Default::default(),
            objects: object::ObjectTrie::with_external_root(
                root,
                SnapshotOffset(snapshot.object_trie_root),
            ),
            snapshot_len: SnapshotOffset(0),
            record_pitch,
            record_stack: Default::default(),
            snapshots: Default::default(),
        })
    }

    pub fn add(&mut self, data: &[u8]) -> Result<Hash, Error<D::Error>> {
        let key = Hash(blake3::hash(data).into());
        let offset = self.snapshot_len;
        let len = u64::try_from(data.len()).expect("usize <= u64");
        let dev = |_, _: SnapshotOffset, _: &mut [u8]| {
            todo!();
        };
        let insert = match self.objects.find(&key, dev)? {
            object::Find::Object(_, _) => return Ok(key),
            object::Find::None(x) => x,
        };
        insert.insert(ObjectPointer { offset, len }, dev);
        self.record_append(data)?;
        Ok(key)
    }

    pub fn get(&mut self, key: &Hash) -> Result<Option<Object<'_, D>>, Error<D::Error>> {
        let dev = |snapshot: SnapshotRoot, offset: SnapshotOffset, out: &mut [u8]| {
            let mut rd = |o, l| self.device.read(o, l).map_err(Error::Device);
            let snapshot = snapshot::Snapshot::from_bytes(
                rd(snapshot.0, 64)?.as_ref().try_into().expect("64 bytes"),
            );
            let mut len = snapshot.len;
            let mut cur = snapshot.record_trie_root;
            let mask = (1u64 << self.record_pitch) - 1;
            let mut depth = self.record_pitch;
            dbg!(depth);
            while (len - 1) >> depth >= 1 {
                depth += (mask / 32).trailing_ones() as u8;
            }
            dbg!(depth);
            while depth > self.record_pitch {
                depth -= (mask / 32).trailing_ones() as u8;
                let i = (offset.0 >> depth) & (mask / 32);
                dbg!(&cur, i, cur.offset + 32 * i);
                cur = record::Entry::from_bytes(
                    rd(cur.offset + 32 * i, 32)?
                        .as_ref()
                        .try_into()
                        .expect("32 bytes"),
                );
            }
            dbg!((), &cur);
            dbg!(
                out.len(),
                depth,
                offset.0,
                offset.0 >> depth,
                len,
                len >> depth,
                self.record_pitch,
                1 << self.record_pitch,
                mask,
                offset.0 & mask
            );
            assert!(cur.uncompressed_len as u64 - (offset.0 & mask) >= out.len() as u64);
            out.copy_from_slice(rd(cur.offset + (offset.0 & mask), out.len())?.as_ref());
            dbg!(&out);
            Ok(())
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

    fn contains_key(&mut self, key: &Hash) -> Result<bool, Error<D::Error>> {
        self.objects
            .find(key, |_, _, _| todo!())
            .map(|x| x.is_none())
    }

    fn record_append(&mut self, data: &[u8]) -> Result<SnapshotOffset, Error<D::Error>> {
        let mut data = data;
        let offset = self.snapshot_len;
        while !data.is_empty() {
            if self.next_record_is_full() {
                self.record_flush()?;
            }
            let n = data.len().min(self.next_record_remaining());
            let x;
            (x, data) = data.split_at(n);
            self.next_record.extend_from_slice(x);
            self.snapshot_len.0 += u64::try_from(x.len()).expect("usize <= u64");
        }
        Ok(offset)
    }

    fn record_flush(&mut self) -> Result<(), Error<D::Error>> {
        if self.next_record.is_empty() {
            return Ok(());
        }

        let remaining = (1 << self.record_pitch) - self.next_record.len();
        self.snapshot_len.0 += remaining as u64;

        let record_len = u32::try_from(self.next_record.len()).unwrap();
        let mut x = self
            .device
            .write(self.next_record.len())
            .map_err(Error::Device)?;
        x.append(&self.next_record).map_err(Error::Device)?;
        let offset = x.offset();
        self.record_stack.push(record::Entry {
            offset,
            compression_info: record::CompressionInfo::new_uncompressed(record_len).unwrap(),
            uncompressed_len: record_len,
            poly1305: 0,
        });
        self.next_record.clear();
        Ok(())
    }

    fn current_data(
        &mut self,
        offset: SnapshotOffset,
        len: usize,
    ) -> Result<Read, Error<D::Error>> {
        if self.next_record_offset() <= offset {
            dbg!(offset, self.next_record_offset());
            let start =
                usize::try_from(offset.0 - self.next_record_offset().0).expect("inside record");
            return Ok(self.next_record[start..start + len].into());
        }

        let (record_addr, record_offset) = self.offset_to_record(offset);
        let x = self
            .device
            .read(record_addr, self.record_pitch())
            .map_err(Error::Device)?;
        // TODO decompress
        let x = &x.as_ref()[record_offset..];
        let x = &x[..len.min(x.len())];
        Ok(x.into())
    }

    fn data(
        &mut self,
        snapshot: SnapshotRoot,
        offset: SnapshotOffset,
        rdlen: usize,
    ) -> Result<Read, Error<D::Error>> {
        if snapshot == SnapshotRoot(u64::MAX) {
            return self.current_data(offset, rdlen);
        }

        let mut rd = |o, l| self.device.read(o, l).map_err(Error::Device);
        let snapshot = snapshot::Snapshot::from_bytes(
            rd(snapshot.0, 64)?.as_ref().try_into().expect("64 bytes"),
        );
        let mut len = snapshot.len;
        let mut cur = snapshot.record_trie_root;
        let mask = (1u64 << self.record_pitch) - 1;
        let mut depth = self.record_pitch;
        while (len - 1) >> depth >= 1 {
            depth += (mask / 32).trailing_ones() as u8;
        }
        while depth > self.record_pitch {
            depth -= (mask / 32).trailing_ones() as u8;
            let i = (offset.0 >> depth) & (mask / 32);
            dbg!(&cur);
            cur = record::Entry::from_bytes(
                rd(cur.offset + 32 * i, 32)?
                    .as_ref()
                    .try_into()
                    .expect("32 bytes"),
            );
        }
        dbg!((), &cur);
        dbg!(
            depth,
            offset.0,
            offset.0 >> depth,
            len,
            len >> depth,
            self.record_pitch,
            1 << self.record_pitch,
            mask,
            offset.0 & mask
        );
        // TODO zeroing??
        let rdlen =
            (rdlen as u64).min(u64::from(cur.uncompressed_len) - (offset.0 & mask)) as usize;
        rd(cur.offset + (offset.0 & mask), rdlen).unwrap_or_else(|_| todo!("WHAUTHEUITHUEHUIF"));
        rd(cur.offset + (offset.0 & mask), rdlen).map(|x| x.as_ref()[..rdlen].into())
    }

    fn commit(&mut self) -> Result<Option<SnapshotRoot>, Error<D::Error>> {
        if !self.objects.dirty() {
            return Ok(None);
        }

        let object_trie_root = self.commit_object_trie()?;
        let len = self.snapshot_len.0;
        let record_trie_root = self.commit_record_trie()?;

        let snap = snapshot::Snapshot {
            poly1305: 0,
            len,
            object_trie_root: object_trie_root.0,
            record_trie_root,
        };
        let snap = snap.into_bytes();
        let offset = self
            .device
            .write(snap.len())
            .and_then(|mut x| x.append(&snap).map(|()| x.offset()))
            .and_then(|x| self.device.sync().map(|()| x))
            .map(SnapshotRoot)
            .map_err(Error::Device)?;
        self.snapshots.insert(
            offset,
            Snapshot {
                object_trie_root,
                record_trie_root,
            },
        );
        self.snapshot_len = SnapshotOffset(0);
        Ok(Some(offset))
    }

    fn commit_object_trie(&mut self) -> Result<SnapshotOffset, Error<D::Error>> {
        self.record_flush()?;
        self.objects.serialize(|data| {
            let remaining = (1 << self.record_pitch) - self.next_record.len();
            if remaining < data.len() {
                //if self.next_record_remaining() < data.len() {
                {
                    {
                        {
                            let record_len = u32::try_from(self.next_record.len()).unwrap();
                            let mut x = self
                                .device
                                .write(self.next_record.len())
                                .map_err(Error::Device)?;
                            x.append(&self.next_record).map_err(Error::Device)?;
                            let offset = x.offset();
                            self.record_stack.push(record::Entry {
                                offset,
                                compression_info: record::CompressionInfo::new_uncompressed(
                                    record_len,
                                )
                                .unwrap(),
                                uncompressed_len: record_len,
                                poly1305: 0,
                            });
                            self.next_record.clear();
                        }
                    }
                }
                self.snapshot_len.0 += remaining as u64;
            }
            let offt = /* self.current_offset() */ self.snapshot_len;
            self.next_record.extend_from_slice(data);
            self.snapshot_len.0 += data.len() as u64;
            Ok(offt)
        })
    }

    fn commit_record_trie(&mut self) -> Result<record::Entry, Error<D::Error>> {
        self.record_flush()?;
        dbg!(&self.record_stack);
        if self.record_stack.len() > 1 {
            //let mut parent = Vec::new();
            for r in core::mem::take(&mut self.record_stack) {
                if self.next_record_is_full() {
                    todo!("flush");
                }
                self.next_record.extend_from_slice(&r.into_bytes());
            }
            self.record_flush()?;
        }
        dbg!(&self.record_stack);
        Ok(self.record_stack.pop().unwrap())
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
    ) -> Result<IterRead<D>, Error<D::Error>> {
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
    fn into_bytes(mut self) -> Result<Vec<u8>, Error<D::Error>> {
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

#[cfg(test)]
mod test {
    use super::*;

    struct Test {
        last_root: SnapshotRoot,
        appender: Appender<std::cell::RefCell<Vec<u8>>>,
    }

    impl Test {
        fn assert_eq(&mut self, key: &Hash, value: &[u8]) {
            let mut x = self.get(&key).unwrap().unwrap();
            let x = x.read_exact(0, usize::MAX).unwrap();
            let x = x.into_bytes().unwrap();
            assert_eq!(&x, value);
        }

        fn commit(&mut self) {
            self.appender.commit().unwrap().map(|x| self.last_root = x);
        }

        fn remount(mut self) -> Self {
            self.commit();
            let dev = self.appender.into_device();
            dbg!(&dev);
            Self {
                last_root: self.last_root,
                appender: Appender::mount(dev, self.last_root, 12).unwrap(),
            }
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
        Test {
            last_root: SnapshotRoot(u64::MAX),
            appender: Appender::init(Default::default(), 12),
        }
    }

    #[test]
    fn insert_one_empty() {
        let mut s = init();
        let key = s.add(b"").unwrap();
        s.assert_eq(&key, &[]);
    }

    #[test]
    fn insert_one() {
        let mut s = init();
        let key = s.add(b"Hello, world!").unwrap();
        s.assert_eq(&key, b"Hello, world!");
    }

    #[test]
    fn insert_two() {
        let mut s = init();
        let a = s.add(b"Hello, world!").unwrap();
        let b = s.add(b"Greetings!").unwrap();
        s.assert_eq(&a, b"Hello, world!");
        s.assert_eq(&b, b"Greetings!");
    }

    #[test]
    fn insert_many() {
        let mut s = init();
        let f = |x| format!("A number {x}").into_bytes();
        let keys = (0..1 << 12)
            .map(|i| s.add(&f(i)).unwrap())
            .collect::<Vec<_>>();
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
        let a = s.add(b"").unwrap();
        let mut s = s.remount();
        s.assert_eq(&a, b"");
    }

    #[test]
    fn remount_one() {
        let mut s = init();
        let a = s.add(b"Hello, world!").unwrap();
        let mut s = s.remount();
        s.assert_eq(&a, b"Hello, world!");
    }

    #[test]
    fn remount_many() {
        let mut s = init();
        let f = |x| format!("A number {x}").into_bytes();
        let keys = (0..1 << 12)
            .map(|i| s.add(&f(i)).unwrap())
            .collect::<Vec<_>>();
        let mut s = s.remount();
        keys.iter()
            .enumerate()
            .for_each(|(i, k)| s.assert_eq(k, &f(i)));
    }
}
