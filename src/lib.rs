pub mod device;
pub mod object;
pub mod record;
pub mod snapshot;

use core::fmt;
use device::Write;
use std::collections::BTreeMap;

pub struct Appender<D> {
    device: D,
    next_record: Vec<u8>,
    next_objects: BTreeMap<Hash, ObjectPointer>,
    snapshot_len: SnapshotOffset,
    record_pitch: u8,
    record_stack: Vec<record::Entry>,
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
    ptr: ObjectPointer,
}

#[derive(Clone, Debug)]
pub enum Error<D> {
    Device(D),
}

#[derive(Clone, Debug)]
pub struct Unmount {
    pub root: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SnapshotOffset(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ObjectPointer {
    offset: SnapshotOffset,
    len: u64,
}

impl<D> Appender<D> {
    pub fn new(device: D, record_pitch: u8) -> Self {
        Self {
            device,
            next_record: Default::default(),
            next_objects: Default::default(),
            snapshot_len: SnapshotOffset(0),
            record_pitch,
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
    pub fn mount(device: D, root: u64) -> Result<Self, Error<D::Error>> {
        todo!()
    }

    pub fn unmount(self) -> Result<(D, Unmount), Error<D::Error>> {
        todo!();
    }

    pub fn add(&mut self, data: &[u8]) -> Result<Hash, Error<D::Error>> {
        let key = Hash(blake3::hash(data).into());
        if self.contains_key(&key)? {
            return Ok(key);
        }
        let offset = self.record_append(data)?;
        let len = u64::try_from(data.len()).expect("usize <= u64");
        self.next_objects.insert(key, ObjectPointer { offset, len });
        Ok(key)
    }

    pub fn get(&mut self, key: &Hash) -> Result<Option<Object<'_, D>>, Error<D::Error>> {
        let ptr = if let Some(x) = self.next_objects.get(key) {
            *x
        } else {
            todo!("look for object on device");
        };
        Ok(Some(Object {
            appender: self,
            ptr,
        }))
    }

    fn contains_key(&mut self, key: &Hash) -> Result<bool, Error<D::Error>> {
        Ok(self.next_objects.contains_key(key))
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

        self.appender.current_data(offset, len)
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
        appender: Appender<std::cell::RefCell<Vec<u8>>>,
    }

    impl Test {
        fn assert_eq(&mut self, key: &Hash, value: &[u8]) {
            let mut x = self.get(&key).unwrap().unwrap();
            let x = x.read_exact(0, usize::MAX).unwrap();
            let x = x.into_bytes().unwrap();
            assert_eq!(&x, value);
        }

        fn remount(self) -> Self {
            let (dev, unmount) = self.appender.unmount().unwrap();
            let Unmount { root } = unmount;
            Self {
                appender: Appender::mount(dev, root).unwrap(),
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
            appender: Appender::new(Default::default(), 12),
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
    fn remount() {
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
