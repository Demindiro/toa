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

pub struct ReadTicket(usize);
pub struct WriteTicket(usize);

pub enum Read {
    Wait(ReadTicket),
    Done(Vec<u8>),
}

pub enum Event<'a> {
    Read {
        ticket: ReadTicket,
        data: &'a [u8],
    },
    Write {
        ticket: WriteTicket,
        data: &'a mut (),
    },
}

#[derive(Clone, Debug)]
pub enum Error<D> {
    Device(D),
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

    pub fn read(
        &mut self,
        key: &Hash,
        offset: u64,
        len: usize,
    ) -> Result<Option<Read>, Error<D::Error>> {
        let ptr = if let Some(x) = self.next_objects.get(key) {
            *x
        } else {
            todo!("look for object on device");
        };
        if ptr.len <= offset {
            return Ok(Some(Read::Done([].into())));
        }
        let len = usize::try_from(ptr.len - offset)
            .unwrap_or(usize::MAX)
            .min(len);
        let offset = SnapshotOffset(ptr.offset.0 + offset);
        if self.next_record_offset() <= offset {
            dbg!(offset, self.next_record_offset());
            let start =
                usize::try_from(offset.0 - self.next_record_offset().0).expect("inside record");
            return Ok(Some(Read::Done(
                self.next_record[start..start + len].into(),
            )));
        }

        let (record_addr, record_offset) = self.offset_to_record(offset);
        let x = self
            .device
            .read(record_addr, self.record_pitch())
            .map_err(Error::Device)?;
        // TODO decompress
        return Ok(Some(Read::Done(x.as_ref()[record_offset..][..len].into())));
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
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|x| write!(f, "{x:02x}"))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn init() -> Appender<std::cell::RefCell<Vec<u8>>> {
        Appender::new(Default::default(), 12)
    }

    #[test]
    fn insert_one_empty() {
        let mut s = init();
        let key = s.add(b"").unwrap();
        match s.read(&key, 0, 10).unwrap().unwrap() {
            Read::Wait(_) => unreachable!(),
            Read::Done(x) => assert_eq!(x, &[]),
        }
    }

    #[test]
    fn insert_one() {
        let mut s = init();
        let key = s.add(b"Hello, world!").unwrap();
        match s.read(&key, 0, 10).unwrap().unwrap() {
            Read::Wait(_) => unreachable!(),
            Read::Done(x) => assert_eq!(x, b"Hello, wor"),
        }
        match s.read(&key, 0, 100).unwrap().unwrap() {
            Read::Wait(_) => unreachable!(),
            Read::Done(x) => assert_eq!(x, b"Hello, world!"),
        }
    }

    #[test]
    fn insert_two() {
        let mut s = init();
        let a = s.add(b"Hello, world!").unwrap();
        let b = s.add(b"Greetings!").unwrap();
        match s.read(&a, 0, 100).unwrap().unwrap() {
            Read::Wait(_) => unreachable!(),
            Read::Done(x) => assert_eq!(x, b"Hello, world!"),
        }
        match s.read(&b, 0, 100).unwrap().unwrap() {
            Read::Wait(_) => unreachable!(),
            Read::Done(x) => assert_eq!(x, b"Greetings!"),
        }
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
            .for_each(|(i, k)| match s.read(k, 0, 100).unwrap().unwrap() {
                Read::Wait(_) => unreachable!(),
                Read::Done(x) => assert_eq!(x, f(i)),
            });
    }
}
