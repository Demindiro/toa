use crate::{
    DEPTH, Hash, ObjectPointer, PITCH, PackOffset, PackRef, device,
    object::builder::{Find, ObjectTrie},
    pack, record,
};
use alloc::vec::Vec;
use chacha20poly1305::{
    AeadInPlace, ChaCha12Poly1305, Key, KeyInit, Tag,
    aead::rand_core::{CryptoRng, RngCore},
};
use core::mem;

pub struct Builder<D> {
    key: Key,
    next_record: Vec<u8>,
    pack_len: PackOffset,
    writers: [RecordWriter; 1 + DEPTH as usize],
    objects: Option<ObjectTrie>,
    device: D,
}

#[derive(Default)]
struct RecordWriter {
    data: Vec<u8>,
    counter: u64,
}

#[derive(Clone, Debug)]
pub enum Error<D> {
    Device(D),
}

impl<D> Builder<D> {
    pub fn new<R>(device: D, rng: R) -> Self
    where
        R: CryptoRng + RngCore,
    {
        Self {
            key: ChaCha12Poly1305::generate_key(rng),
            next_record: Default::default(),
            pack_len: PackOffset(0),
            writers: Default::default(),
            objects: None,
            device,
        }
    }
}

impl<D> Builder<D>
where
    D: device::Write,
{
    pub fn add(&mut self, data: &[u8]) -> Result<Hash, Error<D::Error>> {
        self.add_with_key(Hash(blake3::hash(data).into()), data)
    }

    fn add_with_key(&mut self, key: Hash, data: &[u8]) -> Result<Hash, Error<D::Error>> {
        let len = u64::try_from(data.len()).expect("usize <= u64");
        let Some(mut objects) = self.objects.take() else {
            let offset = self.write(data)?;
            self.objects = Some(ObjectTrie::with_leaf(&key, ObjectPointer { offset, len }));
            return Ok(key);
        };
        let insert = match objects.find(&key) {
            Find::Object(_) => return Ok(key),
            Find::None(x) => x,
        };
        let offset = match self.write(data) {
            Ok(x) => x,
            Err(e) => {
                self.objects = Some(objects);
                return Err(e);
            }
        };
        insert.insert(ObjectPointer { offset, len });
        self.objects = Some(objects);
        Ok(key)
    }

    pub fn finish(mut self) -> Result<(D, Key, Option<PackRef>), Error<D::Error>> {
        self.flush_leaf()?;
        let Some(objects) = self.objects.take() else {
            return Ok((self.device, self.key, None));
        };
        let object_trie_root = objects.serialize(|data| self.write_inside_bounds(data))?;
        let record_trie_root = self.flush_all()?.expect("at least one object");
        let pack = pack::Pack {
            object_trie_root,
            record_trie_root,
        };
        let pack = PackRef(pack.encrypt(&self.key));
        self.device.sync().map_err(Error::Device)?;
        Ok((self.device, self.key, Some(pack)))
    }

    fn write(&mut self, data: &[u8]) -> Result<PackOffset, Error<D::Error>> {
        // TODO risk of desynchronization
        append_record(&mut self.device, &self.key, 1, &mut self.writers, data)?;
        let offset = self.pack_len;
        self.pack_len.0 += data.len() as u64;
        Ok(offset)
    }

    fn write_inside_bounds(&mut self, data: &[u8]) -> Result<PackOffset, Error<D::Error>> {
        assert!(data.len() <= (1 << PITCH));
        if data.len() > self.writers[0].remaining() {
            self.flush_leaf()?;
        }
        self.write(data)
    }

    fn flush_leaf(&mut self) -> Result<(), Error<D::Error>> {
        let [wr, writers @ ..] = &mut self.writers;
        let (dev, key) = (&mut self.device, &self.key);
        let x = flush_record(dev, key, 0, wr)?;
        if let Some(x) = x {
            append_record(dev, key, 1, writers, &x.into_bytes())?;
            const MASK: u64 = (1 << PITCH) - 1;
            self.pack_len.0 += MASK;
            self.pack_len.0 &= !MASK;
        }
        Ok(())
    }

    fn flush_all(&mut self) -> Result<Option<record::Entry>, Error<D::Error>> {
        self.flush_leaf()?;
        self.flush_leaf()?;
        self.flush_leaf()?;
        self.flush_leaf()?;
        let (dev, key) = (&mut self.device, &self.key);
        for d in 0..DEPTH {
            let [wr, writers @ ..] = &mut self.writers[d.into()..] else {
                unreachable!("at least one writer")
            };
            let d = u32::from(d);
            if let Some(x) = flush_record(dev, key, d, wr)? {
                append_record(dev, key, d + 1, writers, &x.into_bytes())?;
            }
        }
        let [.., wr] = &mut self.writers;
        flush_record(&mut self.device, &self.key, DEPTH.into(), wr)
    }
}

impl RecordWriter {
    #[must_use]
    fn append<'a, 'b>(&'a mut self, data: &'b [u8]) -> Option<(&'a mut Vec<u8>, u64, &'b [u8])> {
        if data.len() >= self.remaining() {
            let (add, rest) = data.split_at(self.remaining());
            self.data.extend_from_slice(add);
            let index = self.counter;
            self.counter += 1;
            Some((&mut self.data, index, rest))
        } else {
            self.data.extend_from_slice(data);
            None
        }
    }

    #[must_use]
    fn flush<'a>(&'a mut self) -> Option<(&'a mut Vec<u8>, u64)> {
        (!self.data.is_empty()).then(|| {
            let index = self.counter;
            self.counter += 1;
            (&mut self.data, index)
        })
    }

    fn remaining(&self) -> usize {
        (1 << PITCH) - self.data.len()
    }

    fn is_full(&self) -> bool {
        self.remaining() == 0
    }
}

fn pack_record(key: &Key, depth: u32, index: u64, data: &mut [u8]) -> (Tag, Vec<u8>) {
    let mut data = data.to_vec();
    let nonce = crate::record_nonce(depth, index);
    let tag = ChaCha12Poly1305::new(key)
        .encrypt_in_place_detached(&nonce, &[], &mut data)
        .expect("failed to encrypt data");
    (tag, data)
}

fn write_record<D>(
    dev: &mut D,
    key: &Key,
    depth: u32,
    index: u64,
    buf: &mut Vec<u8>,
) -> Result<record::Entry, Error<D::Error>>
where
    D: device::Write,
{
    let uncompressed_len = u32::try_from(buf.len()).unwrap();
    let (tag, record) = pack_record(key, depth, index, buf);
    let compressed_len = u32::try_from(record.len()).unwrap();
    let offset = dev.append(&record).map_err(Error::Device)?;
    buf.clear();
    Ok(record::Entry {
        tag,
        offset,
        compression_algorithm: 0,
        compressed_len,
        uncompressed_len,
    })
}

fn append_record<D>(
    dev: &mut D,
    key: &Key,
    depth: u32,
    writers: &mut [RecordWriter],
    data: &[u8],
) -> Result<(), Error<D::Error>>
where
    D: device::Write,
{
    let [wr, writers @ ..] = writers else {
        panic!("excessive depth")
    };
    let mut data = data;
    while let Some((buf, index, rest)) = wr.append(data) {
        data = rest;
        let entry = write_record(dev, key, depth, index, buf)?;
        append_record(dev, key, 1 + depth, writers, &entry.into_bytes())?;
    }
    Ok(())
}

fn flush_record<D>(
    dev: &mut D,
    key: &Key,
    depth: u32,
    writer: &mut RecordWriter,
) -> Result<Option<record::Entry>, Error<D::Error>>
where
    D: device::Write,
{
    writer
        .flush()
        .map(|(buf, index)| write_record(dev, key, depth, index, buf))
        .transpose()
}
