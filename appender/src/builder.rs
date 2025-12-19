use crate::{
    DEPTH, Hash, ObjectRaw, PITCH, PackOffset, PackRef, device, object::builder::ObjectTrie, pack,
    record, record::CompressionAlgorithm,
};
use alloc::vec::Vec;
use chacha20poly1305::{
    AeadInPlace, ChaCha12Poly1305, Key, KeyInit, Tag,
    aead::rand_core::{CryptoRng, RngCore},
};

pub struct Builder<D> {
    key: Key,
    pack_len: PackOffset,
    writers: [RecordWriter; 1 + DEPTH as usize],
    objects: Option<ObjectTrie>,
    device: D,
}

pub struct PackedRecord {
    entry: record::Entry,
    parent_depth: u8,
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
        // TODO avoid take(). We do this because self.write() is a pain with lifetimes.
        let Some(mut objects) = self.objects.take() else {
            let offset = self.write(data)?;
            self.objects = Some(ObjectTrie::with_leaf(&key, ObjectRaw { offset, len }));
            return Ok(key);
        };
        let insert = match objects.try_insert(&key) {
            None => {
                self.objects = Some(objects);
                return Ok(key);
            }
            Some(x) => x,
        };
        let offset = match self.write(data) {
            Ok(x) => x,
            Err(e) => {
                self.objects = Some(objects);
                return Err(e);
            }
        };
        insert.insert(ObjectRaw { offset, len });
        self.objects = Some(objects);
        Ok(key)
    }

    pub fn finish(mut self) -> Result<(D, Option<PackRef>), Error<D::Error>> {
        self.flush_leaf()?;
        let Some(objects) = self.objects.take() else {
            return Ok((self.device, None));
        };
        let object_trie_root = objects.serialize(|data| self.write_inside_bounds(data))?;
        let record_trie_root = self.flush_all()?.expect("at least one object");
        assert_eq!(
            record_trie_root.parent_depth,
            DEPTH + 1,
            "final record is not at root"
        );
        let pack = pack::Pack {
            key: self.key,
            object_trie_root,
            record_trie_root: record_trie_root.entry,
        };
        let pack = PackRef(pack.into_bytes());
        self.device.sync().map_err(Error::Device)?;
        Ok((self.device, Some(pack)))
    }

    fn write(&mut self, data: &[u8]) -> Result<PackOffset, Error<D::Error>> {
        // TODO risk of desynchronization
        self.append_record(data)?;
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
        if let Some(x) = self.flush_record(0)? {
            self.append_record_parent(x)?;
            const MASK: u64 = (1 << PITCH) - 1;
            self.pack_len.0 += MASK;
            self.pack_len.0 &= !MASK;
        }
        Ok(())
    }

    fn flush_all(&mut self) -> Result<Option<PackedRecord>, Error<D::Error>> {
        for d in 0..DEPTH {
            if let Some(x) = self.flush_record(d)? {
                self.append_record_parent(x)?;
            }
        }
        let mut entry = self.flush_record(DEPTH)?;
        Ok(entry)
    }

    fn append_record(&mut self, mut data: &[u8]) -> Result<(), Error<D::Error>> {
        while let Some((buf, index, rest)) = self.writers[0].append(data) {
            data = rest;
            let buf = &mut core::mem::take(buf);
            let entry = self.write_record(0, index, buf)?;
            self.append_record_parent(entry)?;
        }
        Ok(())
    }

    fn append_record_parent(&mut self, mut entry: PackedRecord) -> Result<(), Error<D::Error>> {
        while let Some((buf, index, rest)) =
            self.writers[usize::from(entry.parent_depth)].append(&entry.entry.into_bytes())
        {
            assert!(rest.is_empty(), "partial parent");
            let buf = &mut core::mem::take(buf);
            entry = self.write_record(entry.parent_depth, index, buf)?;
        }
        Ok(())
    }

    fn flush_record(&mut self, depth: u8) -> Result<Option<PackedRecord>, Error<D::Error>> {
        if let Some((buf, index)) = self.writers[usize::from(depth)].flush() {
            let buf = &mut core::mem::take(buf);
            self.write_record(depth, index, buf).map(Some)
        } else {
            Ok(None)
        }
    }

    fn write_record(
        &mut self,
        depth: u8,
        index: u64,
        buf: &mut Vec<u8>,
    ) -> Result<PackedRecord, Error<D::Error>> {
        assert!(
            buf.len() <= 1 << PITCH,
            "buffer exceeds maximum record size"
        );
        let mut compress_buf = Vec::new();
        let uncompressed_len = u32::try_from(buf.len()).expect("already checked buf.len()");
        let (data, compression_algorithm) = compress(buf, &mut compress_buf);
        assert!(
            data.len() <= 1 << PITCH,
            "data is guaranteed to be smaller than buf"
        );
        let compressed_len = u32::try_from(data.len()).expect("already checked data.len()");
        let tag = encrypt(&self.key, depth.into(), index, data);
        let offset = self.device.append(data).map_err(Error::Device)?;
        buf.clear();
        let entry = record::Entry {
            tag,
            offset,
            compression_algorithm,
            compressed_len,
            uncompressed_len,
        };
        Ok(PackedRecord {
            entry,
            parent_depth: depth + 1,
        })
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
}

/// # Returns
///
/// A buffer that is guaranteed to be no larger than `data`.
fn compress<'a>(data: &'a mut [u8], buf: &'a mut Vec<u8>) -> (&'a mut [u8], CompressionAlgorithm) {
    let end = data
        .iter()
        .enumerate()
        .rev()
        .find(|x| *x.1 != 0)
        .map_or(0, |x| x.0 + 1);
    let data = &mut data[..end];
    match compress_zstd(data, buf) {
        true => (buf, CompressionAlgorithm::Zstd),
        false => (data, CompressionAlgorithm::None),
    }
}

fn encrypt(key: &Key, depth: u32, index: u64, data: &mut [u8]) -> Tag {
    if data.is_empty() {
        return Tag::default();
    }
    let nonce = crate::record_nonce(depth, index);
    let tag = ChaCha12Poly1305::new(key)
        .encrypt_in_place_detached(&nonce, &[], data)
        .expect("failed to encrypt data");
    tag
}

/// # Returns
///
/// `true` if the data got successfully compressed and is less than the original length.
fn compress_zstd<'a>(data: &'a mut [u8], buf: &'a mut Vec<u8>) -> bool {
    // TODO make compression level configurable
    buf.clear();
    buf.resize(data.len(), 0);
    let len = zstd_safe::compress(&mut **buf, data, zstd_safe::CompressionLevel::MAX)
        .unwrap_or(usize::MAX);
    if len >= data.len() {
        return false;
    }
    // if only Vec had a separate shrink method...
    buf.resize_with(len, || unreachable!());
    true
}
