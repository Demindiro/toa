#![forbid(unused_must_use)]

macro_rules! trace {
    ($($x:tt)*) => {};
    ($($x:tt)*) => {{
        eprint!("[TRACE] ");
        eprintln!($($x)*)
    }};
}

pub mod log;

pub use toa_blob_store::DuplicateBlob;

use bitvec::boxed::BitBox;
#[cfg(feature = "std")]
use std::os::unix::fs::FileExt;
use std::{
    cell::RefCell,
    collections::btree_map::{BTreeMap, Entry},
    error, fmt, io,
    num::NonZeroU32,
    ops,
    rc::Rc,
};

const MAX_BLOB_ID: BlobId = BlobId(999_999);

pub trait ZoneDev {
    /// # Note
    ///
    /// `offset` is in *bytes*.
    ///
    /// The device is expected to handle unaligned reads transparently.
    /// A slower path to handle this case is allowed.
    ///
    /// # Panics
    ///
    /// This method should panic if the offset + buffer length exceeds
    /// the write pointer for this zone. This is not a requirement: if
    /// the device does not track write pointers it is not necessary.
    fn read_at(&self, zone: u32, offset: u64, buf: &mut [u8]) -> io::Result<()>;

    /// # Note
    ///
    /// `offset` is in *bytes*.
    ///
    /// # Panics
    ///
    /// This method should panic if the data length is not a multiple
    /// of the block size, as it is a severe logic error.
    ///
    /// Similarly, this method should panic if the offset does not match
    /// the current zone head.
    fn append<'a>(&'a self, zone: u32, offset: u64, data: &[u8]) -> io::Result<()>;

    /// Wipe a zone, resetting the write pointer to 0.
    fn reset(&self, zone: u32) -> io::Result<()>;

    /// Wipe multiple zones, resetting the write pointer of each to 0.
    fn reset_many(&self, zones: &[u32]) -> io::Result<()> {
        zones.iter().try_for_each(|x| self.reset(*x))
    }

    /// The current write pointer of a zone.
    ///
    /// This may be `None` if the underlying device is not zoned (e.g. CMR HDD).
    /// In such a case it is assumed any block is arbitrarily writeable.
    fn zone_write_head(&self, zone: u32) -> io::Result<Option<u64>>;

    fn block_size(&self) -> BlockShift;
    fn zone_blocks(&self) -> u32;
    fn zone_count(&self) -> NonZeroU32;

    /// Wipe all zones. This may be a noop, but zones must be writeable
    /// from the start after this call.
    fn clear(&mut self) -> io::Result<()>;
}

pub struct BlobStore<U> {
    zone_dev: U,
    data: RefCell<BlobStoreData>,
}

struct BlobStoreData {
    generation: u64,
    blobs: BlobTable,
    blob_map: BTreeMap<Rc<[u8]>, BlobId>,
    log: Vec<u8>,
    log_zone_a: ZoneId,
    log_zone_b: ZoneId,
    /// Write pointer of the current log zone.
    log_zone_head: u64,
    /// Total size of the log in bytes.
    log_len: u64,
    allocated_zones: BitBox,
    transaction_counter: usize,
}

pub struct MemZones<const B: usize> {
    zones: RefCell<Box<[Vec<[u8; B]>]>>,
    zone_size: u32,
}

pub struct MemBlocks {
    blocks: RefCell<Box<[u8]>>,
    block_size: BlockShift,
    zone_blocks: u32,
}

#[cfg(feature = "std")]
pub struct FileBlocks {
    file: std::fs::File,
    block_size: BlockShift,
    zone_blocks: u32,
    zone_count: u32,
}

#[derive(Clone, Copy)]
pub enum BlockShift {
    N9 = 1 << 9,
    N12 = 1 << 12,
}

pub struct BlobRef<'a, T> {
    store: &'a T,
    id: BlobId,
}

pub struct Header {
    pub block_size: u32,
    pub zone_blocks: u32,
    pub zone_count: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobId(u32);

#[derive(Debug)]
pub struct OutOfZones;

#[derive(Debug)]
pub struct DataSmallerThanZone;

/// # Note about zone data alignment
///
/// Data is *not* aligned to block boundaries.
/// This is to maximize compression density and simplify the interface.
///
/// To ensure blocks are written as a whole there is a second tail buffer,
/// which is appended to until it is block-sized.
struct Blob {
    name: Rc<[u8]>,
    /// `None` if this blob is unzoned.
    zones: Option<Vec<ZoneId>>,
    tail: Vec<u8>,
    len: u64,
    flushed: usize,
}

#[derive(Default)]
struct BlobTable {
    table: Vec<Option<Blob>>,
}

#[derive(Clone, Copy, bytemuck::Zeroable, bytemuck::Pod)]
#[repr(transparent)]
pub struct ZoneId(pub u32);

#[derive(Debug)]
struct NoBlobWithId(BlobId);

#[derive(Debug)]
enum BlobInsertError {
    DuplicateBlob(BlobId),
    BlobIdOutOfRange(BlobId),
}

#[derive(Debug)]
struct ZoneIdOutOfRange(ZoneId);

#[derive(Debug)]
enum AddZoneToBlobError {
    NoBlobWithId(BlobId),
    ZoneIdOutOfRange(ZoneId),
    IsUnzonedBlob,
}

impl<U> BlobStore<U>
where
    U: ZoneDev,
{
    pub fn init(mut zone_dev: U) -> io::Result<Self> {
        let generation = 1;
        zone_dev.clear()?;
        let nr_zones = zone_dev.zone_count();

        let hdr = log::entry::Header {
            magic: log::entry::Header::MAGIC,
            version: log::entry::Header::VERSION.into(),
            generation: generation.into(),
            block_size: u32::from(zone_dev.block_size()).into(),
            zone_blocks: zone_dev.zone_blocks().into(),
            zone_count: zone_dev.zone_count().get().into(),
            _pad_0: Default::default(),
        };
        let data = BlobStoreData::new(generation, nr_zones);

        let s = Self {
            zone_dev,
            data: data.into(),
        };
        let mut sd = s.data.borrow_mut();
        s.log_push(&mut sd, &[bytemuck::bytes_of(&hdr)])?;
        s.log_flush(&mut sd)?;
        drop(sd);
        Ok(s)
    }

    pub fn load(zone_dev: U) -> io::Result<Self> {
        let mut store = BlobStoreData::new(0, zone_dev.zone_count());

        let mut in_transaction = false;

        let log_end = log::iter_with(&zone_dev, |entry| {
            match entry {
                log::LogEntry::CreateBlob { id, name, unzoned } => {
                    store
                        .replay_create_blob(id, name, unzoned)
                        .map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))?;
                }
                log::LogEntry::ClearBlob { id } => store.replay_clear_blob(id)?,
                log::LogEntry::DeleteBlob { id } => store.replay_delete_blob(id)?,
                log::LogEntry::RenameBlob { id, name } => {
                    store.replay_rename_blob(id, name)?;
                }
                log::LogEntry::AppendBlobTail { id, data } => store.replay_append_blob(id, data)?,
                log::LogEntry::AddZoneToBlob { id, zone } => {
                    store.replay_add_zone_to_blob(id, zone)?
                }
                log::LogEntry::CommitBlobTail { id, len } => store.replay_commit_blob(id, len)?,
                log::LogEntry::NextLogZone { zones } => {
                    [store.log_zone_a, store.log_zone_b] = zones;
                    store.mark_zone_allocated(store.log_zone_a)?;
                    store.mark_zone_allocated(store.log_zone_b)?;
                }
                log::LogEntry::TransactionBegin => {
                    if in_transaction {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "transaction_begin while already inside transaction",
                        ));
                    }
                    in_transaction = true;
                }
                log::LogEntry::TransactionEnd => {
                    if !in_transaction {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "transaction_end outside of transaction",
                        ));
                    }
                    in_transaction = false;
                }
            }
            Ok(())
        })?;

        if in_transaction {
            // TODO should we attempt rollback? or leave that to fsck?
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unterminated transaction",
            ));
        }

        log::LogEnd {
            generation: store.generation,
            len: store.log_len,
            zone_head: store.log_zone_head,
        } = log_end;

        Ok(BlobStore {
            zone_dev,
            data: store.into(),
        })
    }

    pub fn flush(&self) -> io::Result<()> {
        self.flush_s(&mut self.data.borrow_mut())
    }

    fn flush_s(&self, s: &mut BlobStoreData) -> io::Result<()> {
        let blob_num = s.blobs.table.len() as u32;
        for id in (0..blob_num).map(BlobId) {
            if s.blobs.get(id).is_some() {
                self.flush_blob(s, id)?;
            }
        }
        self.log_flush(s)?;
        Ok(())
    }

    pub fn unmount(self) -> Result<U, (Self, io::Error)> {
        if let Err(e) = self.flush() {
            return Err((self, e));
        }
        Ok(self.zone_dev)
    }

    pub fn transaction<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce() -> io::Result<R>,
    {
        self.transaction_begin(&mut self.data.borrow_mut())?;
        let ret = (f)()?; // TODO good idea or nah?
        self.transaction_end(&mut self.data.borrow_mut())?;
        Ok(ret)
    }

    pub fn blob(&self, id: BlobId) -> BlobRef<'_, Self> {
        BlobRef { store: self, id }
    }

    pub fn find(&self, name: &[u8]) -> io::Result<Option<BlobRef<'_, Self>>> {
        assert!(name.len() <= 255, "name too long");
        match self.data.borrow().blob_map.get(name) {
            None => Ok(None),
            Some(id) => Ok(Some(self.blob(*id))),
        }
    }

    pub fn create_blob<'a>(
        &'a self,
        name: &[u8],
    ) -> io::Result<Result<BlobRef<'a, Self>, DuplicateBlob>> {
        self.create_blob_conf(name, false)
    }

    pub fn create_unzoned_blob<'a>(
        &'a self,
        name: &[u8],
    ) -> io::Result<Result<BlobRef<'a, Self>, DuplicateBlob>> {
        self.create_blob_conf(name, true)
    }

    fn transaction_begin(&self, data: &mut BlobStoreData) -> io::Result<()> {
        if data.transaction_counter == 0 {
            self.log_transaction_begin(data)?;
        }
        data.transaction_counter += 1;
        Ok(())
    }

    fn transaction_end(&self, data: &mut BlobStoreData) -> io::Result<()> {
        data.transaction_counter -= 1;
        if data.transaction_counter == 0 {
            // TODO should we make a distinction between
            // - transaction as just an atomic unit
            // - transaction as atomic unit *and* persisted to disk
            // ?
            // at minimum, we need to fix blob append so it gets log entries before transaction end
            self.flush_s(data)?;
            self.log_transaction_end(data)?;
        }
        Ok(())
    }

    fn create_blob_conf<'a>(
        &'a self,
        name: &[u8],
        unzoned: bool,
    ) -> io::Result<Result<BlobRef<'a, Self>, DuplicateBlob>> {
        assert!(name.len() <= 255, "name too long");
        let s = &mut *self.data.borrow_mut();
        match s.blob_map.entry(name.into()) {
            Entry::Occupied(_) => Ok(Err(DuplicateBlob)),
            Entry::Vacant(e) => {
                let id = s.blobs.insert(Blob::new(e.key().clone(), unzoned));
                e.insert(id);
                self.log_create_blob(s, id, name, unzoned)
                    .map(|()| Ok(BlobRef { store: self, id }))
            }
        }
    }

    pub fn size_on_disk(&self) -> io::Result<u64> {
        let s = self.data.borrow();
        let mut n = s.log_len;
        for (_, x) in s.blobs.iter() {
            n += x.len;
        }
        Ok(n)
    }

    fn flush_blob<'a>(&'a self, s: &mut BlobStoreData, id: BlobId) -> io::Result<()> {
        while s.blobs[id].flushed < s.blobs[id].tail.len() {
            if s.log_free(self.zone_dev.block_size()) == 0 {
                self.log_flush(s)?;
            }
            let start = s.blobs[id].flushed;
            let end = start + s.log_free(self.zone_dev.block_size()) - 8;
            let tail = core::mem::take(&mut s.blobs[id].tail);
            let end = end.min(tail.len());
            let res = self.log_append_blob_tail(s, id, &tail[start..end]);
            s.blobs[id].tail = tail;
            res?;
            s.blobs[id].flushed = end;
        }
        Ok(())
    }

    fn log_create_blob(
        &self,
        s: &mut BlobStoreData,
        id: BlobId,
        name: &[u8],
        unzoned: bool,
    ) -> io::Result<()> {
        let hdr = log::entry::CreateBlob {
            ty: match unzoned {
                false => log::entry::ty::CREATE_BLOB,
                true => log::entry::ty::CREATE_UNZONED_BLOB,
            },
            name_len: u8::try_from(name.len()).unwrap().into(),
            _pad_0: Default::default(),
            blob_id: id.0.into(),
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr), name])
    }

    fn log_clear_blob(&self, s: &mut BlobStoreData, id: BlobId) -> io::Result<()> {
        let hdr = log::entry::ClearBlob {
            ty: log::entry::ty::CLEAR_BLOB,
            _pad_0: Default::default(),
            blob_id: id.0.into(),
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr)])
    }

    fn log_delete_blob(&self, s: &mut BlobStoreData, id: BlobId) -> io::Result<()> {
        let hdr = log::entry::DeleteBlob {
            ty: log::entry::ty::DELETE_BLOB,
            _pad_0: Default::default(),
            blob_id: id.0.into(),
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr)])
    }

    fn log_rename_blob(&self, s: &mut BlobStoreData, id: BlobId, name: &[u8]) -> io::Result<()> {
        let hdr = log::entry::RenameBlob {
            ty: log::entry::ty::RENAME_BLOB,
            name_len: u8::try_from(name.len()).unwrap().into(),
            _pad_0: Default::default(),
            blob_id: id.0.into(),
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr), name])
    }

    fn log_append_blob_tail(
        &self,
        s: &mut BlobStoreData,
        id: BlobId,
        data: &[u8],
    ) -> io::Result<()> {
        let len = u16::try_from(data.len()).unwrap(); // FIXME pre-split data
        let hdr = log::entry::AppendBlobTail {
            ty: log::entry::ty::APPEND_BLOB_TAIL,
            _pad_0: Default::default(),
            data_len: len.into(),
            blob_id: id.0.into(),
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr), data])
    }

    fn log_add_zone_to_blob(
        &self,
        s: &mut BlobStoreData,
        id: BlobId,
        zone_id: ZoneId,
    ) -> io::Result<()> {
        let hdr = log::entry::AddZoneToBlob {
            ty: log::entry::ty::ADD_ZONE_TO_BLOB,
            _pad_0: Default::default(),
            _pad_1: Default::default(),
            blob_id: id.0.into(),
            zone_id: zone_id.0.into(),
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr)])
    }

    fn log_commit_blob_tail(&self, s: &mut BlobStoreData, id: BlobId, len: u64) -> io::Result<()> {
        let hdr = log::entry::CommitBlobTail {
            ty: log::entry::ty::COMMIT_BLOB_TAIL,
            _pad_0: Default::default(),
            blob_id: id.0.into(),
            len: len.into(),
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr)])
    }

    fn log_transaction_begin(&self, s: &mut BlobStoreData) -> io::Result<()> {
        let hdr = log::entry::TransactionBegin {
            ty: log::entry::ty::TRANSACTION_BEGIN,
            _pad_0: [0; 7],
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr)])
    }

    fn log_transaction_end(&self, s: &mut BlobStoreData) -> io::Result<()> {
        let hdr = log::entry::TransactionEnd {
            ty: log::entry::ty::TRANSACTION_END,
            _pad_0: [0; 7],
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr)])
    }

    fn log_push(&self, s: &mut BlobStoreData, data: &[&[u8]]) -> io::Result<()> {
        let len = data.iter().fold(0, |s, x| s + x.len());
        self.log_reserve(s, len)?;
        assert!(
            u64::try_from(len).expect("usize <= u64") < self.log_remaining(s),
            "log entry too large (log size: {len}, remaining: {})",
            self.log_remaining(s),
        );
        s.log.extend(data.iter().copied().flatten());
        s.log_pad();
        Ok(())
    }

    fn log_reserve(&self, s: &mut BlobStoreData, num: usize) -> io::Result<()> {
        let num = (num + 7) & !7;
        let len = (s.log.len() + num) as u64;
        if len > u64::from(self.zone_dev.block_size()) {
            self.log_flush(s)?;
        }
        Ok(())
    }

    fn log_flush<'a>(&'a self, data: &mut BlobStoreData) -> io::Result<()> {
        if data.log.is_empty() {
            return Ok(());
        }
        let block_size = usize::from(self.zone_dev.block_size());

        assert!(
            data.log.len() <= block_size,
            "{} <= {}",
            data.log.len(),
            block_size
        );
        // TODO optimize with long NOPs
        data.log.resize(block_size, 0);
        self.zone_dev
            .append(data.log_zone_a.0, data.log_zone_head, &data.log)?;
        self.zone_dev
            .append(data.log_zone_b.0, data.log_zone_head, &data.log)?;
        data.log_zone_head += block_size as u64;
        data.log.clear();

        // allocate a new zone if we nearly exhausted the current one
        let rem = self.log_remaining(data);

        if rem <= block_size as u64 {
            // TODO don't panic
            // TODO spread zones to improve resilience
            let [new_a, new_b] = data.alloc_zones_array().unwrap();
            for (log_zone, new) in [(data.log_zone_a, new_a), (data.log_zone_b, new_b)] {
                let e = log::entry::NextLogZone {
                    ty: log::entry::ty::NEXT_LOG_ZONE,
                    _pad_0: [0; 3],
                    zone_id: new.0.into(),
                };
                data.log.extend(bytemuck::bytes_of(&e));
                data.log.resize(block_size, 0);
                self.zone_dev
                    .append(log_zone.0, data.log_zone_head, &data.log)?;
                data.log.clear();
            }
            data.log_zone_a = new_a;
            data.log_zone_b = new_b;
            data.log_zone_head = 0;
        }

        data.log_len += block_size as u64;

        self.log_terminate(data)
    }

    fn log_terminate(&self, s: &mut BlobStoreData) -> io::Result<()> {
        let block_size = usize::from(self.zone_dev.block_size());
        let f = |x: ZoneId| self.zone_dev.zone_write_head(x.0);
        match [f(s.log_zone_a)?, f(s.log_zone_b)?] {
            [Some(_), Some(_)] => {}
            [None, None] => {
                // write a block of all zeros to ensure we can detect end-of-log.
                s.log.resize(block_size, 0);
                self.zone_dev
                    .append(s.log_zone_a.0, s.log_zone_head, &s.log)?;
                self.zone_dev
                    .append(s.log_zone_b.0, s.log_zone_head, &s.log)?;
                s.log.clear();
            }
            [_, _] => unreachable!("ZoneDev cannot mix zoned and unzoned regions"),
        }
        Ok(())
    }

    fn log_remaining(&self, s: &mut BlobStoreData) -> u64 {
        let block_size = usize::from(self.zone_dev.block_size());
        let zone_blocks = u64::from(self.zone_dev.zone_blocks());
        let zone_size = zone_blocks * block_size as u64;
        zone_size.checked_sub(s.log_zone_head).unwrap_or_else(|| {
            unreachable!(
                "log_zone_head should not exceed zone_size (log_zone_head: {}, zone_size: {})",
                s.log_zone_head, zone_size
            )
        })
    }
}

impl BlobStoreData {
    fn new(generation: u64, nr_zones: NonZeroU32) -> Self {
        let mut s = Self {
            generation,
            blobs: Default::default(),
            blob_map: Default::default(),
            log: Vec::new(),
            log_zone_a: ZoneId(0),
            log_zone_b: ZoneId(nr_zones.get() - 1),
            log_zone_head: 0,
            log_len: 0,
            allocated_zones: bitvec::bitbox![0; nr_zones.get() as usize],
            transaction_counter: 0,
        };
        s.allocated_zones.set(s.log_zone_a.0 as usize, true);
        s.allocated_zones.set(s.log_zone_b.0 as usize, true);
        s
    }

    /// # Note
    ///
    /// To minimize the risk of data loss, resetting zones should *only*
    /// be done when *releasing* zones, i.e. during log rewrite or blob delete.
    /// This increases the risk of a panic if a zone isn't empty as expected,
    /// but helps with catching double allocations or other issues.
    fn alloc_zones(&mut self, buf: &mut [ZoneId]) -> Result<(), OutOfZones> {
        let mut bits = 0..self.allocated_zones.len();
        'slots: for (k, slot) in buf.iter_mut().enumerate() {
            while let Some(i) = bits.next() {
                if !self.allocated_zones[i] {
                    // false = free
                    *slot = ZoneId(i as u32);
                    self.allocated_zones.set(i, true);
                    continue 'slots;
                }
            }
            // undo previous allocations
            for slot in buf[..k].iter() {
                self.allocated_zones.set(slot.0 as usize, false);
            }
            return Err(OutOfZones);
        }
        Ok(())
    }

    fn alloc_zones_array<const N: usize>(&mut self) -> Result<[ZoneId; N], OutOfZones> {
        let mut x = [const { ZoneId(0) }; N];
        self.alloc_zones(&mut x)?;
        Ok(x)
    }

    fn free_zones(&mut self, zones: &mut [ZoneId]) {
        // sort zones first so we access bits linearly
        // may or may not have a positive influence, should be benchmarked
        zones.sort_by_key(|x| x.0);
        for x in zones {
            self.allocated_zones.set(x.0 as usize, false);
        }
    }

    fn mark_zone_allocated(&mut self, id: ZoneId) -> Result<(), ZoneIdOutOfRange> {
        ((id.0 as usize) < self.allocated_zones.len())
            .then(|| self.allocated_zones.set(id.0 as usize, true))
            .ok_or(ZoneIdOutOfRange(id))
    }

    fn replay_create_blob(
        &mut self,
        id: BlobId,
        name: &[u8],
        unzoned: bool,
    ) -> Result<(), BlobInsertError> {
        assert!(name.len() <= 255, "name too long");
        match self.blob_map.entry(name.into()) {
            Entry::Occupied(_) => Err(BlobInsertError::DuplicateBlob(id)),
            Entry::Vacant(e) => {
                self.blobs
                    .insert_at(id, Blob::new(e.key().clone(), unzoned))?;
                e.insert(id);
                Ok(())
            }
        }
    }

    fn replay_clear_blob(&mut self, id: BlobId) -> Result<(), NoBlobWithId> {
        let blob = self.blobs.try_get_mut(id)?;
        blob.zones.as_mut().map(|x| x.clear());
        blob.tail.clear();
        blob.flushed = 0;
        blob.len = 0;
        Ok(())
    }

    fn replay_delete_blob(&mut self, id: BlobId) -> io::Result<()> {
        let old = self.blobs.remove(id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("old blob with ID {id} missing"),
            )
        })?;
        if let Some(mut old) = old.zones {
            self.free_zones(&mut old);
        }
        self.blob_map.remove(&old.name);
        Ok(())
    }

    /// # Returns
    ///
    /// `true` if the blob actually got renamed, `false` if the operation is a no-op.
    fn replay_rename_blob(
        &mut self,
        id: BlobId,
        new_name: &[u8],
    ) -> io::Result<(bool, Option<Blob>)> {
        let blob = self.blobs.try_get_mut(id)?;
        if &*blob.name == new_name {
            return Ok((false, None));
        }
        self.blob_map.remove(&*blob.name);
        let mut old = match self.blob_map.entry(new_name.into()) {
            Entry::Vacant(e) => {
                blob.name = e.key().clone();
                e.insert(id);
                None
            }
            Entry::Occupied(mut e) => {
                blob.name = e.key().clone();
                let old = *e.get();
                let old = self.blobs.remove(old).expect("old blob missing");
                e.insert(id);
                Some(old)
            }
        };
        if let Some(old) = old.as_mut().and_then(|x| x.zones.as_mut()) {
            self.free_zones(old);
        }
        Ok((true, old))
    }

    fn replay_append_blob(&mut self, id: BlobId, data: &[u8]) -> Result<(), NoBlobWithId> {
        let blob = self.blobs.try_get_mut(id)?;
        blob.tail.extend(data);
        blob.flushed += data.len();
        Ok(())
    }

    fn replay_add_zone_to_blob(
        &mut self,
        id: BlobId,
        zone: ZoneId,
    ) -> Result<(), AddZoneToBlobError> {
        self.blobs
            .try_get_mut(id)?
            .zones
            .as_mut()
            .ok_or(AddZoneToBlobError::IsUnzonedBlob)?
            .push(zone);
        self.mark_zone_allocated(zone)?;
        Ok(())
    }

    fn replay_commit_blob(&mut self, id: BlobId, len: u64) -> Result<(), NoBlobWithId> {
        let blob = self.blobs.try_get_mut(id)?;
        blob.tail.clear();
        blob.len = len;
        blob.flushed = 0;
        Ok(())
    }

    fn log_free(&self, block_size: BlockShift) -> usize {
        usize::from(block_size) - self.log.len()
    }

    fn log_pad(&mut self) {
        let n = self.log.len();
        let n = (n + 7) & !7;
        self.log.resize(n, 0);
    }
}

// It would be more appropriate to implement the check on BlobStore but
// Rust still doesn't have a sane, safe way to move out of Droppable types
impl Drop for BlobStoreData {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // avoid double panic
            return;
        }
        for (id, blob) in self.blobs.iter() {
            debug_assert!(blob.flushed <= blob.tail.len(), "flushed exceeds tail size");
            assert!(
                blob.flushed == blob.tail.len(),
                "unflushed data for blob {id} (did you forget to call BlobStore::flush()?)"
            );
        }
    }
}

impl<'a, T> BlobRef<'a, T> {
    /// # Note
    ///
    /// If this blob gets deleted, the returned ID will be stale and may
    /// eventually be reused for another blob.
    pub fn id(&self) -> BlobId {
        self.id
    }
}

impl<'a, U> BlobRef<'a, BlobStore<U>>
where
    U: ZoneDev,
{
    pub fn clear(&self) -> io::Result<()> {
        let s = &mut *self.store.data.borrow_mut();
        if let Some(zones) = s.blobs[self.id].zones.as_mut() {
            self.store
                .zone_dev
                .reset_many(bytemuck::cast_slice(zones))?;
        }
        s.replay_clear_blob(self.id)?;
        self.store.log_clear_blob(s, self.id)?;
        Ok(())
    }

    pub fn delete(self) -> io::Result<()> {
        let s = &mut *self.store.data.borrow_mut();
        if let Some(zones) = s.blobs[self.id].zones.as_ref() {
            self.store
                .zone_dev
                .reset_many(bytemuck::cast_slice(zones))?;
        }
        s.replay_delete_blob(self.id)?;
        self.store.log_delete_blob(s, self.id)?;
        Ok(())
    }

    /// # Returns
    ///
    /// Start offset of written data.
    pub fn append(&self, data: &[u8]) -> io::Result<u64> {
        let s = &mut *self.store.data.borrow_mut();
        let block_size = usize::from(self.store.zone_dev.block_size());
        let idx = self.id;
        let offt = s.blobs[idx].total_len();

        debug_assert!(
            s.blobs[idx].flushed <= s.blobs[idx].tail.len(),
            "flushed not reset properly"
        );

        if s.blobs[self.id].zones.is_none() {
            s.blobs[idx].tail.extend(data);
            return Ok(offt);
        }

        let n = s.blobs[idx].tail.len().wrapping_neg() % block_size;
        let n = n.min(data.len());
        let (head, data) = data.split_at(n);
        s.blobs[idx].tail.extend(head);

        let flush_tail = s.blobs[idx].tail.len() >= block_size;
        if flush_tail {
            self.store.transaction_begin(s)?;
            let tail = core::mem::take(&mut s.blobs[idx].tail);
            self.append_blocks(s, &tail)?;
            s.blobs[idx].tail = tail;
            s.blobs[idx].tail.clear();
            s.blobs[idx].flushed = 0;
        }
        let n = data.len() & !(block_size - 1);
        let (blocks, tail) = data.split_at(n);
        self.append_blocks(s, blocks)?;
        s.blobs[idx].tail.extend(tail);
        if flush_tail {
            self.store.transaction_end(s)?;
        }

        Ok(offt)
    }

    /// # Returns
    ///
    /// Start offset of written data.
    pub fn append_many(&self, data: &[&[u8]]) -> io::Result<u64> {
        let offt = self.len()?;
        for x in data {
            self.append(x)?;
        }
        Ok(offt)
    }

    pub fn flush(&self) -> io::Result<()> {
        let s = &mut *self.store.data.borrow_mut();
        self.store.flush_blob(s, self.id)?;
        Ok(())
    }

    pub fn rename(&self, new_name: &[u8]) -> io::Result<()> {
        let s = &mut *self.store.data.borrow_mut();
        let (renamed, old) = s.replay_rename_blob(self.id, new_name)?;
        if renamed {
            if let Some(old) = old.and_then(|x| x.zones) {
                self.store.zone_dev.reset_many(bytemuck::cast_slice(&old))?;
            }
            self.store.log_rename_blob(s, self.id, new_name)?;
        }
        Ok(())
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let s = self.store.data.borrow();
        let block_size = usize::from(self.store.zone_dev.block_size());
        let idx = self.id;

        if let Some(x) = offset.checked_sub(s.blobs[idx].len) {
            // all tail
            let x = usize::try_from(x)
                .ok()
                .and_then(|x| s.blobs[idx].tail.get(x..))
                .unwrap_or(&[]);
            let n = x.len().min(buf.len());
            buf[..n].copy_from_slice(&x[..n]);
            Ok(n)
        } else {
            let n = self.len()?.saturating_sub(offset);
            let n = usize::try_from(n).unwrap_or(usize::MAX).min(buf.len());
            let buf = &mut buf[..n];

            let n = s.blobs[idx].len.saturating_sub(offset);
            let n = usize::try_from(n).unwrap_or(usize::MAX).min(buf.len());
            let (mut zone_buf, tail_buf) = buf.split_at_mut(n);

            // do tail first
            let n = tail_buf.len().min(s.blobs[idx].tail.len());
            tail_buf[..n].copy_from_slice(&s.blobs[idx].tail[..n]);

            // the buffer may span multiple zones, so translate zone -> block -> byte
            // account for offset/block misalignment
            let zone_blocks = u64::from(self.store.zone_dev.zone_blocks());
            // TODO this does require a proper division, which is slow.
            // zone_blocks is constant however, so we could precalculate the reciprocal,
            // then just multiply which is fast.
            let zone_size = u64::from(zone_blocks) * block_size as u64;
            let (mut zone, mut offt) = (offset / zone_size, offset % zone_size);

            while !zone_buf.is_empty() {
                let n = zone_buf.len().min((zone_size - offt) as usize);
                self.store.zone_dev.read_at(
                    s.blobs[idx].zones.as_ref().expect("unzoned")[zone as usize].0,
                    offt,
                    &mut zone_buf[..n],
                )?;
                zone_buf = &mut zone_buf[n..];
                zone += 1;
                offt = 0;
            }
            Ok(buf.len())
        }
    }

    pub fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.len()?
            .checked_sub(offset)
            .and_then(|x| x.checked_sub(buf.len() as u64))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offset + buffer length exceeds blob length",
                )
            })?;
        let n = self.read_at(offset, buf)?;
        debug_assert_eq!(n, buf.len());
        Ok(())
    }

    pub fn read_array_at<const N: usize>(&self, offset: u64) -> io::Result<[u8; N]> {
        let mut buf = [0; N];
        self.read_exact_at(offset, &mut buf)?;
        Ok(buf)
    }

    pub fn len(&self) -> io::Result<u64> {
        Ok(self.store.data.borrow().blobs[self.id].total_len())
    }

    fn append_blocks(&self, s: &mut BlobStoreData, mut blocks: &[u8]) -> io::Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let block_size = usize::from(self.store.zone_dev.block_size());
        let zone_blocks = u64::from(self.store.zone_dev.zone_blocks());
        let zone_size = zone_blocks * block_size as u64;

        debug_assert_eq!(
            blocks.len() % block_size,
            0,
            "blocks len is not a multiple of block size"
        );

        let start = s.blobs[self.id].len;
        let end = start + blocks.len() as u64;

        let mut offset = start % zone_size;

        while !blocks.is_empty() {
            let mut zone;
            match s.blobs[self.id].zones.as_ref().expect("unzoned").last() {
                None => {
                    [zone] = s.alloc_zones_array().unwrap(); // TODO don't panic
                    self.store.log_add_zone_to_blob(s, self.id, zone)?;
                    s.replay_add_zone_to_blob(self.id, zone)?;
                }
                Some(z) => zone = *z,
            };
            let n = s.blobs[self.id].zones_capacity(zone_size);
            if n == s.blobs[self.id].len {
                [zone] = s.alloc_zones_array().unwrap(); // TODO don't panic
                self.store.log_add_zone_to_blob(s, self.id, zone)?;
                s.replay_add_zone_to_blob(self.id, zone)?;
            }
            let n = zone_size - offset;
            let n = n.min(blocks.len() as u64) as usize;
            self.store.zone_dev.append(zone.0, offset, &blocks[..n])?;
            blocks = &blocks[n..];
            offset = 0;
            s.blobs[self.id].len += n as u64;
        }
        // TODO delay commit until explicit flush
        s.replay_commit_blob(self.id, end)?;
        self.store.log_commit_blob_tail(s, self.id, end)?;
        Ok(())
    }
}

impl BlobTable {
    fn get(&self, id: BlobId) -> Option<&Blob> {
        self.table.get(id.0 as usize).and_then(|x| x.as_ref())
    }

    fn get_mut(&mut self, id: BlobId) -> Option<&mut Blob> {
        self.table.get_mut(id.0 as usize).and_then(|x| x.as_mut())
    }

    fn try_get(&self, id: BlobId) -> Result<&Blob, NoBlobWithId> {
        self.get(id).ok_or(NoBlobWithId(id))
    }

    fn try_get_mut(&mut self, id: BlobId) -> Result<&mut Blob, NoBlobWithId> {
        self.get_mut(id).ok_or(NoBlobWithId(id))
    }

    fn insert(&mut self, blob: Blob) -> BlobId {
        for (i, x) in self.table.iter_mut().enumerate() {
            if x.is_none() {
                *x = Some(blob);
                return BlobId(i as u32);
            }
        }
        if self.table.len() >= u32::MAX as usize {
            todo!("too many blobs");
        }
        self.table.push(Some(blob));
        BlobId((self.table.len() - 1) as u32)
    }

    fn insert_at(&mut self, id: BlobId, blob: Blob) -> Result<(), BlobInsertError> {
        if id > MAX_BLOB_ID {
            return Err(BlobInsertError::BlobIdOutOfRange(id));
        }
        let n = self.table.len().max(id.0 as usize + 1);
        self.table.resize_with(n, || None);
        let x = &mut self.table[id.0 as usize];
        if x.is_some() {
            return Err(BlobInsertError::DuplicateBlob(id));
        }
        *x = Some(blob);
        Ok(())
    }

    fn remove(&mut self, id: BlobId) -> Option<Blob> {
        self.table.get_mut(id.0 as usize).and_then(|x| x.take())
    }

    fn iter(&self) -> impl Iterator<Item = (BlobId, &Blob)> {
        self.table
            .iter()
            .enumerate()
            .flat_map(|(i, x)| x.as_ref().map(|x| (BlobId(i as u32), x)))
    }
}

impl ops::Index<BlobId> for BlobTable {
    type Output = Blob;

    #[track_caller]
    fn index(&self, id: BlobId) -> &Self::Output {
        match self.try_get(id) {
            Ok(x) => x,
            Err(e) => panic!("{e}"),
        }
    }
}

impl ops::IndexMut<BlobId> for BlobTable {
    #[track_caller]
    fn index_mut(&mut self, id: BlobId) -> &mut Self::Output {
        match self.try_get_mut(id) {
            Ok(x) => x,
            Err(e) => panic!("{e}"),
        }
    }
}

impl<const B: usize> ZoneDev for MemZones<B> {
    #[track_caller]
    fn read_at(&self, zone: u32, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let x = self.zones.borrow();
        let x = x[zone as usize].as_flattened();
        let start = usize::try_from(offset).expect("offset out of bounds");
        let end = start.checked_add(buf.len()).expect("offset out of bounds");
        buf.copy_from_slice(&x[start..end]);
        Ok(())
    }

    #[track_caller]
    fn append<'a>(&'a self, zone: u32, offset: u64, data: &[u8]) -> io::Result<()> {
        let (data, []) = data.as_chunks() else {
            panic!("data len is not a multiple of the block size")
        };
        let x = &mut *self.zones.borrow_mut();
        let x = &mut x[zone as usize];
        let o = (x.len() * B) as u64;
        assert!(
            o == offset,
            "offset does not match write pointer (expect: {o}, got: {offset})"
        );
        if x.len() + data.len() > self.zone_size as usize {
            panic!("zone overflow");
        }
        x.extend(data);
        Ok(())
    }

    fn reset(&self, zone: u32) -> io::Result<()> {
        self.zones.borrow_mut()[zone as usize].clear();
        Ok(())
    }

    fn zone_write_head(&self, zone: u32) -> io::Result<Option<u64>> {
        Ok(Some((self.zones.borrow()[zone as usize].len() * B) as u64))
    }

    fn block_size(&self) -> BlockShift {
        match B {
            512 => BlockShift::N9,
            4096 => BlockShift::N12,
            _ => todo!(),
        }
    }
    fn zone_blocks(&self) -> u32 {
        self.zone_size
    }
    fn zone_count(&self) -> NonZeroU32 {
        let n = self.zones.borrow().len();
        u32::try_from(n).unwrap().try_into().unwrap()
    }

    fn clear(&mut self) -> io::Result<()> {
        self.zones.borrow_mut().iter_mut().for_each(|x| x.clear());
        Ok(())
    }
}

impl<const B: usize> MemZones<B> {
    const _B_IS_POWER_OF_2: () = assert!(B.count_ones() == 1);

    pub fn new(zone_size: u32, zone_count: u32) -> Self {
        Self {
            zones: RefCell::new(vec![vec![]; zone_count as usize].into()),
            zone_size,
        }
    }
}

impl MemBlocks {
    pub fn new(block_size: BlockShift, zone_blocks: u32, zone_count: u32) -> Self {
        let n = zone_count as usize * zone_blocks as usize * usize::from(block_size);
        Self::wrap(block_size, zone_blocks, vec![0; n].into()).expect("data large enough")
    }

    pub fn wrap(
        block_size: BlockShift,
        zone_blocks: u32,
        data: Box<[u8]>,
    ) -> Result<Self, DataSmallerThanZone> {
        if (data.len() as u64) < u64::from(zone_blocks) * u64::from(block_size) {
            return Err(DataSmallerThanZone);
        }
        Ok(Self {
            blocks: RefCell::new(data),
            block_size,
            zone_blocks,
        })
    }

    fn zone_size(&self) -> u64 {
        u64::from(self.zone_blocks) * u64::from(self.block_size)
    }

    #[track_caller]
    fn translate(&self, zone: u32, offset: u64) -> usize {
        let offset = u128::from(zone) * u128::from(self.zone_size()) + u128::from(offset);
        usize::try_from(offset).expect("offset out of bounds")
    }
}

impl ZoneDev for MemBlocks {
    #[track_caller]
    fn read_at(&self, zone: u32, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let start = self.translate(zone, offset);
        let end = start.checked_add(buf.len()).expect("offset out of bounds");
        let x = self.blocks.borrow();
        buf.copy_from_slice(&x[start..end]);
        Ok(())
    }

    #[track_caller]
    fn append<'a>(&'a self, zone: u32, offset: u64, data: &[u8]) -> io::Result<()> {
        assert!(
            data.len() % usize::from(self.block_size) == 0,
            "data len is not a multiple of the block size"
        );
        assert!(
            offset % u64::from(self.block_size) == 0,
            "offset is not aligned"
        );
        let start = self.translate(zone, offset);
        let end = start + data.len();
        self.blocks.borrow_mut()[start..end].copy_from_slice(data);
        Ok(())
    }

    fn reset(&self, _zone: u32) -> io::Result<()> {
        Ok(())
    }

    fn zone_write_head(&self, _zone: u32) -> io::Result<Option<u64>> {
        Ok(None)
    }

    fn block_size(&self) -> BlockShift {
        self.block_size
    }
    fn zone_blocks(&self) -> u32 {
        self.zone_blocks
    }
    fn zone_count(&self) -> NonZeroU32 {
        let n = self.blocks.borrow().len() / self.zone_size() as usize;
        u32::try_from(n).unwrap().try_into().unwrap()
    }

    fn clear(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "std")]
impl FileBlocks {
    pub fn new(
        block_size: BlockShift,
        zone_blocks: u32,
        zone_count: u32,
        file: std::fs::File,
    ) -> io::Result<Self> {
        let n = u64::from(zone_count) * u64::from(zone_blocks) * u64::from(block_size);
        file.set_len(n)?;
        Ok(Self::wrap(block_size, zone_blocks, zone_count, file))
    }

    pub fn wrap(
        block_size: BlockShift,
        zone_blocks: u32,
        zone_count: u32,
        file: std::fs::File,
    ) -> Self {
        Self {
            file,
            block_size,
            zone_blocks,
            zone_count,
        }
    }

    fn zone_size(&self) -> u64 {
        u64::from(self.zone_blocks) * u64::from(self.block_size)
    }

    #[track_caller]
    fn translate(&self, zone: u32, offset: u64) -> u64 {
        let offset = u128::from(zone) * u128::from(self.zone_size()) + u128::from(offset);
        u64::try_from(offset).expect("offset out of bounds")
    }
}

#[cfg(feature = "std")]
impl ZoneDev for FileBlocks {
    fn read_at(&self, zone: u32, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let start = self.translate(zone, offset);
        self.file.read_exact_at(buf, start)?;
        Ok(())
    }

    fn append<'a>(&'a self, zone: u32, offset: u64, data: &[u8]) -> io::Result<()> {
        let start = self.translate(zone, offset);
        self.file.write_all_at(data, start)?;
        Ok(())
    }

    fn reset(&self, _zone: u32) -> io::Result<()> {
        Ok(())
    }

    fn zone_write_head(&self, _zone: u32) -> io::Result<Option<u64>> {
        Ok(None)
    }

    fn block_size(&self) -> BlockShift {
        self.block_size
    }
    fn zone_blocks(&self) -> u32 {
        self.zone_blocks
    }
    fn zone_count(&self) -> NonZeroU32 {
        self.zone_count.try_into().unwrap()
    }

    fn clear(&mut self) -> io::Result<()> {
        Ok(())
    }
}

macro_rules! proxy_zonedev {
    ($ty:ty) => {
        impl ZoneDev for $ty {
            #[track_caller]
            fn read_at(&self, zone: u32, offset: u64, buf: &mut [u8]) -> io::Result<()> {
                (&**self).read_at(zone, offset, buf)
            }

            #[track_caller]
            fn append<'a>(&'a self, zone: u32, offset: u64, data: &[u8]) -> io::Result<()> {
                (&**self).append(zone, offset, data)
            }

            #[track_caller]
            fn reset(&self, zone: u32) -> io::Result<()> {
                (&**self).reset(zone)
            }
            #[track_caller]
            fn reset_many(&self, zones: &[u32]) -> io::Result<()> {
                (&**self).reset_many(zones)
            }

            #[track_caller]
            fn zone_write_head(&self, zone: u32) -> io::Result<Option<u64>> {
                (&**self).zone_write_head(zone)
            }

            #[track_caller]
            fn block_size(&self) -> BlockShift {
                (&**self).block_size()
            }
            #[track_caller]
            fn zone_blocks(&self) -> u32 {
                (&**self).zone_blocks()
            }
            #[track_caller]
            fn zone_count(&self) -> NonZeroU32 {
                (&**self).zone_count()
            }

            #[track_caller]
            fn clear(&mut self) -> io::Result<()> {
                (&mut **self).clear()
            }
        }
    };
}

proxy_zonedev!(Box<dyn ZoneDev>);
proxy_zonedev!(&mut dyn ZoneDev);

impl From<BlockShift> for u32 {
    fn from(x: BlockShift) -> u32 {
        match x {
            BlockShift::N9 => 1 << 9,
            BlockShift::N12 => 1 << 12,
        }
    }
}

impl From<BlockShift> for u64 {
    fn from(x: BlockShift) -> u64 {
        u32::from(x).into()
    }
}

impl From<BlockShift> for usize {
    fn from(x: BlockShift) -> usize {
        u32::from(x) as usize
    }
}

impl Blob {
    fn new(name: Rc<[u8]>, unzoned: bool) -> Self {
        Self {
            name,
            zones: (!unzoned).then(Vec::new),
            tail: Vec::new(),
            len: 0,
            flushed: 0,
        }
    }

    fn total_len(&self) -> u64 {
        self.len + self.tail.len() as u64
    }

    fn zones_capacity(&self, zone_size: u64) -> u64 {
        self.zones.as_ref().map_or(0, |x| x.len() as u64) * zone_size
    }
}

impl Header {
    pub const SIZE: usize = 32;
}

impl<U> toa_blob_store::BlobStore for BlobStore<U>
where
    U: ZoneDev,
{
    type BlobHandle = BlobId;

    fn open_clear(&self, name: &str) -> io::Result<Self::BlobHandle> {
        let name = name.as_bytes();
        if let Some(x) = self.find(&name)? {
            x.delete()?;
        }
        Ok(self.create_blob(&name)?.unwrap().id())
    }
    fn find(&self, name: &str) -> io::Result<Option<Self::BlobHandle>> {
        Ok(self.find(name.as_bytes())?.map(|x| x.id()))
    }
    fn transaction<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce() -> io::Result<R>,
    {
        self.transaction(f)
    }
    fn name(&self, blob: &Self::BlobHandle) -> io::Result<String> {
        Ok(String::from_utf8_lossy(&self.data.borrow().blobs[*blob].name).to_string())
    }
    fn create(&self, name: &str) -> io::Result<Result<Self::BlobHandle, DuplicateBlob>> {
        Ok(self.create_blob(name.as_bytes())?.map(|x| x.id()))
    }
    fn create_unzoned(&self, name: &str) -> io::Result<Result<Self::BlobHandle, DuplicateBlob>> {
        Ok(self.create_unzoned_blob(name.as_bytes())?.map(|x| x.id()))
    }
    fn rename(&self, old_name: &str, new_name: &str) -> io::Result<()> {
        self.find(old_name.as_bytes())?
            .unwrap()
            .rename(new_name.as_bytes())
    }
    fn append(&self, blob: &Self::BlobHandle, data: &[u8]) -> io::Result<u64> {
        self.blob(*blob).append(data)
    }
    fn append_many(&self, blob: &Self::BlobHandle, data: &[&[u8]]) -> io::Result<u64> {
        self.blob(*blob).append_many(data)
    }
    fn read_at(&self, blob: &Self::BlobHandle, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.blob(*blob).read_at(offset, buf)
    }
    fn len(&self, blob: &Self::BlobHandle) -> io::Result<u64> {
        self.blob(*blob).len()
    }
    fn clear(&self, blob: &Self::BlobHandle) -> io::Result<()> {
        self.blob(*blob).clear()
    }
    fn delete(&self, blob: Self::BlobHandle) -> io::Result<()> {
        self.blob(blob).delete()
    }
    fn flush(&self) -> io::Result<()> {
        (&*self).flush()
    }
    fn size_on_disk(&self) -> io::Result<u64> {
        self.size_on_disk()
    }
    fn blobs<'a>(&'a self) -> io::Result<impl Iterator<Item = io::Result<Self::BlobHandle>> + 'a> {
        struct Iter<'a> {
            data: &'a RefCell<BlobStoreData>,
            index: u32,
        }

        impl<'a> Iterator for Iter<'a> {
            type Item = io::Result<BlobId>;

            fn next(&mut self) -> Option<Self::Item> {
                let data = self.data.borrow();
                while let Some(x) = data.blobs.table.get(self.index as usize) {
                    let id = BlobId(self.index);
                    self.index += 1;
                    if x.is_some() {
                        return Some(Ok(id));
                    }
                }
                None
            }
        }

        Ok(Iter {
            data: &self.data,
            index: 0,
        })
    }
}

impl error::Error for AddZoneToBlobError {}
impl error::Error for NoBlobWithId {}
impl error::Error for BlobInsertError {}
impl error::Error for ZoneIdOutOfRange {}

impl fmt::Display for AddZoneToBlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoBlobWithId(x) => NoBlobWithId(*x).fmt(f),
            Self::ZoneIdOutOfRange(x) => ZoneIdOutOfRange(*x).fmt(f),
            Self::IsUnzonedBlob => "blob is unzoned".fmt(f),
        }
    }
}

impl fmt::Display for NoBlobWithId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no blob with ID {}", self.0)
    }
}

impl fmt::Display for BlobInsertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateBlob(x) => write!(f, "duplicate blob ID {x}"),
            Self::BlobIdOutOfRange(x) => write!(f, "blob ID {x} out of range"),
        }
    }
}

impl fmt::Display for ZoneIdOutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "zone ID {} out of range", self.0)
    }
}

impl From<NoBlobWithId> for AddZoneToBlobError {
    fn from(x: NoBlobWithId) -> Self {
        Self::NoBlobWithId(x.0)
    }
}

impl From<ZoneIdOutOfRange> for AddZoneToBlobError {
    fn from(x: ZoneIdOutOfRange) -> Self {
        Self::ZoneIdOutOfRange(x.0)
    }
}

macro_rules! err_to_ioerr {
    ($($ty:ident)*) => {$(
        impl From<$ty> for io::Error {
            fn from(x: $ty) -> Self {
                io::Error::new(io::ErrorKind::InvalidData, x)
            }
        }
    )*};
}

err_to_ioerr! {
    AddZoneToBlobError
    BlobInsertError
    NoBlobWithId
    ZoneIdOutOfRange
}

macro_rules! fmt_id {
    ($name:ident) => {
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

fmt_id!(BlobId);
fmt_id!(ZoneId);

/// Try to extract information from the first few bytes of a blob store.
///
/// # Returns
///
/// `None` if magic or version is not recognized.
/// Otherwise various information extracted from the header.
pub fn snoop_header(first_bytes: [u8; Header::SIZE]) -> Option<Header> {
    let hdr = bytemuck::cast::<_, log::entry::Header>(first_bytes);
    (hdr.magic == log::entry::Header::MAGIC && hdr.version == log::entry::Header::VERSION)
        .then_some(Header {
            block_size: hdr.block_size.into(),
            zone_blocks: hdr.zone_blocks.into(),
            zone_count: hdr.zone_count.into(),
        })
}

#[cfg(test)]
mod test {
    use super::*;

    const BLOCK_SIZE: u32 = 512;
    const ZONE_BLOCKS: u32 = 42;
    const ZONE_SIZE: u32 = ZONE_BLOCKS * BLOCK_SIZE;

    macro_rules! with_dev {
        ($mod:ident $dev:ty : $init:expr) => {
            mod $mod {
                use super::*;

                type Dev = $dev;

                struct Test {
                    store: BlobStore<Dev>,
                }

                impl Test {
                    fn new() -> Self {
                        Self {
                            store: BlobStore::init($init).unwrap(),
                        }
                    }

                    fn remount(self) -> Self {
                        let zone_dev = self.store.unmount().map_err(|e| e.1).unwrap();
                        Self {
                            store: BlobStore::load(zone_dev).unwrap(),
                        }
                    }

                    #[track_caller]
                    fn blob<'a>(&'a self, name: &[u8]) -> BlobRef<'a, BlobStore<Dev>> {
                        self.store.find(name).unwrap().expect("missing blob")
                    }

                    #[track_caller]
                    fn append(&self, blob: &[u8], expect_offset: u64, data: &[u8]) {
                        let o = self.blob(blob).append(data).unwrap();
                        assert_eq!(o, expect_offset, "got <> expected")
                    }

                    #[track_caller]
                    fn assert_len(&self, blob: &[u8], expect_len: u64) {
                        let x = self.blob(blob).len().unwrap();
                        assert_eq!(x, expect_len);
                    }
                }

                impl core::ops::Deref for Test {
                    type Target = BlobStore<Dev>;

                    fn deref(&self) -> &Self::Target {
                        &self.store
                    }
                }

                impl core::ops::DerefMut for Test {
                    fn deref_mut(&mut self) -> &mut Self::Target {
                        &mut self.store
                    }
                }

                // these tests are all based on fuzz artifacts.
                // when adding or changing a feature: first run tests,
                // then update the fuzzer, run it and just wait for test cases to pop up.

                #[test]
                fn empty() {
                    Test::new().remount();
                }

                #[test]
                fn create_blobs() {
                    let mut store = Test::new();
                    store.create_blob(b"a").unwrap().unwrap();
                    store.create_blob(b"b").unwrap().unwrap();
                    store.blob(b"a");
                    store.blob(b"b");
                    store = store.remount();
                    store.blob(b"a");
                    store.blob(b"b");
                    store = store.remount();
                    store.create_blob(b"c").unwrap().unwrap();
                    store.blob(b"a");
                    store.blob(b"b");
                    store.blob(b"c");
                    store = store.remount();
                    store.blob(b"a");
                    store.blob(b"b");
                    store.blob(b"c");
                }

                #[test]
                fn create_duplicate_blobs() {
                    let store = Test::new();
                    store.create_blob(b"a").unwrap().unwrap();
                    assert!(store.create_blob(b"a").unwrap().is_err());
                }

                #[test]
                fn delete_blob() {
                    let store = Test::new();
                    store.create_blob(b"a").unwrap().unwrap();
                    store.blob(b"a").delete().unwrap();
                    store.create_blob(b"a").unwrap().unwrap();
                    store.blob(b"a").delete().unwrap();
                    store.remount();
                }

                #[test]
                fn append_blob() {
                    let s = Test::new();
                    let b = s.create_blob(b"a").unwrap().unwrap();
                    let o = b.append(&[0; 507]).unwrap();
                    assert_eq!(o, 0);
                    s.store.unmount().map_err(|e| e.1).unwrap();
                }

                #[test]
                fn append_blob_remount() {
                    let s = Test::new();
                    s.create_blob(b"a").unwrap().unwrap();
                    let s = s.remount();
                    let o = s.blob(b"a").append(&[0; 513]).unwrap();
                    assert_eq!(o, 0);
                }

                #[test]
                fn append_blob_large() {
                    let s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[0; (ZONE_SIZE + BLOCK_SIZE) as usize]);
                    s.store.unmount().map_err(|e| e.1).unwrap();
                }

                #[test]
                fn append_blob_small_large() {
                    let s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[0; 400]);
                    s.append(b"", 400, &[0; (ZONE_SIZE + BLOCK_SIZE) as usize]);
                    s.store.unmount().map_err(|e| e.1).unwrap();
                }

                #[test]
                fn rename_blob_shuffle_bloblist() {
                    let s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.create_blob(b"a").unwrap().unwrap();
                    s.create_blob(b"b").unwrap().unwrap();
                    s.blob(b"a").rename(b"").unwrap();
                    s.append(b"b", 0, b"");
                }

                #[test]
                fn log_overflow() {
                    let s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[b'a'; 10000]);
                    s.append(b"", 10000, &[b'b'; 20000]);
                    let s = s.remount();
                    let buf = &mut [0; 40000];
                    let n = s.blob(b"").read_at(0, buf).unwrap();
                    assert_eq!(n, 30000);
                    assert_eq!(buf[..10000], [b'a'; 10000]);
                    assert_eq!(buf[10000..30000], [b'b'; 20000]);
                    // ensure we commit to the right zone
                    s.create_blob(b"a").unwrap().unwrap();
                    s.flush().unwrap();
                }

                // triggered a particular case where the mirror log used the wrong zone ID
                #[test]
                fn log_overflow_delete() {
                    let s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[b'a'; 10000]);
                    s.append(b"", 10000, &[b'b'; 20000]);
                    let s = s.remount();
                    s.blob(b"").delete().unwrap();
                    s.remount();
                }

                #[test]
                fn log_overflow_load_zone_allocation_map() {
                    let s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    // 42 * 512 = 21504
                    // hence, assuming no "commit blob", this forcibly allocates a second log zone
                    s.append(b"", 0, &[0; 30000]);
                    let s = s.remount();
                    // this breaks after a remount if *zone allocation* tracking isn't done properly
                    s.append(b"", 30000, &[0; 20000]);
                }

                #[test]
                fn append_blob_truncated_tail() {
                    let s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[0]);
                    s.append(b"", 1, &[0]);
                    s.append(b"", 2, &[]);
                }

                #[test]
                fn load_replay_add_zone_to_blob() {
                    let s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[1; 30000]);
                    let s = s.remount();
                    s.append(b"", 30000, &[2; 20000]);
                    let buf = &mut [0];
                    let n = s.blob(b"").read_at(48000, buf).unwrap();
                    assert_eq!(n, 1);
                    assert_eq!(buf, &[2]);
                }

                /// We did correctly update flushed for AppendBlob during replay
                /// but forgot to reset it when encountering a CommitBlob entry.
                #[test]
                fn load_commit_blob_reset_flushed() {
                    const A: usize = 1;
                    const B: usize = 511;
                    const C: usize = 1;
                    let s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[0; A]);
                    let s = s.remount();
                    s.append(b"", A as _, &[0; B]);
                    let s = s.remount();
                    s.append(b"", (A + B) as _, &[0; C]);
                    let s = s.remount();
                    s.assert_len(b"", (A + B + C) as u64);
                }

                #[test]
                fn delete_blob_release_zones() {
                    let s = Test::new();
                    for _ in 0..100 {
                        let b = s.create_blob(b"").unwrap().unwrap();
                        b.append(&[0; 1024]).unwrap();
                        b.delete().unwrap();
                    }
                }

                #[test]
                fn rename_blob_release_zones() {
                    let s = Test::new();
                    s.create_blob(&[0]).unwrap().unwrap();
                    for x in 0..100 {
                        let b = s.create_blob(&[x + 1]).unwrap().unwrap();
                        b.append(&[0; 1024]).unwrap();
                        s.blob(&[x]).rename(&[x + 1]).unwrap();
                    }
                }

                #[test]
                fn dirty_dev() {
                    let s = Test::new().store;
                    s.create_blob(b"").unwrap().unwrap();
                    // reset
                    let dev = s.unmount().map_err(|e| e.1).unwrap();
                    let s = BlobStore::init(dev).unwrap();
                    // remount
                    let dev = s.unmount().map_err(|e| e.1).unwrap();
                    let s = BlobStore::load(dev).unwrap();
                    // will fail if stale log entries are used
                    s.create_blob(b"")
                        .unwrap()
                        .expect("stale log should not be used");
                }

                /// Ensure no attempts are made at remounting.
                /// `zones` is None, so simply apppending and waiting for a panic (or not)
                /// is sufficient.
                #[test]
                fn unzoned_blob_append() {
                    let s = Test::new();
                    let b = s.create_unzoned_blob(b"").unwrap().unwrap();
                    b.append(&[0; 512]).unwrap();
                }

                /// Ensure the right log entry type is used by simulating a remount.
                #[test]
                fn unzoned_blob_log_ty() {
                    let mut s = Test::new();
                    let b = s.create_unzoned_blob(b"").unwrap().unwrap();
                    b.append(&[0; 513]).unwrap();
                    s = s.remount();
                    s.append(b"", 513, &[]);
                }
            }
        };
    }

    with_dev!(memzones  MemZones<512> : Dev::new(42, 10));
    with_dev!(memblocks MemBlocks     : Dev::new(BlockShift::N9, 42, 10));

    #[test]
    fn snoop_header() {
        let s = BlobStore::init(MemBlocks::new(BlockShift::N9, 42, 10))
            .unwrap()
            .unmount()
            .map_err(|e| e.1)
            .unwrap();
        let x = s.blocks.borrow()[..Header::SIZE].try_into().unwrap();
        let x = super::snoop_header(x).unwrap();
        assert_eq!(x.block_size, 512);
        assert_eq!(x.zone_blocks, 42);
        assert_eq!(x.zone_count, 10);
    }
}
