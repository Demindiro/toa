use bitvec::boxed::BitBox;
#[cfg(feature = "std")]
use std::os::unix::fs::FileExt;
use std::{
    cell::RefCell,
    collections::btree_map::{BTreeMap, Entry},
    fmt, io, ops,
    rc::Rc,
};

mod log {
    pub mod entry {
        use nora_endian::{u16le, u32le, u64le};

        macro_rules! ty {
            ($($value:literal $name:ident)*) => {
                pub mod ty {
                    $(pub const $name: u8 = $value;)*
                }
            };
        }
        ty! {
            0 LOG_BLOCK_END
            1 CREATE_BLOB
            2 DELETE_BLOB
            3 ADD_ZONE_TO_BLOB
            4 RENAME_BLOB
            5 APPEND_BLOB_TAIL
            6 NEXT_LOG_ZONE
            7 COMMIT_BLOB_TAIL
            84 HEADER
        }

        // finally found a usecase that ChatGPT is actually
        // reliable for. Just needs a few substitution fixes.

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct LogBlockEnd {
            pub ty: u8,
            pub _pad_0: [u8; 7],
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct CreateBlob {
            pub ty: u8,
            pub name_len: u8,
            // name: u8[]
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct DeleteBlob {
            pub ty: u8,
            pub _pad_0: [u8; 3],
            pub blob_id: u32le,
        }

        #[repr(C, align(8))]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AddZoneToBlob {
            pub ty: u8,
            pub _pad_0: [u8; 3],
            pub blob_id: u32le,
            pub zone_id: u32le,
            pub _pad_1: [u8; 4],
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct RenameBlob {
            pub ty: u8,
            pub name_len: u8,
            pub _pad_0: u16le,
            pub blob_id: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AppendBlobTail {
            pub ty: u8,
            pub _pad_0: u8,
            pub data_len: u16le,
            pub blob_id: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct CommitBlobTail {
            pub ty: u8,
            pub _pad_0: [u8; 3],
            pub blob_id: u32le,
            pub len: u64le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct NextLogZone {
            pub ty: u8,
            pub _pad_0: [u8; 3],
            pub zone_id: u32le,
        }

        #[derive(Clone, Copy, Default, bytemuck::Pod, bytemuck::Zeroable)]
        #[repr(C)]
        pub struct Header {
            pub magic: [u8; 4],
            pub version: u32le,
            pub generation: u64le,
            pub block_size: u32le,
            pub zone_blocks: u32le,
            pub zone_count: u32le,
            pub _pad_0: [u8; 4],
        }

        impl Header {
            pub const MAGIC: [u8; 4] = *b"ToaB";
            pub const VERSION: u32 = 0x20260307;
        }
    }
}

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
    fn zone_count(&self) -> u32;

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
pub struct DuplicateBlob;

#[derive(Debug)]
pub struct OutOfZones;

/// # Note about zone data alignment
///
/// Data is *not* aligned to block boundaries.
/// This is to maximize compression density and simplify the interface.
///
/// To ensure blocks are written as a whole there is a second tail buffer,
/// which is appended to until it is block-sized.
struct Blob {
    name: Rc<[u8]>,
    zones: Vec<ZoneId>,
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
struct ZoneId(u32);

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
            zone_count: zone_dev.zone_count().into(),
            _pad_0: Default::default(),
        };
        let hdr = bytemuck::bytes_of(&hdr);
        let buf = &mut vec![0; usize::from(zone_dev.block_size())];
        buf[..hdr.len()].copy_from_slice(hdr);
        zone_dev.append(0, 0, buf)?;
        zone_dev.append(nr_zones - 1, 0, buf)?;

        let mut data = BlobStoreData::new(generation, nr_zones);
        data.log_zone_head = zone_dev.block_size().into();

        Ok(Self {
            zone_dev,
            data: data.into(),
        })
    }

    pub fn load(zone_dev: U) -> io::Result<Self> {
        let block_size = usize::from(zone_dev.block_size());
        let zone_blocks = u64::from(zone_dev.zone_blocks());
        let zone_size = zone_blocks * block_size as u64;
        let block_a = &mut *vec![0; block_size];
        let block_b = &mut *vec![0; block_size];
        let mut log_zone_a = 0;
        let mut log_zone_b = u32::from(zone_dev.zone_count()) - 1;
        // TODO check write pointer first
        zone_dev.read_at(log_zone_a, 0, block_a)?;
        zone_dev.read_at(log_zone_b, 0, block_b)?;

        let mut gen_a @ mut gen_b = 0;
        for (genn, blk) in [(&mut gen_a, &block_a), (&mut gen_b, &block_b)] {
            let hdr = &blk[..core::mem::size_of::<log::entry::Header>()];
            let hdr = bytemuck::from_bytes::<log::entry::Header>(hdr);

            if hdr.magic != log::entry::Header::MAGIC {
                todo!("bad magic");
            }
            if hdr.version != log::entry::Header::VERSION {
                todo!("bad version");
            }

            if hdr.block_size != u32::from(zone_dev.block_size()) {
                todo!("block size mismatch");
            }
            if hdr.zone_blocks != zone_dev.zone_blocks() {
                todo!("zone blocks mismatch");
            }
            if hdr.zone_count != zone_dev.zone_count() {
                todo!("zone count mismatch");
            }

            *genn = hdr.generation.into();
        }
        assert_eq!(gen_a, gen_b); // TODO don't panic, return error

        let mut store = BlobStoreData::new(gen_a, zone_dev.zone_count());

        let mut log_end = zone_dev.zone_write_head(log_zone_a)?.unwrap_or(zone_size);

        while store.log_zone_head < log_end {
            store.log_zone_head += block_size as u64;

            let mut end_of_log = true;

            let mut k = 0;
            let (buf_a, []) = block_a.as_chunks_mut::<8>() else {
                unreachable!()
            };
            let (buf_b, []) = block_b.as_chunks_mut::<8>() else {
                unreachable!()
            };
            while let Some(x) = buf_a.get(k) {
                let [ty, b, c, d, e, f, g, h] = *x;
                end_of_log &= ty == log::entry::ty::LOG_BLOCK_END;
                // FIXME ensure log entries are equal *except* NEXT_LOG_ZONE
                // we should have a helper function which just returns an entry,
                // that way we can do a simple (==) check
                match ty {
                    log::entry::ty::LOG_BLOCK_END => break,
                    log::entry::ty::CREATE_BLOB => {
                        let name_len = usize::from(b);
                        let name = &buf_a[k..].as_flattened()[2..2 + name_len];
                        k += ((2 + name_len) + 7) >> 3;
                        store.replay_create_blob(name).unwrap();
                    }
                    log::entry::ty::DELETE_BLOB => {
                        k += 1;
                        let id = u32::from_le_bytes([e, f, g, h]);
                        store.replay_delete_blob(BlobId(id));
                    }
                    log::entry::ty::RENAME_BLOB => {
                        k += 1;
                        let name_len = usize::from(b);
                        let id = u32::from_le_bytes([e, f, g, h]);
                        let name = &buf_a[k..].as_flattened()[..usize::from(name_len)];
                        store.replay_rename_blob(BlobId(id), name);
                        k += (name_len + 7) >> 3;
                    }
                    log::entry::ty::APPEND_BLOB_TAIL => {
                        k += 1;
                        let len = usize::from(u16::from_le_bytes([c, d]));
                        let id = u32::from_le_bytes([e, f, g, h]);
                        let data = &buf_a[k..].as_flattened()[..usize::from(len)];
                        store.replay_append_blob(BlobId(id), data);
                        k += (len + 7) >> 3;
                    }
                    log::entry::ty::ADD_ZONE_TO_BLOB => {
                        k += 1;
                        let id = u32::from_le_bytes([e, f, g, h]);
                        let [x, y, z, w, _, _, _, _] = buf_a[k];
                        let zone = u32::from_le_bytes([x, y, z, w]);
                        k += 1;
                        store.replay_add_zone_to_blob(BlobId(id), ZoneId(zone));
                    }
                    log::entry::ty::COMMIT_BLOB_TAIL => {
                        k += 1;
                        let id = u32::from_le_bytes([e, f, g, h]);
                        let len = u64::from_le_bytes(buf_a[k]);
                        k += 1;
                        store.replay_commit_blob(BlobId(id), len);
                    }
                    log::entry::ty::NEXT_LOG_ZONE => {
                        let [_, _, _, _, x, y, z, w] = buf_b[k];
                        log_zone_a = u32::from_le_bytes([e, f, g, h]);
                        log_zone_b = u32::from_le_bytes([x, y, z, w]);
                        store.log_zone_head = 0;
                        store.log_zone_a = ZoneId(log_zone_a);
                        store.log_zone_b = ZoneId(log_zone_b);
                        store.mark_zone_allocated(store.log_zone_a);
                        store.mark_zone_allocated(store.log_zone_b);
                        log_end = zone_dev.zone_write_head(log_zone_a)?.unwrap_or(zone_size);
                        break;
                    }
                    log::entry::ty::HEADER => k += 2,
                    ty => todo!("{ty}"),
                }
            }

            if end_of_log {
                assert!(
                    zone_dev.zone_write_head(log_zone_a)?.is_none(),
                    "zoned device should not contain end_of_log"
                );
                store.log_zone_head -= block_size as u64;
                break;
            }

            store.log_len += block_size as u64;

            if store.log_zone_head < log_end {
                zone_dev.read_at(log_zone_a, store.log_zone_head, block_a)?;
                zone_dev.read_at(log_zone_b, store.log_zone_head, block_b)?;
            }
        }

        Ok(BlobStore {
            zone_dev,
            data: store.into(),
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let s = &mut *self.data.borrow_mut();
        let blob_num = s.blobs.table.len() as u32;
        for id in (0..blob_num).map(BlobId) {
            if s.blobs.get(id).is_some() {
                self.flush_blob(s, id)?;
            }
        }
        self.log_flush(s)?;
        Ok(())
    }

    pub fn unmount(mut self) -> Result<U, (Self, io::Error)> {
        if let Err(e) = self.flush() {
            return Err((self, e));
        }
        Ok(self.zone_dev)
    }

    pub fn blob(&self, id: BlobId) -> io::Result<BlobRef<'_, Self>> {
        Ok(BlobRef { store: self, id })
    }

    pub fn find(&self, name: &[u8]) -> io::Result<Option<BlobRef<'_, Self>>> {
        assert!(name.len() <= 255, "name too long");
        match self.data.borrow().blob_map.get(name) {
            None => Ok(None),
            Some(id) => self.blob(*id).map(Some),
        }
    }

    pub fn create_blob<'a>(
        &'a self,
        name: &[u8],
    ) -> io::Result<Result<BlobRef<'a, Self>, DuplicateBlob>> {
        let s = &mut *self.data.borrow_mut();
        let res = s.replay_create_blob(name);
        match res {
            Ok(id) => self
                .log_create_blob(s, name)
                .map(|()| Ok(BlobRef { store: self, id })),
            Err(e) => Ok(Err(e)),
        }
    }

    pub fn size_on_disk(&self) -> io::Result<u64> {
        let s = self.data.borrow();
        let mut n = s.log_len;
        for x in s.blobs.iter() {
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

    fn log_create_blob(&self, s: &mut BlobStoreData, name: &[u8]) -> io::Result<()> {
        let hdr = log::entry::CreateBlob {
            ty: log::entry::ty::CREATE_BLOB,
            name_len: u8::try_from(name.len()).unwrap().into(),
        };
        self.log_push(s, &[bytemuck::bytes_of(&hdr), name])
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

    fn log_push(&self, s: &mut BlobStoreData, data: &[&[u8]]) -> io::Result<()> {
        let len = data.iter().fold(0, |s, x| s + x.len());
        self.log_reserve(s, len)?;
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
        let zone_blocks = u64::from(self.zone_dev.zone_blocks());
        let zone_size = zone_blocks * block_size as u64;

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
        let rem = zone_size - data.log_zone_head;

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

        Ok(())
    }
}

impl BlobStoreData {
    fn new(generation: u64, nr_zones: u32) -> Self {
        let mut s = Self {
            generation,
            blobs: Default::default(),
            blob_map: Default::default(),
            log: Vec::new(),
            log_zone_a: ZoneId(0),
            log_zone_b: ZoneId(nr_zones - 1),
            log_zone_head: 0,
            log_len: 0,
            allocated_zones: bitvec::bitbox![0; nr_zones as usize],
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

    fn mark_zone_allocated(&mut self, id: ZoneId) {
        self.allocated_zones.set(id.0 as usize, true);
    }

    fn replay_create_blob(&mut self, name: &[u8]) -> Result<BlobId, DuplicateBlob> {
        assert!(name.len() <= 255, "name too long");
        match self.blob_map.entry(name.into()) {
            Entry::Occupied(_) => Err(DuplicateBlob),
            Entry::Vacant(e) => {
                let id = self.blobs.insert(Blob::new(e.key().clone()));
                e.insert(id);
                Ok(id)
            }
        }
    }

    fn replay_delete_blob(&mut self, id: BlobId) {
        let mut old = self.blobs.remove(id).expect("old blob missing");
        self.free_zones(&mut old.zones);
        self.blob_map.remove(&old.name);
    }

    /// # Returns
    ///
    /// `true` if the blob actually got renamed, `false` if the operation is a no-op.
    fn replay_rename_blob(&mut self, id: BlobId, new_name: &[u8]) -> (bool, Option<Blob>) {
        let blob = &mut self.blobs[id];
        if &*blob.name == new_name {
            return (false, None);
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
        if let Some(old) = old.as_mut() {
            self.free_zones(&mut old.zones);
        }
        (true, old)
    }

    fn replay_append_blob(&mut self, id: BlobId, data: &[u8]) {
        self.blobs[id].tail.extend(data);
        self.blobs[id].flushed += data.len();
    }

    fn replay_add_zone_to_blob(&mut self, id: BlobId, zone: ZoneId) {
        self.blobs[id].zones.push(zone);
        self.mark_zone_allocated(zone);
    }

    fn replay_commit_blob(&mut self, id: BlobId, len: u64) {
        self.blobs[id].tail.clear();
        self.blobs[id].len = len;
        self.blobs[id].flushed = 0;
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
    pub fn delete(self) -> io::Result<()> {
        let s = &mut *self.store.data.borrow_mut();
        self.store
            .zone_dev
            .reset_many(bytemuck::cast_slice(&s.blobs[self.id].zones))?;
        s.replay_delete_blob(self.id);
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

        let n = s.blobs[idx].tail.len().wrapping_neg() % block_size;
        let n = n.min(data.len());
        let (head, data) = data.split_at(n);
        s.blobs[idx].tail.extend(head);

        if s.blobs[idx].tail.len() >= block_size {
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
        let (renamed, old) = s.replay_rename_blob(self.id, new_name);
        if renamed {
            if let Some(old) = old {
                self.store
                    .zone_dev
                    .reset_many(bytemuck::cast_slice(&old.zones))?;
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
                    s.blobs[idx].zones[zone as usize].0,
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
            match s.blobs[self.id].zones.last() {
                None => {
                    [zone] = s.alloc_zones_array().unwrap(); // TODO don't panic
                    self.store.log_add_zone_to_blob(s, self.id, zone)?;
                    s.replay_add_zone_to_blob(self.id, zone);
                }
                Some(z) => zone = *z,
            };
            let n = s.blobs[self.id].zones_capacity(zone_size);
            if n == s.blobs[self.id].len {
                [zone] = s.alloc_zones_array().unwrap(); // TODO don't panic
                self.store.log_add_zone_to_blob(s, self.id, zone)?;
                s.replay_add_zone_to_blob(self.id, zone);
            }
            let n = zone_size - offset;
            let n = n.min(blocks.len() as u64) as usize;
            self.store.zone_dev.append(zone.0, offset, &blocks[..n])?;
            blocks = &blocks[n..];
            offset = 0;
            s.blobs[self.id].len += n as u64;
        }
        // TODO delay commit until explicit flush
        s.replay_commit_blob(self.id, end);
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

    fn remove(&mut self, id: BlobId) -> Option<Blob> {
        self.table.get_mut(id.0 as usize).and_then(|x| x.take())
    }

    fn iter(&self) -> impl Iterator<Item = &Blob> {
        self.table.iter().flat_map(|x| x.as_ref())
    }
}

impl ops::Index<BlobId> for BlobTable {
    type Output = Blob;

    fn index(&self, id: BlobId) -> &Self::Output {
        self.get(id)
            .unwrap_or_else(|| panic!("no blob with ID {id}"))
    }
}

impl ops::IndexMut<BlobId> for BlobTable {
    fn index_mut(&mut self, id: BlobId) -> &mut Self::Output {
        self.get_mut(id)
            .unwrap_or_else(|| panic!("no blob with ID {id}"))
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
    fn zone_count(&self) -> u32 {
        self.zones.borrow().len() as u32
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
        Self::wrap(block_size, zone_blocks, vec![0; n].into())
    }

    pub fn wrap(block_size: BlockShift, zone_blocks: u32, data: Box<[u8]>) -> Self {
        Self {
            blocks: RefCell::new(data),
            block_size,
            zone_blocks,
        }
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
    fn zone_count(&self) -> u32 {
        (self.blocks.borrow().len() / self.zone_size() as usize) as u32
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
    fn zone_count(&self) -> u32 {
        self.zone_count
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
            fn zone_count(&self) -> u32 {
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
    fn new(name: Rc<[u8]>) -> Self {
        Self {
            name,
            zones: Vec::new(),
            tail: Vec::new(),
            len: 0,
            flushed: 0,
        }
    }

    fn total_len(&self) -> u64 {
        self.len + self.tail.len() as u64
    }

    fn zones_capacity(&self, zone_size: u64) -> u64 {
        self.zones.len() as u64 * zone_size
    }
}

impl Header {
    pub const SIZE: usize = 32;
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobId({})", self.0)
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

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
                    let mut s = Test::new();
                    s.create_blob(b"a").unwrap().unwrap();
                    s = s.remount();
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
                    let mut s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[b'a'; 10000]);
                    s.append(b"", 10000, &[b'b'; 20000]);
                    s = s.remount();
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
                    let mut s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[b'a'; 10000]);
                    s.append(b"", 10000, &[b'b'; 20000]);
                    s = s.remount();
                    s.blob(b"").delete().unwrap();
                    s.remount();
                }

                #[test]
                fn log_overflow_load_zone_allocation_map() {
                    let mut s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    // 42 * 512 = 21504
                    // hence, assuming no "commit blob", this forcibly allocates a second log zone
                    s.append(b"", 0, &[0; 30000]);
                    s = s.remount();
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
                    let mut s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[1; 30000]);
                    s = s.remount();
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
                    let mut s = Test::new();
                    s.create_blob(b"").unwrap().unwrap();
                    s.append(b"", 0, &[0; A]);
                    s = s.remount();
                    s.append(b"", A as _, &[0; B]);
                    s = s.remount();
                    s.append(b"", (A + B) as _, &[0; C]);
                    s = s.remount();
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
