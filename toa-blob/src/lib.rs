use bitvec::boxed::BitBox;
use std::{
    cell::RefCell,
    collections::btree_map::{BTreeMap, Entry},
    io,
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
            0 NOP
            1 CREATE_BLOB
            2 DELETE_BLOB
            4 RENAME_BLOB
            5 APPEND_BLOB_TAIL
            6 NEXT_LOG_ZONE
            84 HEADER
        }

        // finally found a usecase that ChatGPT is actually
        // reliable for. Just needs a few substitution fixes.

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct Nop {
            pub ty: u8,
            pub _pad_: [u8; 3],
            pub _pad_ding_size: u32le,
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
            pub blob_index: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AddZoneToBlob {
            pub ty: u8,
            pub _pad_0: [u8; 3],
            pub blob_index: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct RenameBlob {
            pub ty: u8,
            pub name_len: u8,
            pub _pad_0: u16le,
            pub blob_index: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AppendBlobTail {
            pub ty: u8,
            pub _pad_0: u8,
            pub data_len: u16le,
            pub blob_index: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct CommitBlobTail {
            pub ty: u8,
            pub _pad_0: [u8; 3],
            pub blob_index: u32le,
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
    type Read<'a>: AsRef<[u8]>
    where
        Self: 'a;

    /// # Note
    ///
    /// `offset` and `len` are in *bytes*.
    fn read_at<'a>(&'a self, zone: u32, lba: u32, blocks: u32) -> io::Result<Self::Read<'a>>;

    /// # Note
    ///
    /// `offset` is in *bytes*.
    ///
    /// This method should panic if the offset is not aligned
    /// to a block boundary, as it is a severe logic error.
    // TODO extra copy is very bad and sad :(
    fn append<'a>(&'a self, zone: u32, data: &[u8]) -> io::Result<u64>;

    /// How many blocks can still be appended.
    fn blocks_free(&self, zone: u32) -> io::Result<u32>;

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
    blobs: Vec<Blob>,
    blob_map: BTreeMap<Rc<[u8]>, u32>,
    log: Vec<u8>,
    log_zone_a: ZoneId,
    log_zone_b: ZoneId,
    allocated_zones: BitBox,
}

pub struct MemZones<const B: usize> {
    zones: RefCell<Box<[Vec<[u8; B]>]>>,
    zone_size: u32,
}

pub struct MemZonesRef<'a>(core::cell::Ref<'a, [u8]>);

pub enum BlockShift {
    N9 = 1 << 9,
    N12 = 1 << 12,
}

pub struct BlobRef<'a, T> {
    store: &'a T,
    index: u32,
}

#[derive(Clone, Copy)]
pub struct BlobHandle(u32);

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
}

#[derive(Clone, Copy)]
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
        zone_dev.append(0, buf)?;
        zone_dev.append(nr_zones - 1, buf)?;

        Ok(Self {
            zone_dev,
            data: BlobStoreData::new(generation, nr_zones).into(),
        })
    }

    pub fn load(zone_dev: U) -> io::Result<Self> {
        let mut log_zone_a = 0;
        let mut log_zone_b = u32::from(zone_dev.zone_count()) - 1;
        let mut log_block = 0;
        let mut block_a = zone_dev.read_at(log_zone_a, log_block, 1)?;
        let mut block_b = zone_dev.read_at(log_zone_b, log_block, 1)?;
        log_block += 1;

        let mut gen_a @ mut gen_b = 0;
        for (genn, blk) in [(&mut gen_a, &block_a), (&mut gen_b, &block_b)] {
            let hdr = &blk.as_ref()[..core::mem::size_of::<log::entry::Header>()];
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

        loop {
            let buf_a = block_a.as_ref();
            let buf_b = block_b.as_ref();
            assert_eq!(buf_a.len(), buf_b.len()); // TODO don't panic, return error
            if buf_a.is_empty() {
                break;
            }

            let mut k = 0;
            let (buf_a, []) = buf_a.as_chunks::<8>() else {
                unreachable!()
            };
            let (buf_b, []) = buf_b.as_chunks::<8>() else {
                unreachable!()
            };
            while let Some(x) = buf_a.get(k) {
                let [ty, b, c, d, e, f, g, h] = *x;
                // FIXME ensure log entries are equal *except* NEXT_LOG_ZONE
                // we should have a helper function which just returns an entry,
                // that way we can do a simple (==) check
                match ty {
                    log::entry::ty::NOP => {
                        k += 1 + ((u32::from_le_bytes([e, f, g, h]) as usize) >> 3);
                    }
                    log::entry::ty::CREATE_BLOB => {
                        let name_len = usize::from(b);
                        let name = &buf_a[k..].as_flattened()[2..2 + name_len];
                        k += ((2 + name_len) + 7) >> 3;
                        store.replay_create_blob(name).unwrap();
                    }
                    log::entry::ty::DELETE_BLOB => {
                        k += 1;
                        let idx = u32::from_le_bytes([e, f, g, h]);
                        store.replay_delete_blob(idx);
                    }
                    log::entry::ty::RENAME_BLOB => {
                        k += 1;
                        let name_len = usize::from(b);
                        let idx = u32::from_le_bytes([e, f, g, h]);
                        let name = &buf_a[k..].as_flattened()[..usize::from(name_len)];
                        store.replay_rename_blob(idx, name);
                        k += (name_len + 7) >> 3;
                    }
                    log::entry::ty::APPEND_BLOB_TAIL => {
                        k += 1;
                        let len = usize::from(u16::from_le_bytes([c, d]));
                        let idx = u32::from_le_bytes([e, f, g, h]);
                        let data = &buf_a[k..].as_flattened()[..usize::from(len)];
                        store.replay_append_blob(idx, data);
                        k += (len + 7) >> 3;
                    }
                    log::entry::ty::NEXT_LOG_ZONE => {
                        let [_, _, _, _, x, y, z, w] = buf_b[k];
                        log_zone_a = u32::from_le_bytes([e, f, g, h]);
                        log_zone_b = u32::from_le_bytes([x, y, z, w]);
                        log_block = 0;
                        store.log_zone_a = ZoneId(log_zone_a);
                        store.log_zone_b = ZoneId(log_zone_b);
                        store.mark_zone_allocated(store.log_zone_a);
                        store.mark_zone_allocated(store.log_zone_b);
                        break;
                    }
                    log::entry::ty::HEADER => k += 2,
                    ty => todo!("{ty}"),
                }
            }

            block_a = zone_dev.read_at(log_zone_a, log_block, 1)?;
            block_b = zone_dev.read_at(log_zone_b, log_block, 1)?;
            log_block += 1;
        }

        drop(block_a);
        drop(block_b);
        Ok(BlobStore {
            zone_dev,
            data: store.into(),
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.log_flush()?;
        Ok(())
    }

    pub fn unmount(mut self) -> Result<U, (Self, io::Error)> {
        if let Err(e) = self.flush() {
            return Err((self, e));
        }
        Ok(self.zone_dev)
    }

    pub fn blob(&self, name: &[u8]) -> io::Result<Option<BlobRef<'_, Self>>> {
        assert!(name.len() <= 255, "name too long");
        match self.data.borrow().blob_map.get(name) {
            None => Ok(None),
            Some(&index) => Ok(Some(BlobRef { store: self, index })),
        }
    }

    pub fn create_blob<'a>(
        &'a mut self,
        name: &[u8],
    ) -> io::Result<Result<BlobRef<'a, Self>, DuplicateBlob>> {
        let res = self.data.borrow_mut().replay_create_blob(name);
        match res {
            Ok(index) => self
                .log_create_blob(name)
                .map(|()| Ok(BlobRef { store: self, index })),
            Err(e) => Ok(Err(e)),
        }
    }

    pub fn size_on_disk(&self) -> io::Result<u64> {
        // TODO proper accounting
        let data = self.data.borrow();
        Ok(data.log.len() as u64)
    }

    fn log_create_blob(&self, name: &[u8]) -> io::Result<()> {
        let hdr = log::entry::CreateBlob {
            ty: log::entry::ty::CREATE_BLOB,
            name_len: u8::try_from(name.len()).unwrap().into(),
        };
        self.log_push(&[bytemuck::bytes_of(&hdr), name])
    }

    fn log_delete_blob(&self, index: u32) -> io::Result<()> {
        let hdr = log::entry::DeleteBlob {
            ty: log::entry::ty::DELETE_BLOB,
            _pad_0: Default::default(),
            blob_index: index.into(),
        };
        self.log_push(&[bytemuck::bytes_of(&hdr)])
    }

    fn log_rename_blob(&self, index: u32, name: &[u8]) -> io::Result<()> {
        let hdr = log::entry::RenameBlob {
            ty: log::entry::ty::RENAME_BLOB,
            name_len: u8::try_from(name.len()).unwrap().into(),
            _pad_0: Default::default(),
            blob_index: index.into(),
        };
        self.log_push(&[bytemuck::bytes_of(&hdr), name])
    }

    fn log_append_blob_tail(&self, index: u32, data: &[u8]) -> io::Result<()> {
        let len = u16::try_from(data.len()).unwrap(); // FIXME pre-split data
        let hdr = log::entry::AppendBlobTail {
            ty: log::entry::ty::APPEND_BLOB_TAIL,
            _pad_0: Default::default(),
            data_len: len.into(),
            blob_index: index.into(),
        };
        self.log_push(&[bytemuck::bytes_of(&hdr), data])
    }

    fn log_push(&self, data: &[&[u8]]) -> io::Result<()> {
        let len = data.iter().fold(0, |s, x| s + x.len());
        self.log_reserve(len)?;
        self.data
            .borrow_mut()
            .log
            .extend(data.iter().copied().flatten());
        self.log_pad();
        Ok(())
    }

    fn log_reserve(&self, num: usize) -> io::Result<()> {
        let num = (num + 7) & !7;
        let len = (self.data.borrow().log.len() + num) as u64;
        if len > u64::from(self.zone_dev.block_size()) {
            self.log_flush()?;
        }
        Ok(())
    }

    fn log_flush(&self) -> io::Result<()> {
        let data = &mut *self.data.borrow_mut();
        if data.log.is_empty() {
            return Ok(());
        }
        let max_len = usize::from(self.zone_dev.block_size());
        assert!(
            data.log.len() <= max_len,
            "{} <= {}",
            data.log.len(),
            max_len
        );
        // TODO optimize with long NOPs
        data.log.resize(max_len, 0);
        self.zone_dev.append(data.log_zone_a.0, &data.log)?;
        self.zone_dev.append(data.log_zone_b.0, &data.log)?;
        data.log.clear();

        // allocate a new zone if we nearly exhausted the current one
        let rem_a = self.zone_dev.blocks_free(data.log_zone_a.0)?;
        let rem_b = self.zone_dev.blocks_free(data.log_zone_b.0)?;
        assert_eq!(rem_a, rem_b, "log length mismatch");

        if rem_a <= 1 {
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
                data.log.resize(max_len, 0);
                self.zone_dev.append(log_zone.0, &data.log)?;
                data.log.clear();
            }
            data.log_zone_a = new_a;
            data.log_zone_b = new_b;
        }

        Ok(())
    }

    fn log_pad(&self) {
        let data = &mut *self.data.borrow_mut();
        let n = data.log.len();
        let n = (n + 7) & !7;
        data.log.resize(n, 0);
    }

    fn log_free(&self) -> usize {
        let data = self.data.borrow();
        usize::from(self.zone_dev.block_size()) - data.log.len()
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
            allocated_zones: bitvec::bitbox![0; nr_zones as usize],
        };
        s.allocated_zones.set(s.log_zone_a.0 as usize, true);
        s.allocated_zones.set(s.log_zone_b.0 as usize, true);
        s
    }

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

    fn mark_zone_allocated(&mut self, id: ZoneId) {
        self.allocated_zones.set(id.0 as usize, true);
    }

    fn replay_create_blob(&mut self, name: &[u8]) -> Result<u32, DuplicateBlob> {
        assert!(name.len() <= 255, "name too long");
        match self.blob_map.entry(name.into()) {
            Entry::Occupied(_) => Err(DuplicateBlob),
            Entry::Vacant(e) => {
                let idx = self.blobs.len() as u32;
                self.blobs.push(Blob::new(e.key().clone()));
                e.insert(idx);
                Ok(idx)
            }
        }
    }

    fn replay_delete_blob(&mut self, index: u32) {
        let old_name = self.blobs.swap_remove(index as usize).name;
        self.blob_map.remove(&old_name);
        if let Some(new) = self.blobs.get(index as usize) {
            *self.blob_map.get_mut(&new.name).unwrap() = index;
        }
    }

    /// # Returns
    ///
    /// `true` if the blob actually got renamed, `false` if the operation is a no-op.
    fn replay_rename_blob(&mut self, index: u32, new_name: &[u8]) -> bool {
        let blob = &mut self.blobs[index as usize];
        if &*blob.name == new_name {
            return false;
        }
        self.blob_map.remove(&*blob.name);
        match self.blob_map.entry(new_name.into()) {
            Entry::Vacant(e) => {
                blob.name = e.key().clone();
                e.insert(index);
            }
            Entry::Occupied(mut e) => {
                blob.name = e.key().clone();
                let other_idx = *e.get();
                self.blobs.swap_remove(other_idx as usize);
                // If the renamed blob is at the end, then that
                // blob just got moved to other_idx, so nothing to do
                if self.blobs.len() != index as usize {
                    // Otherwise, another blob got moved, so
                    // we need to update the map to point new_name
                    // to index and update the new blob at other_idx
                    debug_assert_ne!(index, other_idx);
                    e.insert(index);
                    // ... keep in mind that we may have just removed
                    // the blob at the very end.
                    if let Some(x) = self.blobs.get(other_idx as usize) {
                        *self
                            .blob_map
                            .get_mut(&x.name)
                            .expect("other blob is missing") = other_idx;
                    }
                }
            }
        }
        true
    }

    fn replay_append_blob(&mut self, index: u32, data: &[u8]) {
        self.blobs[index as usize].tail.extend(data);
    }
}

impl<'a, T> BlobRef<'a, T> {
    // FIXME not sound in the presence of deletes/renames
    /*
    pub fn into_handle(self) -> BlobHandle {
        BlobHandle(self.index)
    }
    */
}

impl<'a, U> BlobRef<'a, BlobStore<U>>
where
    U: ZoneDev,
{
    pub fn delete(self) -> io::Result<()> {
        self.store.data.borrow_mut().replay_delete_blob(self.index);
        self.store.log_delete_blob(self.index)
    }

    /// # Returns
    ///
    /// Start offset of written data.
    pub fn append(&mut self, mut data: &[u8]) -> io::Result<u64> {
        let offt = self.len()?;
        while !data.is_empty() {
            if self.store.log_free() == 0 {
                self.store.log_flush()?;
            }
            let wr;
            let n = (self.store.log_free() - 8).min(data.len());
            (wr, data) = data.split_at(n);
            self.store
                .data
                .borrow_mut()
                .replay_append_blob(self.index, wr);
            self.store.log_append_blob_tail(self.index, wr)?;
        }
        Ok(offt)
    }

    /// # Returns
    ///
    /// Start offset of written data.
    pub fn append_many(&mut self, data: &[&[u8]]) -> io::Result<u64> {
        let offt = self.len()?;
        for x in data {
            self.append(x)?;
        }
        Ok(offt)
    }

    pub fn rename(&mut self, new_name: &[u8]) -> io::Result<()> {
        // FIXME update index
        if self
            .store
            .data
            .borrow_mut()
            .replay_rename_blob(self.index, new_name)
        {
            self.store.log_rename_blob(self.index, new_name)?;
        }
        Ok(())
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let data = self.store.data.borrow();
        let s = usize::try_from(offset)
            .ok()
            .and_then(|x| data.blobs[self.index as usize].tail.get(x..))
            .unwrap_or(&[]);
        let n = s.len().min(buf.len());
        buf[..n].copy_from_slice(&s[..n]);
        Ok(n)
    }

    pub fn len(&self) -> io::Result<u64> {
        Ok(self.store.data.borrow().blobs[self.index as usize]
            .tail
            .len() as u64)
    }
}

impl<const B: usize> ZoneDev for MemZones<B> {
    type Read<'a>
        = MemZonesRef<'a>
    where
        Self: 'a;

    fn read_at<'a>(&'a self, zone: u32, lba: u32, blocks: u32) -> io::Result<Self::Read<'a>> {
        let start = lba as usize;
        let end = start + blocks as usize;
        let x = core::cell::Ref::map(self.zones.borrow(), |x| {
            let x = &x[zone as usize];
            let end = end.min(x.len());
            x[start..end].as_flattened()
        });
        Ok(MemZonesRef(x))
    }

    #[track_caller]
    fn append<'a>(&'a self, zone: u32, data: &[u8]) -> io::Result<u64> {
        let (data, []) = data.as_chunks() else {
            panic!("data len is not a multiple of the block size")
        };
        let x = &mut *self.zones.borrow_mut();
        let x = &mut x[zone as usize];
        let o = x.len() as u64;
        if x.len() + data.len() > self.zone_size as usize {
            panic!("zone overflow");
        }
        x.extend(data);
        Ok(o)
    }

    fn blocks_free(&self, zone: u32) -> io::Result<u32> {
        Ok(self.zone_size - self.zones.borrow()[zone as usize].len() as u32)
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

impl<'a> AsRef<[u8]> for MemZonesRef<'a> {
    fn as_ref(&self) -> &[u8] {
        &*self.0
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
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn init() -> BlobStore<MemZones<512>> {
        BlobStore::init(MemZones::new(42, 10)).unwrap()
    }

    fn remount(store: BlobStore<MemZones<512>>) -> BlobStore<MemZones<512>> {
        let zone_dev = store.unmount().map_err(|e| e.1).unwrap();
        BlobStore::load(zone_dev).unwrap()
    }

    // these tests are all based on fuzz artifacts.
    // when adding or changing a feature: first run tests,
    // then update the fuzzer, run it and just wait for test cases to pop up.

    #[test]
    fn empty() {
        let _ = remount(init());
    }

    #[test]
    fn create_blobs() {
        let mut store = init();
        store.create_blob(b"a").unwrap().unwrap();
        store.create_blob(b"b").unwrap().unwrap();
        store.blob(b"a").unwrap().expect("missing blob a");
        store.blob(b"b").unwrap().expect("missing blob b");
        store = remount(store);
        store.blob(b"a").unwrap().expect("missing blob a");
        store.blob(b"b").unwrap().expect("missing blob b");
        store = remount(store);
        store.create_blob(b"c").unwrap().unwrap();
        store.blob(b"a").unwrap().expect("missing blob a");
        store.blob(b"b").unwrap().expect("missing blob b");
        store.blob(b"c").unwrap().expect("missing blob c");
        store = remount(store);
        store.blob(b"a").unwrap().expect("missing blob a");
        store.blob(b"b").unwrap().expect("missing blob b");
        store.blob(b"c").unwrap().expect("missing blob c");
    }

    #[test]
    fn create_duplicate_blobs() {
        let mut store = init();
        store.create_blob(b"a").unwrap().unwrap();
        assert!(store.create_blob(b"a").unwrap().is_err());
    }

    #[test]
    fn delete_blob() {
        let mut store = init();
        store.create_blob(b"a").unwrap().unwrap();
        store.blob(b"a").unwrap().unwrap().delete().unwrap();
        store.create_blob(b"a").unwrap().unwrap();
        store.blob(b"a").unwrap().unwrap().delete().unwrap();
        remount(store);
    }

    #[test]
    fn append_blob() {
        let mut s = init();
        let mut b = s.create_blob(b"a").unwrap().unwrap();
        b.append(&[0; 507]).unwrap();
        s.unmount().map_err(|e| e.1).unwrap();
    }

    #[test]
    fn append_blob_remount() {
        let mut s = init();
        s.create_blob(b"a").unwrap().unwrap();
        s = remount(s);
        s.blob(b"a").unwrap().unwrap().append(&[0; 513]).unwrap();
    }

    #[test]
    fn rename_blob_shuffle_bloblist() {
        let mut s = init();
        s.create_blob(b"").unwrap().unwrap();
        s.create_blob(b"a").unwrap().unwrap();
        s.create_blob(b"b").unwrap().unwrap();
        s.blob(b"a").unwrap().unwrap().rename(b"").unwrap();
        s.blob(b"b").unwrap().unwrap().append(b"").unwrap();
    }

    #[test]
    fn log_overflow() {
        let mut s = init();
        let mut b = s.create_blob(b"").unwrap().unwrap();
        b.append(&[b'a'; 10000]).unwrap();
        b.append(&[b'b'; 20000]).unwrap();
        s = remount(s);
        let buf = &mut [0; 40000];
        let n = s.blob(b"").unwrap().unwrap().read_at(0, buf).unwrap();
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
        let mut s = init();
        let mut b = s.create_blob(b"").unwrap().unwrap();
        b.append(&[b'a'; 10000]).unwrap();
        b.append(&[b'b'; 20000]).unwrap();
        s = remount(s);
        s.blob(b"").unwrap().unwrap().delete().unwrap();
        remount(s);
    }

    #[test]
    fn log_overflow_load_zone_allocation_map() {
        let mut s = init();
        let mut b = s.create_blob(b"").unwrap().unwrap();
        // 42 * 512 = 21504
        // hence, assuming no "commit blob", this forcibly allocates a second log zone
        b.append(&[0; 30000]).unwrap();
        s = remount(s);
        b = s.blob(b"").unwrap().unwrap();
        // this breaks after a remount if *zone allocation* tracking isn't done properly
        b.append(&[0; 20000]).unwrap();
    }
}
