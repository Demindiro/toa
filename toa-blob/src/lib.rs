use nora_endian::u64le;
use std::{
    cell::RefCell,
    collections::btree_map::{BTreeMap, Entry},
    io,
    rc::Rc,
};

type Uuid = [u8; 16];

mod root {
    use crate::Uuid;
    use core::mem;
    use nora_endian::{u32le, u64le};

    #[derive(Clone, Copy, Default, bytemuck::Pod, bytemuck::Zeroable)]
    #[repr(C)]
    pub struct Header {
        pub magic: [u8; 4],
        pub version: u32le,
        pub generation: u64le,
        pub zone_dev_uuid: Uuid,
        pub zone_size: u64le,
        pub block_size: u64le,
        pub log_zone_id_head: u64le,
        pub _pad_0: u64le,
    }

    const _: () = assert!(mem::size_of::<Header>() == 64);

    impl Header {
        pub const MAGIC: [u8; 4] = *b"ToaB";
        pub const VERSION: u32 = 0x20260307;

        pub fn log_head(&self) -> u64 {
            (self.log_zone_id_head % self.zone_size).into()
        }

        pub fn log_zone_id(&self) -> u64 {
            (self.log_zone_id_head / self.zone_size).into()
        }
    }
}

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
    }
}

pub trait RootDev {
    fn write_at(&mut self, sector: u8, data: &[u8; 64]) -> io::Result<()>;
    fn read_at(&self, sector: u8) -> io::Result<[u8; 64]>;

    /// 512 = 2**9
    /// 4096 = 2**12
    fn block_shift(&self) -> BlockShift;
    fn highest_sector(&self) -> u8;

    /// Completely fill the device with zeros.
    fn zeroize(&mut self) -> io::Result<()>;
}

pub trait ZoneDev {
    type Read<'a>: AsRef<[u8]>
    where
        Self: 'a;

    /// # Note
    ///
    /// `offset` and `len` are in *bytes*.
    fn read_at<'a>(&'a self, offset: u64, len: usize) -> io::Result<Self::Read<'a>>;

    /// # Note
    ///
    /// `offset` is in *bytes*.
    ///
    /// This method should panic if the offset is not aligned
    /// to a block boundary, as it is a severe logic error.
    // TODO extra copy is very bad and sad :(
    fn write_at<'a>(&'a self, offset: u64, data: &[u8]) -> io::Result<()>;

    fn block_shift(&self) -> BlockShift;

    /// Wipe all zones. This may be a noop, but zones must be writeable
    /// from the start after this call.
    fn clear(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct BlobRoot<T> {
    root_dev: T,
    header: root::Header,
}

pub struct BlobStore<T, U> {
    root_dev: T,
    zone_dev: U,
    data: RefCell<BlobStoreData>,
}

struct BlobStoreData {
    header: root::Header,
    blobs: Vec<Blob>,
    blob_map: BTreeMap<Rc<[u8]>, u32>,
    log: Vec<u8>,
}

pub struct MemRoot {
    sectors: Box<[[u8; 64]]>,
}

pub struct MemZones {
    buffer: RefCell<Box<[u8]>>,
    zone_size: usize,
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

struct Zone {
    /// High bits are ID, low bits are head.
    ///
    /// Bit allocation depends on zone size.
    id_head: u64,
}

struct ZoneId(u32);

impl<T> BlobRoot<T>
where
    T: RootDev,
{
    pub fn load(root_dev: T) -> io::Result<Self> {
        let mut header = root::Header::default();
        for i in 0..=root_dev.highest_sector() {
            let hdr = bytemuck::cast::<_, root::Header>(root_dev.read_at(i)?);
            if hdr.generation > header.generation {
                header = hdr;
            }
        }
        Ok(Self { root_dev, header })
    }

    pub fn zone_dev_uuid(&self) -> Uuid {
        self.header.zone_dev_uuid
    }

    pub fn with_zone_dev<U>(self, zone_dev: U) -> io::Result<BlobStore<T, U>>
    where
        U: ZoneDev,
    {
        let mut store = BlobStoreData {
            header: self.header,
            blobs: Default::default(),
            blob_map: Default::default(),
            log: Vec::new(),
        };

        let mut buf = vec![0; zone_dev.block_shift().into()];
        let mut i = 0;
        while i < self.header.log_head() {
            let rd = zone_dev.read_at(i, buf.len())?;
            buf.copy_from_slice(rd.as_ref());
            drop(rd);
            i += u64::from(zone_dev.block_shift());

            let mut k = 0;
            let (buf, []) = buf.as_chunks::<8>() else {
                unreachable!()
            };
            while let Some(x) = buf.get(k) {
                let [ty, b, c, d, e, f, g, h] = *x;
                match ty {
                    log::entry::ty::NOP => {
                        k += 1 + ((u32::from_le_bytes([e, f, g, h]) as usize) >> 3);
                    }
                    log::entry::ty::CREATE_BLOB => {
                        let name_len = usize::from(b);
                        let name = &buf[k..].as_flattened()[2..2 + name_len];
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
                        let name = &buf[k..].as_flattened()[..usize::from(name_len)];
                        store.replay_rename_blob(idx, name);
                        k += (name_len + 7) >> 3;
                    }
                    log::entry::ty::APPEND_BLOB_TAIL => {
                        k += 1;
                        let len = usize::from(u16::from_le_bytes([c, d]));
                        let idx = u32::from_le_bytes([e, f, g, h]);
                        let data = &buf[k..].as_flattened()[..usize::from(len)];
                        store.replay_append_blob(idx, data);
                        k += (len + 7) >> 3;
                    }
                    ty => todo!("{ty}"),
                }
            }
        }

        Ok(BlobStore {
            root_dev: self.root_dev,
            zone_dev,
            data: store.into(),
        })
    }
}

impl<T, U> BlobStore<T, U>
where
    T: RootDev,
    U: ZoneDev,
{
    pub fn init(
        mut root_dev: T,
        mut zone_dev: U,
        zone_dev_uuid: Uuid,
        zone_size: u64,
    ) -> io::Result<Self> {
        root_dev.zeroize()?;
        zone_dev.clear()?;

        let header = root::Header {
            magic: root::Header::MAGIC,
            version: root::Header::VERSION.into(),
            generation: 1.into(),
            zone_dev_uuid,
            zone_size: zone_size.into(),
            block_size: u64::from(zone_dev.block_shift()).into(),
            log_zone_id_head: 0.into(),
            _pad_0: Default::default(),
        };
        root_dev.write_at(0, bytemuck::cast_ref(&header))?;

        Ok(Self {
            root_dev,
            zone_dev,
            data: BlobStoreData::new(header).into(),
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        // FIXME round & round...
        self.log_flush()?;
        self.root_dev
            .write_at(0, bytemuck::cast_ref(&self.data.borrow().header))?;
        Ok(())
    }

    pub fn unmount(mut self) -> Result<(T, U), (Self, io::Error)> {
        if let Err(e) = self.flush() {
            return Err((self, e));
        }
        Ok((self.root_dev, self.zone_dev))
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
        Ok(data.log.len() as u64 + data.header.log_head())
    }

    pub fn clear(&mut self) -> io::Result<()> {
        self.root_dev.zeroize()?;
        self.zone_dev.clear()?;
        let mut data = self.data.borrow_mut();
        data.blobs.clear();
        data.blob_map.clear();
        data.log.clear();
        data.header.log_zone_id_head = 0.into();
        Ok(())
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
        if len > self.data.borrow().header.block_size {
            self.log_flush()?;
        }
        Ok(())
    }

    fn log_flush(&self) -> io::Result<()> {
        let data = &mut *self.data.borrow_mut();
        if data.log.is_empty() {
            return Ok(());
        }
        let max_len = u64::from(data.header.block_size) as usize;
        assert!(
            data.log.len() <= max_len,
            "{} <= {}",
            data.log.len(),
            max_len
        );
        // TODO optimize with long NOPs
        data.log.resize(max_len, 0);
        self.zone_dev.write_at(data.header.log_head(), &data.log)?;
        data.header.log_zone_id_head += u64le::from(data.log.len() as u64);
        data.log.clear();
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
        u64::from(data.header.block_size) as usize - data.log.len()
    }
}

impl BlobStoreData {
    fn new(header: root::Header) -> Self {
        Self {
            header,
            blobs: Default::default(),
            blob_map: Default::default(),
            log: Vec::new(),
        }
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

impl<'a, T, U> BlobRef<'a, BlobStore<T, U>>
where
    T: RootDev,
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

impl RootDev for MemRoot {
    fn write_at(&mut self, sector: u8, data: &[u8; 64]) -> io::Result<()> {
        self.sectors[usize::from(sector)] = *data;
        Ok(())
    }

    fn read_at(&self, sector: u8) -> io::Result<[u8; 64]> {
        Ok(self.sectors[usize::from(sector)])
    }

    fn block_shift(&self) -> BlockShift {
        BlockShift::N9
    }

    fn highest_sector(&self) -> u8 {
        (self.sectors.len() - 1) as u8
    }

    fn zeroize(&mut self) -> io::Result<()> {
        self.sectors.fill([0; 64]);
        Ok(())
    }
}

impl ZoneDev for MemZones {
    type Read<'a>
        = MemZonesRef<'a>
    where
        Self: 'a;

    fn read_at<'a>(&'a self, offset: u64, len: usize) -> io::Result<Self::Read<'a>> {
        let offset = offset as usize;
        let x = self.buffer.borrow();
        let x = core::cell::Ref::map(x, |x| &x[offset..offset + len]);
        Ok(MemZonesRef(x))
    }

    fn write_at<'a>(&'a self, offset: u64, data: &[u8]) -> io::Result<()> {
        let offset = offset as usize;
        let mut buf = self.buffer.borrow_mut();
        buf[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn block_shift(&self) -> BlockShift {
        // TODO
        BlockShift::N9
    }
}

impl<'a> AsRef<[u8]> for MemZonesRef<'a> {
    fn as_ref(&self) -> &[u8] {
        &*self.0
    }
}

impl MemRoot {
    pub fn new(highest_sector: u8) -> Self {
        Self {
            sectors: vec![[0; 64]; usize::from(highest_sector) + 1].into(),
        }
    }
}

impl MemZones {
    pub fn new(zone_size: usize, zone_count: usize) -> Self {
        let len = zone_size
            .checked_mul(zone_count)
            .expect("zone size*count overflow");
        Self {
            buffer: RefCell::new(vec![0; len].into()),
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

    const ZONE_UUID: Uuid = *b"AbracadabraKapow";

    fn init() -> BlobStore<MemRoot, MemZones> {
        BlobStore::init(
            MemRoot::new(5),
            MemZones::new(1 << 20, 10),
            ZONE_UUID,
            1 << 20,
        )
        .unwrap()
    }

    fn remount(store: BlobStore<MemRoot, MemZones>) -> BlobStore<MemRoot, MemZones> {
        let (root_dev, zone_dev) = store.unmount().map_err(|e| e.1).unwrap();
        let root = BlobRoot::load(root_dev).unwrap();
        root.with_zone_dev(zone_dev).unwrap()
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
        dbg!(&s.data.borrow().blob_map);
        s.blob(b"a").unwrap().unwrap().rename(b"").unwrap();
        s.blob(b"b").unwrap().unwrap().append(b"").unwrap();
    }
}
