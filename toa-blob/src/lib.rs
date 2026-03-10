use nora_endian::u64le;
use std::{
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
            pub compression_algorithm: u8,
            pub _pad_0: [u8; 2],
            pub blob_index: u32le,
            pub offset: u64le,
            pub compressed_size: u32le,
            pub _pad_1: [u8; 4],
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
    fn write_at<'a>(&'a mut self, offset: u64, data: &[u8]) -> io::Result<()>;

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
    header: root::Header,
    blobs: Vec<Blob>,
    blob_map: BTreeMap<Rc<[u8]>, u32>,
    log: Vec<u8>,
}

pub struct MemRoot {
    sectors: Box<[[u8; 64]]>,
}

pub struct MemZones {
    buffer: Box<[u8]>,
    zone_size: usize,
}

pub enum BlockShift {
    N9 = 1 << 9,
    N12 = 1 << 12,
}

pub struct BlobRef<'a, T> {
    store: &'a mut T,
    index: u32,
}

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
    data_zones: Vec<ZoneId>,
    table_zones: Vec<ZoneId>,
    /// Data appended *before* compression
    new_tail: Vec<u8>,
    /// Data appended *after* compression
    compressed_tail: Vec<u8>,
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
        let mut store = BlobStore {
            root_dev: self.root_dev,
            zone_dev,
            header: self.header,
            blobs: Default::default(),
            blob_map: Default::default(),
            log: Vec::new(),
        };

        let mut buf = vec![0; store.zone_dev.block_shift().into()];
        let mut i = 0;
        while i < self.header.log_head() {
            let rd = store.zone_dev.read_at(i, buf.len())?;
            buf.copy_from_slice(rd.as_ref());
            drop(rd);
            i += u64::from(store.zone_dev.block_shift());

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

        Ok(store)
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
            header,
            blobs: Default::default(),
            blob_map: Default::default(),
            log: Vec::new(),
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        // FIXME round & round...
        self.log_flush()?;
        self.root_dev
            .write_at(0, bytemuck::cast_ref(&self.header))?;
        Ok(())
    }

    pub fn unmount(mut self) -> Result<(T, U), (Self, io::Error)> {
        if let Err(e) = self.flush() {
            return Err((self, e));
        }
        Ok((self.root_dev, self.zone_dev))
    }

    pub fn blob(&mut self, name: &[u8]) -> io::Result<Option<BlobRef<'_, Self>>> {
        assert!(name.len() <= 255, "name too long");
        match self.blob_map.get(name) {
            None => Ok(None),
            Some(&index) => Ok(Some(BlobRef { store: self, index })),
        }
    }

    pub fn create_blob<'a>(
        &'a mut self,
        name: &[u8],
    ) -> io::Result<Result<BlobRef<'a, Self>, DuplicateBlob>> {
        match self.replay_create_blob(name) {
            Ok(index) => self
                .log_create_blob(name)
                .map(|()| Ok(BlobRef { store: self, index })),
            Err(e) => Ok(Err(e)),
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

    fn replay_append_blob(&mut self, index: u32, data: &[u8]) {
        self.blobs[index as usize].new_tail.extend(data);
    }

    fn log_create_blob(&mut self, name: &[u8]) -> io::Result<()> {
        let hdr = log::entry::CreateBlob {
            ty: log::entry::ty::CREATE_BLOB,
            name_len: u8::try_from(name.len()).unwrap().into(),
        };
        self.log_push(&[bytemuck::bytes_of(&hdr), name])
    }

    fn log_delete_blob(&mut self, index: u32) -> io::Result<()> {
        let hdr = log::entry::DeleteBlob {
            ty: log::entry::ty::DELETE_BLOB,
            _pad_0: Default::default(),
            blob_index: index.into(),
        };
        self.log_push(&[bytemuck::bytes_of(&hdr)])
    }

    fn log_append_blob_tail(&mut self, index: u32, data: &[u8]) -> io::Result<()> {
        let len = u16::try_from(data.len()).unwrap(); // FIXME pre-split data
        let hdr = log::entry::AppendBlobTail {
            ty: log::entry::ty::APPEND_BLOB_TAIL,
            _pad_0: Default::default(),
            data_len: len.into(),
            blob_index: index.into(),
        };
        self.log_push(&[bytemuck::bytes_of(&hdr), data])
    }

    fn log_push(&mut self, data: &[&[u8]]) -> io::Result<()> {
        let len = data.iter().fold(0, |s, x| s + x.len());
        self.log_reserve(len)?;
        self.log.extend(data.iter().copied().flatten());
        self.log_pad();
        Ok(())
    }

    fn log_reserve(&mut self, num: usize) -> io::Result<()> {
        let num = (num + 7) & !7;
        let len = (self.log.len() + num) as u64;
        if len > self.header.block_size {
            self.log_flush()?;
        }
        Ok(())
    }

    fn log_flush(&mut self) -> io::Result<()> {
        if self.log.is_empty() {
            return Ok(());
        }
        let max_len = u64::from(self.header.block_size) as usize;
        assert!(
            self.log.len() <= max_len,
            "{} <= {}",
            self.log.len(),
            max_len
        );
        // TODO optimize with long NOPs
        self.log.resize(max_len, 0);
        self.zone_dev.write_at(self.header.log_head(), &self.log)?;
        self.header.log_zone_id_head += u64le::from(self.log.len() as u64);
        self.log.clear();
        Ok(())
    }

    fn log_pad(&mut self) {
        let n = self.log.len();
        let n = (n + 7) & !7;
        self.log.resize(n, 0);
    }

    fn log_free(&mut self) -> usize {
        u64::from(self.header.block_size) as usize - self.log.len()
    }
}

impl<'a, T, U> BlobRef<'a, BlobStore<T, U>>
where
    T: RootDev,
    U: ZoneDev,
{
    pub fn delete(self) -> io::Result<()> {
        self.store.replay_delete_blob(self.index);
        self.store.log_delete_blob(self.index)
    }

    pub fn append(&mut self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            if self.store.log_free() == 0 {
                self.store.log_flush()?;
            }
            let wr;
            let n = (self.store.log_free() - 8).min(data.len());
            (wr, data) = data.split_at(n);
            self.store.replay_append_blob(self.index, wr);
            self.store.log_append_blob_tail(self.index, wr)?;
        }
        Ok(())
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let s = usize::try_from(offset)
            .ok()
            .and_then(|x| self.blob().new_tail.get(x..))
            .unwrap_or(&[]);
        let n = s.len().min(buf.len());
        buf[..n].copy_from_slice(&s[..n]);
        Ok(n)
    }

    fn blob(&self) -> &Blob {
        &self.store.blobs[self.index as usize]
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
        = &'a [u8]
    where
        Self: 'a;

    fn read_at<'a>(&'a self, offset: u64, len: usize) -> io::Result<Self::Read<'a>> {
        let offset = offset as usize;
        Ok(&self.buffer[offset..offset + len])
    }

    fn write_at<'a>(&'a mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        let offset = offset as usize;
        self.buffer[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn block_shift(&self) -> BlockShift {
        // TODO
        BlockShift::N9
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
            buffer: vec![0; len].into(),
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
            data_zones: Vec::new(),
            table_zones: Vec::new(),
            new_tail: Vec::new(),
            compressed_tail: Vec::new(),
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
}
