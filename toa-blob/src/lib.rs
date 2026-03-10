use core::fmt;
use std::io;

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
            pub blob_id: u16le,
            pub _pad_0: u32le,
            // table_zones: u32le[]
            // data_zones: u32le[]
            // name: u8[]
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct DeleteBlob {
            pub ty: u8,
            pub _pad_0: u8,
            pub blob_id: u16le,
            pub _pad_1: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AddZoneToBlob {
            pub ty: u8,
            pub _pad_0: u8,
            pub blob_id: u16le,
            pub _pad_1: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AppendBlobTail {
            pub ty: u8,
            pub _pad_0: u8,
            pub blob_id: u16le,
            pub data_len: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AllocateZone {
            pub ty: u8,
            pub _pad_0: u8,
            pub blob_id: u16le,
            pub zone_id: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct CommitBlobTail {
            pub ty: u8,
            pub compression_algorithm: u8,
            pub blob_id: u16le,
            pub compressed_size: u32le,
            pub offset: u64le,
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
    // this is slow for allocation but blob creation should be infrequent anyway.
    blobs: Vec<Option<Blob>>,
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobId(pub u16);

pub struct BlobRef<'a, T> {
    store: &'a mut T,
    id: BlobId,
}

struct DecryptError;

/// # Note about zone data alignment
///
/// Data is *not* aligned to block boundaries.
/// This is to maximize compression density and simplify the interface.
///
/// To ensure blocks are written as a whole there is a second tail buffer,
/// which is appended to until it is block-sized.
struct Blob {
    data_zones: Vec<ZoneId>,
    table_zones: Vec<ZoneId>,
    name: Box<[u8]>,
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
        let mut blobs = Vec::new();

        let mut buf = vec![0; zone_dev.block_shift().into()];
        let mut i = 0;
        while i < self.header.log_head() {
            let rd = zone_dev.read_at(i, buf.len())?;
            buf.copy_from_slice(rd.as_ref());
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
                        k += 1;
                        let name_len = usize::from(b);
                        let blob_id = usize::from(u16::from_le_bytes([c, d]));
                        let name = &buf[k..].as_flattened()[..name_len];
                        k += (name_len + 7) >> 3;
                        let n = blobs.len().max(1 + blob_id);
                        blobs.resize_with(n, || None);
                        blobs[blob_id] = Some(Blob::new(name));
                    }
                    ty => todo!("{ty}"),
                }
            }
        }

        Ok(BlobStore {
            root_dev: self.root_dev,
            zone_dev,
            header: self.header,
            blobs,
            log: Vec::new(),
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
            header,
            blobs: Vec::new(),
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

    pub fn blob(&mut self, id: BlobId) -> io::Result<Option<BlobRef<'_, Self>>> {
        match self
            .blobs
            .get(usize::from(id.0))
            .is_some_and(|x| x.is_some())
        {
            false => Ok(None),
            true => Ok(Some(BlobRef { store: self, id })),
        }
    }

    pub fn create_blob(&mut self, name: &[u8]) -> io::Result<BlobId> {
        assert!(name.len() <= 255, "name too long");
        let idx = self.blobs.iter().position(|x| x.is_none());
        let id = if let Some(idx) = idx {
            BlobId(idx as u16)
        } else {
            // TODO return error if too many blobs
            let id = BlobId(self.blobs.len().try_into().unwrap());
            self.blobs.push(None);
            id
        };
        let blob = Blob::new(name);
        self.log_create_blob(id, &blob)?;
        self.blobs[usize::from(id.0)] = Some(blob);
        Ok(id)
    }

    fn log_create_blob(&mut self, id: BlobId, blob: &Blob) -> io::Result<()> {
        let hdr = log::entry::CreateBlob {
            ty: log::entry::ty::CREATE_BLOB,
            blob_id: id.0.into(),
            name_len: u8::try_from(blob.name.len()).unwrap().into(),
            _pad_0: Default::default(),
        };
        let hdr = bytemuck::bytes_of(&hdr);
        let len = hdr.len() + blob.name.len();
        self.log_reserve(len)?;
        self.log.extend(hdr);
        self.log.extend(&blob.name);
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
        self.header.log_zone_id_head += nora_endian::u64le::from(self.log.len() as u64);
        self.log.clear();
        Ok(())
    }

    fn log_pad(&mut self) {
        let n = self.log.len();
        let n = (n + 7) & !7;
        self.log.resize(n, 0);
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
    fn new(name: &[u8]) -> Self {
        Self {
            data_zones: Vec::new(),
            table_zones: Vec::new(),
            new_tail: Vec::new(),
            compressed_tail: Vec::new(),
            name: name.into(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const ZONE_UUID: Uuid = *b"AbracadabraKapow";

    fn remount(mut store: BlobStore<MemRoot, MemZones>) -> BlobStore<MemRoot, MemZones> {
        let (root_dev, zone_dev) = store.unmount().map_err(|e| e.1).unwrap();
        let root = BlobRoot::load(root_dev).unwrap();
        root.with_zone_dev(zone_dev).unwrap()
    }

    #[test]
    fn empty() {
        let store = BlobStore::init(
            MemRoot::new(5),
            MemZones::new(1 << 20, 10),
            ZONE_UUID,
            1 << 20,
        )
        .unwrap();
        let _ = remount(store);
    }

    #[test]
    fn create_blobs() {
        let mut store = BlobStore::init(
            MemRoot::new(5),
            MemZones::new(1 << 20, 10),
            ZONE_UUID,
            1 << 20,
        )
        .unwrap();
        store.create_blob(b"a").unwrap();
        store.create_blob(b"b").unwrap();
        store.blob(BlobId(0)).unwrap().expect("missing blob 0");
        store.blob(BlobId(1)).unwrap().expect("missing blob 1");
        store = remount(store);
        store.blob(BlobId(0)).unwrap().expect("missing blob 0");
        store.blob(BlobId(1)).unwrap().expect("missing blob 1");
        store = remount(store);
        store.create_blob(b"c").unwrap();
        store.blob(BlobId(0)).unwrap().expect("missing blob 0");
        store.blob(BlobId(1)).unwrap().expect("missing blob 1");
        store.blob(BlobId(2)).unwrap().expect("missing blob 2");
        store = remount(store);
        store.blob(BlobId(0)).unwrap().expect("missing blob 0");
        store.blob(BlobId(1)).unwrap().expect("missing blob 1");
        store.blob(BlobId(2)).unwrap().expect("missing blob 2");
    }
}
