pub use rand;

use chacha20poly1305::{KeyInit, aead::AeadInPlace};
use core::fmt;
use rand::RngExt;
use std::{io, collections::BTreeMap};

type Tag = [u8; 16];
type Uuid = [u8; 16];
type Key = [u8; 32];
type Nonce = [u8; 12];

mod root {
    use crate::{Key, Tag, Uuid};
    use core::mem;
    use nora_endian::{u32le, u64le};

    #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
    #[repr(C)]
    pub struct Header {
        pub magic: [u8; 4],
        pub version: u32le,
        pub generation: u64le,
        pub keyslots: [KeySlot; 3],
        pub encrypted_area: EncryptedArea,
        pub encrypted_area_tag: Tag,
    }

    const _: () = assert!(mem::size_of::<Header>() == 512);

    pub type KeySlot = [u8; 80];

    #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
    #[repr(C)]
    pub struct KeySlotNone {
        pub ty: u8,
        pub _params: [u8; 79],
    }

    #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
    #[repr(C)]
    pub struct KeySlotArgon2id {
        pub ty: u8,
        pub _pad_0: [u8; 3],
        pub m_cost: u32le,
        pub t_cost: u32le,
        pub p_cost: u32le,
        pub salt: [u8; 16],
        pub header_key: Key,
        pub header_key_tag: Tag,
    }

    const _: () = assert!(mem::size_of::<KeySlotArgon2id>() == 80);

    #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
    #[repr(C)]
    pub struct EncryptedArea {
        pub zone_dev_uuid: Uuid,
        pub zone_size: u64le,
        pub log_zone_id: u32le,
        pub log_head: u32le,
        pub log_block_size: u32le,
        pub _pad_0: [u8; 28],
        pub log_key: Key,
        pub _pad_1: [u8; 144],
    }

    const _: () = assert!(mem::size_of::<EncryptedArea>() == 240);

    impl Header {
        pub const MAGIC: [u8; 4] = *b"ToaB";
        pub const VERSION: u32 = 0x20260307;
    }

    impl Default for Header {
        fn default() -> Self {
            Self {
                magic: [0; 4],
                version: 0.into(),
                generation: 0.into(),
                keyslots: [[0; 80]; 3],
                encrypted_area: Default::default(),
                encrypted_area_tag: Default::default(),
            }
        }
    }

    impl Default for EncryptedArea {
        fn default() -> Self {
            Self {
                zone_dev_uuid: Uuid::default(),
                zone_size: 0u64.into(),
                log_zone_id: 0u32.into(),
                log_head: 0u32.into(),
                log_block_size: 0u32.into(),
                _pad_0: [0u8; 28],
                log_key: Key::default(),
                _pad_1: [0u8; 144],
            }
        }
    }
}

mod log {
    pub mod entry {
        use crate::{Key, Tag};
        use core::mem;
        use nora_endian::{u16le, u32le, u64le};

        // finally found a usecase that ChatGPT is actually
        // reliable for. Just needs a few substitution fixes.

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct Nop {
            pub typ: u8,
            pub _pad_: [u8; 3],
            pub _pad_ding_size: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct CreateBlob {
            pub typ: u8,
            pub name_len: u8,
            pub blob_id: u16le,
            pub data_zone_count: u32le,
            pub table_zone_count: u32le,
            pub encryption_key: Key,
            pub nonce_high: u32le,
            // table_zones: u32le[]
            // data_zones: u32le[]
            // name: u8[]
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct DeleteBlob {
            pub typ: u8,
            pub _pad_0: u8,
            pub blob_id: u16le,
            pub _pad_1: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AddZoneToBlob {
            pub typ: u8,
            pub _pad_0: u8,
            pub blob_id: u16le,
            pub _pad_1: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AppendBlobTail {
            pub typ: u8,
            pub _pad_0: u8,
            pub blob_id: u16le,
            pub data_len: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct AllocateZone {
            pub typ: u8,
            pub _pad_0: u8,
            pub blob_id: u16le,
            pub zone_id: u32le,
        }

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        pub struct CommitBlobTail {
            pub typ: u8,
            pub compression_algorithm: u8,
            pub blob_id: u16le,
            pub compressed_size: u32le,
            pub offset: u64le,
            pub tag: Tag,
        }
    }
}

pub trait RootDev {
    fn write_at(&mut self, sector: u8, data: &[u8; 512]) -> io::Result<()>;
    fn read_at(&self, sector: u8) -> io::Result<[u8; 512]>;

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

    fn read_at<'a>(&'a self, offset: u64, len: usize) -> io::Result<Self::Read<'a>>;

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

pub struct BlobRootDecrypted<T> {
    root_dev: T,
    header: root::Header,
}

pub struct BlobStore<T, U> {
    root_dev: T,
    zone_dev: U,
    header: root::Header,
    /*
    // this is slow for allocation but blob creation should be infrequent anyway.
    blobs: Vec<Option<Blob>>,
    tail: Vec<u8>,
    */
}

pub struct MemRoot {
    sectors: Box<[[u8; 512]]>,
}

pub struct MemZones {
    buffer: Box<[u8]>,
    zone_size: usize,
}

pub enum BlockShift {
    N9,
    N12,
}

pub enum UnlockMethod {
    Argon2id {
        p: u32,
        t: u32,
        s: u32,
        salt: [u8; 16],
    },
}

pub struct UnlockError<T>(pub BlobRoot<T>);

pub enum KeySlot {
    N0 = 0,
    N1 = 1,
    N2 = 2,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobId(u16);

struct DecryptError;

struct Blob {
    key: Key,
    data_zones: Vec<Zone>,
    table_zones: Vec<Zone>,
    name: Box<str>,
    nonce_high: u32,
}

struct Zone {
    /// High bits are ID, low bits are head.
    ///
    /// Bit allocation depends on zone size.
    id_head: u64,
}

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

    pub fn unlock_methods(&self) -> [Option<UnlockMethod>; 3] {
        [None, None, None]
    }

    pub fn unlock(
        mut self,
        key: &Key,
        slot: KeySlot,
    ) -> Result<BlobRootDecrypted<T>, UnlockError<T>> {
        let slot = &self.header.keyslots[slot as usize];
        match slot[0] {
            1 => {
                let slot = bytemuck::cast_ref::<_, root::KeySlotArgon2id>(slot);
                let mut header_key = slot.header_key;
                match decrypt(key, &[0; 12], &slot.header_key_tag, &mut header_key) {
                    Ok(()) => {
                        let enc = bytemuck::bytes_of_mut(&mut self.header.encrypted_area);
                        let nonce = &nonce_64_32(self.header.generation.into(), 0);
                        if decrypt(&header_key, nonce, &self.header.encrypted_area_tag, enc).is_ok()
                        {
                            Ok(BlobRootDecrypted {
                                header: self.header,
                                root_dev: self.root_dev,
                            })
                        } else {
                            Err(UnlockError(self))
                        }
                    }
                    Err(self::DecryptError) => Err(UnlockError(self)),
                }
            }
            _ => Err(UnlockError(self)),
        }
    }
}

impl<T> BlobRootDecrypted<T>
where
    T: RootDev,
{
    pub fn zone_dev_uuid(&self) -> Uuid {
        self.header.encrypted_area.zone_dev_uuid
    }

    pub fn with_zone_dev<U>(self, zone_dev: U) -> io::Result<BlobStore<T, U>>
    where
        U: ZoneDev,
    {
        Ok(BlobStore {
            root_dev: self.root_dev,
            zone_dev,
            header: self.header,
        })
    }
}

impl<T, U> BlobStore<T, U>
where
    T: RootDev,
    U: ZoneDev,
{
    pub fn init<R>(
        mut rng: R,
        mut root_dev: T,
        mut zone_dev: U,
        zone_dev_uuid: Uuid,
        zone_size: u64,
    ) -> io::Result<Self>
    where
        R: rand::CryptoRng,
    {
        root_dev.zeroize()?;
        zone_dev.clear()?;

        let mut header_key = rng.random();
        let log_key = rng.random();
        let generation = 1;

        let unencrypted_area @ mut encrypted_area = root::EncryptedArea {
            _pad_0: Default::default(),
            _pad_1: [0; 144],
            zone_dev_uuid,
            zone_size: zone_size.into(),
            log_zone_id: 0.into(),
            log_block_size: 0.into(),
            log_head: 0.into(),
            log_key,
        };
        let encrypted_area_tag = encrypt(
            &header_key,
            &nonce_64_32(generation, 0),
            bytemuck::bytes_of_mut(&mut encrypted_area),
        );

        let header_key_tag = encrypt(&[0; 32], &[0; 12], &mut header_key);
        let salt = Default::default(); // TODO
        let mut header = root::Header {
            magic: root::Header::MAGIC,
            version: root::Header::VERSION.into(),
            generation: generation.into(),
            encrypted_area,
            encrypted_area_tag,
            keyslots: [
                bytemuck::cast(root::KeySlotArgon2id {
                    ty: 1,
                    _pad_0: Default::default(),
                    m_cost: 20_000.into(),
                    t_cost: 2.into(),
                    p_cost: 4.into(),
                    header_key,
                    header_key_tag,
                    salt,
                }),
                [0; 80],
                [0; 80],
            ],
        };
        root_dev.write_at(0, bytemuck::cast_ref(&header))?;
        header.encrypted_area = unencrypted_area;

        Ok(Self {
            root_dev,
            zone_dev,
            header,
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    pub fn unmount(mut self) -> Result<(T, U), (Self, io::Error)> {
        if let Err(e) = self.flush() {
            return Err((self, e));
        }
        Ok((self.root_dev, self.zone_dev))
    }

    /*
    pub fn create_blob(&mut self, name: &[u8]) -> io::Result<BlobId> {
        self
    }
    */
}

impl RootDev for MemRoot {
    fn write_at(&mut self, sector: u8, data: &[u8; 512]) -> io::Result<()> {
        self.sectors[usize::from(sector)] = *data;
        Ok(())
    }

    fn read_at(&self, sector: u8) -> io::Result<[u8; 512]> {
        Ok(self.sectors[usize::from(sector)])
    }

    fn block_shift(&self) -> BlockShift {
        BlockShift::N9
    }

    fn highest_sector(&self) -> u8 {
        (self.sectors.len() - 1) as u8
    }

    fn zeroize(&mut self) -> io::Result<()> {
        self.sectors.fill([0; 512]);
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
}

impl MemRoot {
    pub fn new(highest_sector: u8) -> Self {
        Self {
            sectors: vec![[0; 512]; usize::from(highest_sector) + 1].into(),
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

impl<T> fmt::Debug for UnlockError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(stringify!(UnlockError))
    }
}

fn decrypt(key: &Key, nonce: &Nonce, tag: &Tag, data: &mut [u8]) -> Result<(), DecryptError> {
    let cipher = chacha20poly1305::ChaCha12Poly1305::new(key.into());
    cipher
        .decrypt_in_place_detached(nonce.into(), &[], data, tag.into())
        .map_err(|_| DecryptError)
}

fn encrypt(key: &Key, nonce: &Nonce, data: &mut [u8]) -> Tag {
    let cipher = chacha20poly1305::ChaCha12Poly1305::new(key.into());
    let tag = cipher
        .encrypt_in_place_detached(nonce.into(), &[], data)
        .expect("encryption failure");
    tag.into()
}

fn nonce_64_32(x: u64, y: u32) -> Nonce {
    let mut z = [0; 12];
    z[..8].copy_from_slice(&x.to_le_bytes());
    z[8..].copy_from_slice(&y.to_le_bytes());
    z
}

#[cfg(test)]
mod test {
    use super::*;

    const ZONE_UUID: Uuid = *b"AbracadabraKapow";

    #[test]
    fn remount() {
        let store = BlobStore::init(
            rand::rng(),
            MemRoot::new(5),
            MemZones::new(1 << 20, 10),
            ZONE_UUID,
            1 << 20,
        )
        .unwrap();
        let key = [0; 32];

        let (root_dev, zone_dev) = store.unmount().map_err(|e| e.1).unwrap();
        let root = BlobRoot::load(root_dev).unwrap();
        let root = root.unlock(&key, KeySlot::N0).unwrap();
        let _ = root.with_zone_dev(zone_dev).unwrap();
    }
}
