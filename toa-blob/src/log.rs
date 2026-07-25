use crate::{BlobId, ZoneDev, ZoneId};
use bstr::BStr;
use core::fmt;
use std::io;

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
        8 CREATE_UNZONED_BLOB
        9 CLEAR_BLOB
        10 TRANSACTION_BEGIN
        11 TRANSACTION_END
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
        pub _pad_0: [u8; 2],
        pub blob_id: u32le,
        // name: u8[]
    }

    #[repr(C)]
    #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
    pub struct ClearBlob {
        pub ty: u8,
        pub _pad_0: [u8; 3],
        pub blob_id: u32le,
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

    #[repr(C)]
    #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
    pub struct TransactionBegin {
        pub ty: u8,
        pub _pad_0: [u8; 7],
    }

    #[repr(C)]
    #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
    pub struct TransactionEnd {
        pub ty: u8,
        pub _pad_0: [u8; 7],
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

pub enum LogEntry<'a> {
    CreateBlob {
        id: BlobId,
        name: &'a [u8],
        unzoned: bool,
    },
    ClearBlob {
        id: BlobId,
    },
    DeleteBlob {
        id: BlobId,
    },
    RenameBlob {
        id: BlobId,
        name: &'a [u8],
    },
    AppendBlobTail {
        id: BlobId,
        data: &'a [u8],
    },
    AddZoneToBlob {
        id: BlobId,
        zone: ZoneId,
    },
    CommitBlobTail {
        id: BlobId,
        len: u64,
    },
    NextLogZone {
        zones: [ZoneId; 2],
    },
    TransactionBegin,
    TransactionEnd,
}

pub struct LogEnd {
    pub generation: u64,
    pub len: u64,
    pub zone_head: u64,
}

impl fmt::Display for LogEntry<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateBlob { id, name, unzoned } => {
                let name = BStr::new(name);
                write!(f, "create blob {id} {name:?} unzoned:{unzoned}")
            }
            Self::ClearBlob { id } => write!(f, "clear blob {id}"),
            Self::DeleteBlob { id } => write!(f, "delete blob {id}"),
            Self::RenameBlob { id, name } => {
                let name = BStr::new(name);
                write!(f, "rename blob {id} {name:?}")
            }
            Self::AppendBlobTail { id, data } => {
                let data = BStr::new(data);
                write!(f, "append blob tail {id} {data:?}")
            }
            Self::AddZoneToBlob { id, zone } => write!(f, "add zone to blob {id} {zone}"),
            Self::CommitBlobTail { id, len } => write!(f, "commit blob tail {id} {len}"),
            Self::NextLogZone { zones: [a, b] } => write!(f, "next log zone {a} {b}"),
            Self::TransactionBegin => write!(f, "transaction begin"),
            Self::TransactionEnd => write!(f, "transaction end"),
        }
    }
}

impl fmt::Display for LogEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            generation,
            len,
            zone_head,
        } = self;
        write!(f, "generation:{generation} len:{len} zone_head:{zone_head}")
    }
}

pub fn iter_with<'a, T, F>(dev: &T, mut cb: F) -> io::Result<LogEnd>
where
    T: ZoneDev,
    F: FnMut(LogEntry) -> io::Result<()>,
{
    trace!("parsing log...");
    let block_size = usize::from(dev.block_size());
    let zone_blocks = u64::from(dev.zone_blocks());
    let zone_size = zone_blocks * block_size as u64;

    let mut visited_zones = bitvec::bitvec![0; dev.zone_count().get() as usize];

    let mut buf = vec![0; block_size * 2];
    let (block_a, block_b) = buf.split_at_mut(block_size);

    let mut log_zone_a = 0;
    let mut log_zone_b = u32::from(dev.zone_count()) - 1;
    // TODO check write pointer first
    dev.read_at(log_zone_a, 0, block_a)?;
    dev.read_at(log_zone_b, 0, block_b)?;

    let mut gen_a @ mut gen_b = 0;
    for (genn, blk) in [(&mut gen_a, &block_a), (&mut gen_b, &block_b)] {
        let hdr = &blk[..core::mem::size_of::<entry::Header>()];
        let hdr = bytemuck::from_bytes::<entry::Header>(hdr);

        if hdr.magic != entry::Header::MAGIC {
            Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"))?;
        }
        if hdr.version != entry::Header::VERSION {
            Err(io::Error::new(io::ErrorKind::InvalidData, "bad version"))?;
        }

        if hdr.block_size != u32::from(dev.block_size()) {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "block size mismatch",
            ))?;
        }
        if hdr.zone_blocks != dev.zone_blocks() {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zone blocks mismatch",
            ))?;
        }
        if hdr.zone_count != dev.zone_count().get() {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zone count mismatch",
            ))?;
        }

        *genn = hdr.generation.into();
    }
    assert_eq!(gen_a, gen_b); // TODO don't panic, return error

    let mut log_len = 0;
    let mut log_zone_head = 0;
    let mut log_end = dev.zone_write_head(log_zone_a)?.unwrap_or(zone_size);
    while log_zone_head < log_end {
        let (block_a, block_b) = buf.split_at_mut(block_size);

        log_zone_head += block_size as u64;

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
            trace!("log entry type={ty} zone={log_zone_a} offset={k}");
            end_of_log &= ty == entry::ty::LOG_BLOCK_END;
            // FIXME ensure log entries are equal *except* NEXT_LOG_ZONE
            // we should have a helper function which just returns an entry,
            // that way we can do a simple (==) check
            match ty {
                entry::ty::LOG_BLOCK_END => break,
                entry::ty::CREATE_BLOB | entry::ty::CREATE_UNZONED_BLOB => {
                    let hdr = bytemuck::cast::<_, entry::CreateBlob>(*x);
                    k += 1;
                    let id = BlobId(hdr.blob_id.into());
                    let name_len = usize::from(b);
                    let name = buf_a[k..].as_flattened().get(..name_len).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "log entry: create blob: missing name data",
                        )
                    })?;
                    k += (name_len + 7) >> 3;
                    let unzoned = ty == entry::ty::CREATE_UNZONED_BLOB;
                    (cb)(LogEntry::CreateBlob { id, name, unzoned })?;
                }
                entry::ty::CLEAR_BLOB => {
                    k += 1;
                    let id = BlobId(u32::from_le_bytes([e, f, g, h]));
                    (cb)(LogEntry::ClearBlob { id })?;
                }
                entry::ty::DELETE_BLOB => {
                    k += 1;
                    let id = BlobId(u32::from_le_bytes([e, f, g, h]));
                    (cb)(LogEntry::DeleteBlob { id })?;
                }
                entry::ty::RENAME_BLOB => {
                    k += 1;
                    let name_len = usize::from(b);
                    let id = BlobId(u32::from_le_bytes([e, f, g, h]));
                    let name = &buf_a[k..].as_flattened()[..usize::from(name_len)];
                    k += (name_len + 7) >> 3;
                    (cb)(LogEntry::RenameBlob { id, name })?;
                }
                entry::ty::APPEND_BLOB_TAIL => {
                    k += 1;
                    let len = usize::from(u16::from_le_bytes([c, d]));
                    let id = BlobId(u32::from_le_bytes([e, f, g, h]));
                    let data = &buf_a[k..]
                        .as_flattened()
                        .get(..usize::from(len))
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "log entry: append blob tail: missing data",
                            )
                        })?;
                    k += (len + 7) >> 3;
                    (cb)(LogEntry::AppendBlobTail { id, data })?;
                }
                entry::ty::ADD_ZONE_TO_BLOB => {
                    k += 1;
                    let id = BlobId(u32::from_le_bytes([e, f, g, h]));
                    let [x, y, z, w, _, _, _, _] = buf_a[k];
                    let zone = ZoneId(u32::from_le_bytes([x, y, z, w]));
                    k += 1;
                    (cb)(LogEntry::AddZoneToBlob { id, zone })?;
                }
                entry::ty::COMMIT_BLOB_TAIL => {
                    k += 1;
                    let id = BlobId(u32::from_le_bytes([e, f, g, h]));
                    let len = u64::from_le_bytes(buf_a[k]);
                    k += 1;
                    (cb)(LogEntry::CommitBlobTail { id, len })?;
                }
                entry::ty::NEXT_LOG_ZONE => {
                    visited_zones.set(log_zone_a as usize, true);
                    visited_zones.set(log_zone_b as usize, true);
                    let [_, _, _, _, x, y, z, w] = buf_b[k];
                    log_zone_a = u32::from_le_bytes([e, f, g, h]);
                    log_zone_b = u32::from_le_bytes([x, y, z, w]);
                    if log_zone_a == log_zone_b {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "both logs point to same zone",
                        ));
                    }
                    if visited_zones[log_zone_a as usize] || visited_zones[log_zone_b as usize] {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "zone already in use by log",
                        ));
                    }
                    log_zone_head = 0;
                    log_end = dev.zone_write_head(log_zone_a)?.unwrap_or(zone_size);
                    let zones = [log_zone_a, log_zone_b].map(ZoneId);
                    (cb)(LogEntry::NextLogZone { zones })?;
                    break;
                }
                entry::ty::TRANSACTION_BEGIN => {
                    k += 1;
                    (cb)(LogEntry::TransactionBegin)?;
                }
                entry::ty::TRANSACTION_END => {
                    k += 1;
                    (cb)(LogEntry::TransactionEnd)?;
                }
                entry::ty::HEADER => k += 2,
                ty => {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unrecognized log entry type {ty}"),
                    ))?;
                }
            }
        }

        if end_of_log {
            assert!(
                dev.zone_write_head(log_zone_a)?.is_none(),
                "zoned device should not contain end_of_log"
            );
            log_zone_head -= block_size as u64;
            break;
        }

        log_len += block_size as u64;

        if log_zone_head < log_end {
            dev.read_at(log_zone_a, log_zone_head, block_a)?;
            dev.read_at(log_zone_b, log_zone_head, block_b)?;
        }
    }

    trace!("finished parsing log");
    Ok(LogEnd {
        generation: gen_a,
        len: log_len,
        zone_head: log_zone_head,
    })
}
