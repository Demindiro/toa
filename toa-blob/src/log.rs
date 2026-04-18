use crate::Header;

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
