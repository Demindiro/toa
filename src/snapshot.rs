use core::mem;

const SKIPLIST_NUM: usize = 10;

#[repr(C)]
pub struct Snapshot {
    skiplist: [u64; SKIPLIST_NUM],
    id: u64,
    timestamp: u64,
    object_tree_root: u64,
    records_root: u64,
    records_pitch: u8,
    records_depth: u8,
    _zero: [u8; 14],
}

const _: () = assert!(mem::size_of::<Snapshot>() == 128);
