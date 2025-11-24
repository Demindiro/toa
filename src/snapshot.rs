use core::mem;

const SKIPLIST_NUM: usize = 10;

#[repr(C)]
pub struct Snapshot {
    pub skiplist: [u64; SKIPLIST_NUM],
    pub id: u64,
    pub object_trie_root: u64,
    pub record_trie_root: crate::record::Entry,
}

const _: () = assert!(mem::size_of::<Snapshot>() == 128);
