use core::mem;

#[repr(C)]
struct Leaf {
    hash: [u8; 32],
    offset: u64,
    length: u64,
}

#[repr(C)]
struct Parent {
    populated: u16,
    nibble: u8,
    _zero: [u8; 5],
    branches: [u64; 16],
}

#[repr(C)]
struct ExternalNode {
    snapshot_id: u64,
    offset: u64,
}

const _: () = assert!(mem::size_of::<Leaf>() == 48);
//const _: () = assert!(mem::size_of::<Leaf>() == 48);
const _: () = assert!(mem::size_of::<ExternalNode>() == 16);
