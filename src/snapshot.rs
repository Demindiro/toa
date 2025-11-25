use core::mem;

#[repr(C)]
pub struct Snapshot {
    pub poly1305: u128,
    pub object_trie_root: u64,
    pub timestamp: u64,
    pub record_trie_root: crate::record::Entry,
}

const _: () = assert!(mem::size_of::<Snapshot>() == 64);

impl Snapshot {
    pub fn into_bytes(self) -> [u8; 64] {
        let mut buf = [0; 64];
        buf[..16].copy_from_slice(&self.poly1305.to_le_bytes());
        buf[16..24].copy_from_slice(&self.object_trie_root.to_le_bytes());
        buf[24..32].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[32..].copy_from_slice(&self.record_trie_root.into_bytes());
        buf
    }
}

fn append_u64(out: &mut [u8], n: u64) -> &mut [u8] {
    let (x, y) = out.split_at_mut(8);
    x.copy_from_slice(&n.to_le_bytes());
    y
}
