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

impl Snapshot {
    pub fn into_bytes(self) -> [u8; 128] {
        let mut buf = [0; 128];
        self.skiplist
            .into_iter()
            .chain([self.id, self.object_trie_root])
            .fold(&mut buf[..96], append_u64);
        buf[96..].copy_from_slice(&self.record_trie_root.into_bytes());
        buf
    }
}

fn append_u64(out: &mut [u8], n: u64) -> &mut [u8] {
    let (x, y) = out.split_at_mut(8);
    x.copy_from_slice(&n.to_le_bytes());
    y
}
