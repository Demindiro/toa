use crate::{Key, PackOffset, record};

pub(crate) struct Pack {
    pub key: Key,
    pub object_trie_root: PackOffset,
    pub record_trie_root: record::Entry,
}

impl Pack {
    pub const LEN: usize = 72;

    pub fn into_bytes(self) -> [u8; Self::LEN] {
        let mut buf = [0; Self::LEN];
        buf[..32].copy_from_slice(&self.key);
        buf[32..64].copy_from_slice(&self.record_trie_root.into_bytes());
        buf[64..].copy_from_slice(&self.object_trie_root.0.to_le_bytes());
        buf
    }

    pub fn from_bytes(data: [u8; Self::LEN]) -> Self {
        Self {
            key: *Key::from_slice(&data[..32]),
            record_trie_root: record::Entry::from_bytes(data[32..64].try_into().unwrap()).unwrap(),
            object_trie_root: PackOffset(u64::from_le_bytes(data[64..].try_into().unwrap())),
        }
    }
}
