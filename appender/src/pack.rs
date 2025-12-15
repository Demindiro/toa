use crate::{PackOffset, record};
use chacha20poly1305::{AeadInPlace, ChaCha12Poly1305, Key, KeyInit, Nonce, Tag};

pub(crate) struct Pack {
    pub object_trie_root: PackOffset,
    pub record_trie_root: record::Entry,
}

impl Pack {
    pub const ENCRYPTED_LEN: usize = 64;

    pub fn encrypt(self, key: &Key) -> [u8; Self::ENCRYPTED_LEN] {
        let mut buf = [0; Self::ENCRYPTED_LEN];
        let (hdr, data) = buf.split_at_mut(16);
        data[..8].copy_from_slice(&self.object_trie_root.0.to_le_bytes());
        data[16..].copy_from_slice(&self.record_trie_root.into_bytes());

        let nonce = Nonce::from_slice(&[u8::MAX; 12]);
        let tag = ChaCha12Poly1305::new(key)
            .encrypt_in_place_detached(nonce, &[], data)
            .expect("failed to encrypt data");
        hdr[..16].copy_from_slice(tag.as_slice());

        buf
    }

    pub fn decrypt(
        mut data: [u8; Self::ENCRYPTED_LEN],
        key: &Key,
    ) -> Result<Self, chacha20poly1305::Error> {
        let (tag, data) = data.split_at_mut(16);
        let tag = Tag::from_slice(tag);
        let nonce = Nonce::from_slice(&[u8::MAX; 12]);
        ChaCha12Poly1305::new(key).decrypt_in_place_detached(nonce, &[], data, tag)?;
        Ok(Self {
            object_trie_root: PackOffset(u64::from_le_bytes(data[..8].try_into().unwrap())),
            record_trie_root: record::Entry::from_bytes(data[16..].try_into().unwrap()),
        })
    }
}
