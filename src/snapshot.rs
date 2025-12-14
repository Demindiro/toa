use crate::{Poly1305, record};
use chacha20poly1305::{
    AeadCore, AeadInPlace, Key, KeyInit, XChaCha12Poly1305, XNonce,
    aead::rand_core::{CryptoRng, RngCore},
};
use core::mem;

pub struct Snapshot {
    pub object_trie_root: u64,
    pub len: u64,
    pub record_trie_root: record::Entry,
}

impl Snapshot {
    pub const ENCRYPTED_LEN: usize = 128;

    pub fn encrypt<R>(self, key: &Key, rng: R) -> [u8; Self::ENCRYPTED_LEN]
    where
        R: CryptoRng + RngCore,
    {
        let mut buf = [0; Self::ENCRYPTED_LEN];
        /*
        buf[..16].copy_from_slice(self.poly1305.as_slice());
        buf[16..40].copy_from_slice(self.nonce.as_slice());
        */
        buf[64..72].copy_from_slice(&self.object_trie_root.to_le_bytes());
        buf[72..80].copy_from_slice(&self.len.to_le_bytes());
        buf[96..].copy_from_slice(&self.record_trie_root.into_bytes());

        let cipher = XChaCha12Poly1305::new(key);
        let nonce = XChaCha12Poly1305::generate_nonce(rng);
        let tag = cipher
            .encrypt_in_place_detached(&nonce, &[], &mut buf[40..])
            .expect("failed to encrypt snapshot");
        buf[16..40].copy_from_slice(nonce.as_slice());
        buf[..16].copy_from_slice(tag.as_slice());

        buf
    }

    pub fn decrypt(
        mut b: [u8; Self::ENCRYPTED_LEN],
        key: &Key,
    ) -> Result<Self, chacha20poly1305::Error> {
        let (hdr, data) = b.split_at_mut(40);
        let tag = Poly1305::from_slice(&hdr[..16]);
        let nonce = XNonce::from_slice(&hdr[16..40]);
        XChaCha12Poly1305::new(key)
            .decrypt_in_place_detached(nonce, &[], data, tag)
            .map(|()| Self {
                object_trie_root: u64::from_le_bytes(b[64..72].try_into().unwrap()),
                len: u64::from_le_bytes(b[72..80].try_into().unwrap()),
                record_trie_root: record::Entry::from_bytes(b[96..].try_into().unwrap()),
            })
    }
}
