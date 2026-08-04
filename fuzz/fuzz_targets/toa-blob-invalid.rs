#![no_main]

use libfuzzer_sys::Corpus;
use toa_blob::{BlobStore, BlockShift, MemBlocks};

libfuzzer_sys::fuzz_target!(|init: &[u8]| -> Corpus {
    let data = init.to_vec().into_boxed_slice();
    let Ok(dev) = MemBlocks::wrap(BlockShift::N9, 4, data) else {
        return Corpus::Reject;
    };
    let _ = BlobStore::load(dev);
    Corpus::Keep
});
