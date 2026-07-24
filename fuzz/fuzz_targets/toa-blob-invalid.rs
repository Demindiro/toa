#![no_main]

use toa_blob::{BlobStore, BlockShift, MemBlocks};

libfuzzer_sys::fuzz_target!(|init: &[u8]| {
    let mut data = vec![0; 1 << 20].into_boxed_slice();
    data[..init.len()].copy_from_slice(init);
    let dev = MemBlocks::wrap(BlockShift::N9, 200, data);
    let _ = BlobStore::load(dev);
});
