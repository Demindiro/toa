#![no_main]

#[derive(Debug, arbitrary::Arbitrary)]
enum Op<'a> {
    Remount,
    CreateBlob { name: &'a [u8] },
}

libfuzzer_sys::fuzz_target!(|ops: Vec<Op<'_>>| {
    let zone_uuid = *b"AbracadabraKapow";
    let mut rng = rand::rng();
    let mut store = toa_blob::BlobStore::init(
        &mut rng,
        toa_blob::MemRoot::new(5),
        toa_blob::MemZones::new(1 << 20, 10),
        zone_uuid,
        1 << 20,
    )
    .unwrap();
    let key = [0; 32];

    for op in ops {
        match op {
            Op::Remount => {
                let (root_dev, zone_dev) = store.unmount().map_err(|e| e.1).unwrap();
                let root = toa_blob::BlobRoot::load(root_dev).unwrap();
                let root = root.unlock(&key, toa_blob::KeySlot::N0).unwrap();
                store = root.with_zone_dev(zone_dev).unwrap()
            }
            Op::CreateBlob { name } => {
                store.create_blob(&mut rng, name).unwrap();
            }
        }
    }
});
