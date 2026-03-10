#![no_main]

#[derive(Debug, arbitrary::Arbitrary)]
enum Op<'a> {
    Remount,
    CreateBlob { name: &'a [u8] },
}

libfuzzer_sys::fuzz_target!(|ops: Vec<Op<'_>>| {
    let zone_uuid = *b"AbracadabraKapow";
    let mut store = toa_blob::BlobStore::init(
        toa_blob::MemRoot::new(5),
        toa_blob::MemZones::new(1 << 20, 10),
        zone_uuid,
        1 << 20,
    )
    .unwrap();

    let mut blobs = vec![None; 1 << 16].into_boxed_slice();

    for op in ops {
        match op {
            Op::Remount => {
                let (root_dev, zone_dev) = store.unmount().map_err(|e| e.1).unwrap();
                let root = toa_blob::BlobRoot::load(root_dev).unwrap();
                store = root.with_zone_dev(zone_dev).unwrap();
                for (id, blob) in blobs.iter().enumerate() {
                    let id = toa_blob::BlobId(id as u16);
                    match (blob, store.blob(id).unwrap()) {
                        (Some(_), Some(_)) => {}
                        (None, None) => {}
                        (Some(_), None) => panic!("store is missing blob {:?}", id.0),
                        (None, Some(_)) => panic!("store has ghost blob {:?}", id.0),
                    }
                }
            }
            Op::CreateBlob { name } => {
                let id = store.create_blob(name).unwrap();
                blobs[usize::from(id.0)] = Some(());
            }
        }
    }
});
