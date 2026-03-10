#![no_main]

use std::collections::hash_map::{Entry, HashMap};

#[derive(Debug, arbitrary::Arbitrary)]
enum Op<'a> {
    Remount,
    CreateBlob { name: &'a [u8] },
    DeleteBlob { name: &'a [u8] },
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

    let mut blobs = HashMap::<&[u8], _>::with_capacity(1 << 20);

    for op in ops {
        match op {
            Op::Remount => {
                let (root_dev, zone_dev) = store.unmount().map_err(|e| e.1).unwrap();
                let root = toa_blob::BlobRoot::load(root_dev).unwrap();
                store = root.with_zone_dev(zone_dev).unwrap();
                for (name, blob) in blobs.iter() {
                    store
                        .blob(name)
                        .unwrap()
                        .unwrap_or_else(|| panic!("store is missing blob {name:?}"));
                    let _ = blob;
                }
            }
            Op::CreateBlob { name } => {
                match (blobs.entry(name), store.create_blob(name).unwrap()) {
                    (Entry::Vacant(e), Ok(())) => {
                        e.insert(());
                    }
                    (Entry::Occupied(_), Err(toa_blob::DuplicateBlob)) => {}
                    _ => panic!("blob map corrupt"),
                }
            }
            Op::DeleteBlob { name } => {
                match (blobs.remove(name), store.delete_blob(name).unwrap()) {
                    (Some(_), Ok(())) => {}
                    (None, Err(toa_blob::NoBlobByName)) => {}
                    (Some(_), Err(toa_blob::NoBlobByName)) => panic!("store is missing blob"),
                    (None, Ok(())) => panic!("store has ghost blob"),
                }
            }
        }
    }
});
