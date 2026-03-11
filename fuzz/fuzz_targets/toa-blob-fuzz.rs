#![no_main]

use std::collections::hash_map::{Entry, HashMap};

#[derive(Debug, arbitrary::Arbitrary)]
enum Op<'a> {
    Remount,
    CreateBlob { name: &'a [u8] },
    DeleteBlob { name: &'a [u8] },
    AppendBlob { slot: u16, data: &'a [u8] },
    ReadBlob { slot: u16, offset: u32, len: u16 },
    RenameBlob { slot: u16, new_name: &'a [u8] },
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

    let mut blob_map = HashMap::<&[u8], u16>::with_capacity(1 << 16);
    let mut blobs = Vec::<Option<(&[u8], Vec<u8>)>>::with_capacity(1 << 16);

    for op in ops {
        match op {
            Op::Remount => {
                let (root_dev, zone_dev) = store.unmount().map_err(|e| e.1).unwrap();
                let root = toa_blob::BlobRoot::load(root_dev).unwrap();
                store = root.with_zone_dev(zone_dev).unwrap();
                for name in blob_map.keys() {
                    store
                        .blob(name)
                        .unwrap()
                        .unwrap_or_else(|| panic!("store is missing blob {name:?}"));
                }
            }
            Op::CreateBlob { name } => {
                let name = &name[..name.len().min(255)];
                match (blob_map.entry(name), store.create_blob(name).unwrap()) {
                    (Entry::Vacant(e), Ok(_)) => {
                        e.insert(blobs.len() as u16);
                        blobs.push(Some((name, Vec::<u8>::new())));
                    }
                    (Entry::Occupied(_), Err(toa_blob::DuplicateBlob)) => {}
                    _ => panic!("blob map corrupt"),
                }
            }
            Op::DeleteBlob { name } => {
                let name = &name[..name.len().min(255)];
                match (blob_map.remove(name), store.blob(name).unwrap()) {
                    (Some(x), Some(y)) => {
                        blobs[usize::from(x)] = None;
                        y.delete().unwrap();
                    }
                    (None, None) => {}
                    (Some(_), None) => panic!("store is missing blob"),
                    (None, Some(_)) => panic!("store has ghost blob"),
                }
            }
            Op::AppendBlob { slot, data } => {
                let Some((name, x)) = blobs.get_mut(usize::from(slot)).and_then(|x| x.as_mut())
                else {
                    continue;
                };
                let name: &[u8] = name;
                match store.blob(name).unwrap() {
                    Some(mut y) => {
                        let offt = y.append(data).unwrap();
                        assert_eq!(offt, x.len() as u64, "offset mismatch");
                        x.extend(data);
                    }
                    None => panic!("store is missing blob"),
                }
            }
            Op::ReadBlob { slot, offset, len } => {
                let Some((name, x)) = blobs.get(usize::from(slot)).and_then(|x| x.as_ref()) else {
                    continue;
                };
                match store.blob(name).unwrap() {
                    Some(y) => {
                        let mut buf = vec![0; len.into()];
                        let n = y.read_at(offset.into(), &mut buf).unwrap();
                        let x = x.get(offset as usize..).unwrap_or(&[]);
                        let x = x.get(..len.into()).unwrap_or(x);
                        assert_eq!(x, &buf[..n]);
                    }
                    None => panic!("store is missing blob"),
                }
            }
            Op::RenameBlob { slot, new_name } => {
                let new_name = &new_name[..new_name.len().min(255)];
                let Some((old_name, _)) = blobs.get(usize::from(slot)).and_then(|x| x.as_ref())
                else {
                    continue;
                };
                store
                    .blob(old_name)
                    .unwrap()
                    .expect("store is missing blob")
                    .rename(new_name)
                    .unwrap();
                if *old_name != new_name {
                    blob_map.remove(old_name);
                    blob_map
                        .entry(new_name)
                        .and_modify(|x| {
                            blobs[usize::from(*x)] = None;
                            *x = slot;
                        })
                        .or_insert(slot);
                    blobs[usize::from(slot)].as_mut().unwrap().0 = new_name;
                }
            }
        }
    }
});
