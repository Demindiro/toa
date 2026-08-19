#![no_main]

use core::{
    cell::RefCell,
    hash::{BuildHasher, Hasher},
};
use std::collections::HashMap;
use toa::{Compression, Hash, Object, PageSize};
use toa_blob::{BlobStore, MemZones};

/// Like a slice but shorter and designed for repeating.
#[derive(Debug)]
struct ShortSlice<'a>(&'a [u8]);

#[derive(arbitrary::Arbitrary, Debug)]
enum Op<'a> {
    AddData {
        bytes: ShortSlice<'a>,
        repeat: u16,
    },
    AddRefs {
        head: u8,
        tail: u8,
    },
    // use u24 instead of usize because 64-bit usize is excessive + not consistent between
    // 32/64-bit platforms
    Read {
        slot: u8,
        offset: u128,
        len: [u8; 3],
    },
    Remount,
}

struct Buffers {
    data: Vec<u8>,
    objs: Vec<(Vec<u8>, Hash)>,
    accel: HashMap<Hash, toa::accel::IndexEntry, NoopBuildHasher>,
}

#[derive(Default)]
struct NoopBuildHasher;
struct NoopHash(u64);

thread_local! {
    static BUFFERS: RefCell<Buffers> = RefCell::new(Buffers {
        data: vec![0; 1 << 24],
        objs: vec![],
        accel: Default::default(),
    });
}

impl<'a> arbitrary::Arbitrary<'a> for ShortSlice<'a> {
    fn arbitrary(s: &mut arbitrary::Unstructured<'a>) -> Result<Self, arbitrary::Error> {
        let n = s.arbitrary_len::<u8>()? % 256;
        s.bytes(n).map(Self)
    }
}

impl BuildHasher for NoopBuildHasher {
    type Hasher = NoopHash;

    fn build_hasher(&self) -> Self::Hasher {
        NoopHash(0)
    }
}

impl Hasher for NoopHash {
    fn write(&mut self, bytes: &[u8]) {
        let &[a, b, c, d, e, f, g, h, ..] = bytes else {
            panic!("at least 32 bytes")
        };
        self.0 = u64::from_ne_bytes([a, b, c, d, e, f, g, h])
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

libfuzzer_sys::fuzz_target!(|ops: Vec<Op>| {
    BUFFERS.with(|buffers| {
        let buffers = &mut *buffers.borrow_mut();
        let Buffers {
            data: buf_data,
            objs,
            accel,
        } = buffers;

        let store = BlobStore::init(MemZones::<512>::new(1 << 20, 20)).unwrap();

        objs.clear();
        accel.clear();

        let id = [0; 16];
        let mut toa = toa::Toa::init(store, accel, id, PageSize::K4, Compression::Lz4, 0)
            .unwrap()
            .unwrap();

        // avoid repeated remounts
        let ops = {
            let mut n = Vec::with_capacity(ops.len());
            for x in ops {
                match (n.last(), x) {
                    (Some(Op::Remount), Op::Remount) => {}
                    (_, x) => n.push(x),
                }
            }
            n
        };

        for op in ops {
            let rept = |x: &[u8], n: u16| (0..n).flat_map(|_| x).copied().collect::<Vec<_>>();
            match op {
                Op::AddData { bytes, repeat } => {
                    let bytes = rept(bytes.0, repeat);
                    let key = toa.add_data(&bytes).unwrap();
                    objs.push((bytes, key));
                }
                Op::AddRefs { head, tail } => {
                    let xy = [head, tail].map(|x| objs.get(usize::from(x)));
                    let [Some(x), Some(y)] = xy else {
                        continue;
                    };
                    let key = toa.add_refs(x.1, y.1).unwrap();
                    objs.push(([head, tail].into(), key));
                }
                Op::Read {
                    slot,
                    offset,
                    len: [a, b, c],
                } => {
                    let len = u32::from_le_bytes([a, b, c, 0]) as usize;
                    let Some((expect, test)) = objs.get(usize::from(slot)) else {
                        continue;
                    };
                    let test = toa.get(test).unwrap().expect("data object disappeared");
                    match test {
                        Object::Data(test) => {
                            let expect = {
                                let offset =
                                    offset.try_into().unwrap_or(usize::MAX).min(expect.len());
                                let expect = &expect[offset..];
                                let len = len.min(expect.len());
                                &expect[..len]
                            };
                            let buf = &mut *buf_data;
                            let n = test.read(offset, &mut buf[..len]).unwrap();
                            assert_eq!(&buf[..n], expect, "object data mismatch");
                        }
                        Object::Refs([head, tail]) => {
                            let [x, y] =
                                (&**expect).try_into().expect("refs has exactly 2 elements");
                            assert_eq!(objs[usize::from(x)].1, head);
                            assert_eq!(objs[usize::from(y)].1, tail);
                        }
                    }
                }
                Op::Remount => {
                    let (store, accel, res) = toa.unmount();
                    res.unwrap();
                    toa = toa::Toa::load(store, accel).unwrap().unwrap();
                }
            }
        }
    });
});
