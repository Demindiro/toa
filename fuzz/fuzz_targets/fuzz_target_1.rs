#![no_main]

use core::cell::RefCell;
use toa::{Hash, Object};

#[derive(arbitrary::Arbitrary, Debug)]
enum Op<'a> {
    AddData {
        bytes: &'a [u8],
        repeat: u8,
    },
    AddRefs {
        slots: &'a [u8],
        repeat: u8,
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
    refs: Vec<Hash>,
}

thread_local! {
    static BUFFERS: RefCell<Buffers> = RefCell::new(Buffers {
        data: vec![0; 1 << 24],
        refs: vec![Hash::default(); 1 << 24],
    });
}

libfuzzer_sys::fuzz_target!(|ops: Vec<Op>| {
    let tempdir = tempfile::tempdir().unwrap();
    let mut toa = toa::Toa::open(tempdir.path()).unwrap();
    let mut objs = Vec::new();

    BUFFERS.with(|buffers| {
        let buffers = &mut *buffers.borrow_mut();
        let Buffers {
            data: buf_data,
            refs: buf_refs,
        } = buffers;

        for op in ops {
            let collect_refs = |slots: &[u8]| -> Option<Vec<Hash>> {
                slots
                    .iter()
                    .map(|&i| objs.get(usize::from(i)).map(|x: &(_, _)| x.1))
                    .collect::<Option<Vec<_>>>()
            };
            let rept = |x: &[u8], n: u8| (0..n).flat_map(|_| x).copied().collect::<Vec<_>>();
            match op {
                Op::AddData { bytes, repeat } => {
                    let bytes = rept(bytes, repeat);
                    let key = toa.add_data(&bytes).unwrap();
                    objs.push((bytes, key));
                }
                Op::AddRefs { slots, repeat } => {
                    let slots = rept(slots, repeat);
                    let Some(refs) = collect_refs(&slots) else {
                        continue;
                    };
                    let key = toa.add_refs(&refs).unwrap();
                    objs.push((slots, key));
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
                        Object::Refs(test) => {
                            let expect = {
                                let offset =
                                    offset.try_into().unwrap_or(usize::MAX).min(expect.len());
                                let expect = &expect[offset..];
                                let len = len.min(expect.len());
                                &expect[..len]
                            };
                            let Some(expect) = collect_refs(expect) else {
                                continue;
                            };
                            let buf = &mut *buf_refs;
                            let n = test.read(offset, &mut buf[..len]).unwrap();
                            assert_eq!(&buf[..n], expect, "object refs mismatch");
                        }
                    }
                }
                Op::Remount => {
                    drop(toa);
                    toa = toa::Toa::open(tempdir.path()).unwrap();
                }
            }
        }
    });
});
