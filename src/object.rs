use core::mem;
use crate::{Hash, SnapshotId, SnapshotOffset};

#[derive(Default)]
pub struct ObjectTrie {
    root: Option<Node>,
}

enum Find {
    None { different_nibble: Nibble },
    Object { offset: SnapshotOffset, length: u64 },
}

#[derive(Clone, Copy)]
pub struct Nibble(u8);

enum Node {
    Parent(Parent),
    Leaf {
        hash: Hash,
        offset: SnapshotOffset,
        length: u64,
    },
    External {
        id: SnapshotId,
        offset: SnapshotOffset,
    },
}

struct Parent {
    population: u16,
    nibble: Nibble,
    branches: Box<[Node]>,
}

#[repr(C)]
struct Leaf {
    hash: [u8; 32],
    offset: u64,
    length: u64,
}

#[repr(C)]
struct ParentHead {
    populated: u16,
    nibble: u8,
    _zero: [u8; 5],
    //branches: [u64],
}

#[repr(C)]
struct ExternalNode {
    snapshot_id: u64,
    offset: u64,
}

const _: () = assert!(mem::size_of::<Leaf>() == 48);
const _: () = assert!(mem::size_of::<ParentHead>() == 8);
const _: () = assert!(mem::size_of::<ExternalNode>() == 16);

impl ObjectTrie {
    pub fn reset(&mut self, id: SnapshotId, offset: SnapshotOffset) {
        self.root = Some(Node::External { id, offset });
    }

    pub fn find<F>(&mut self, key: &Hash, dev: F) -> Find
    where
        F: Fn(),
    {
        let none = |x| Find::None { different_nibble: x };
        let Some(mut cur) = self.root.as_ref() else { return none(Nibble(0)) };
        loop {
            match cur {
                Node::Parent(x) => {
                    let Some(c) = x.get(key) else { return none(x.nibble) };
                    cur = c;
                }
                &Node::Leaf { hash, offset, length } => return differing_nibble(&hash, key)
                    .map_or(Find::Object { offset, length }, |i| Find::None { different_nibble: i }),
                &Node::External { id, offset } => todo!(),
            }
        }
    }
}

impl Parent {
    fn get(&self, key: &Hash) -> Option<&Node> {
        let (i, b) = (self.nibble.0 >> 3, self.nibble.0 & 8);
        let nibble = (key.0[usize::from(i)] >> b) & 0xf;
        let bit = 1 << nibble;
        if self.population & bit == 0 {
            return None;
        }
        let i = (self.population & (bit - 1)).count_ones();
        Some(&self.branches[i as usize])
    }
}

impl Leaf {
    fn into_bytes(self) -> [u8; 48] {
        let mut buf = [0; 48];
        buf[..32].copy_from_slice(&self.hash);
        buf[32..40].copy_from_slice(&self.offset.to_le_bytes());
        buf[40..].copy_from_slice(&self.length.to_le_bytes());
        buf
    }
}

impl ParentHead {
    fn into_bytes(self) -> [u8; 8] {
        let mut buf = [0; 8];
        buf[..2].copy_from_slice(&self.populated.to_le_bytes());
        buf[2] = self.nibble;
        buf
    }
}

impl ExternalNode {
    fn into_bytes(self) -> [u8; 16] {
        let mut buf = [0; 16];
        buf[..8].copy_from_slice(&self.snapshot_id.to_le_bytes());
        buf[8..].copy_from_slice(&self.offset.to_le_bytes());
        buf
    }
}

/*
fn different_nibble(x: &Hash, y: &Hash) -> Option<Nibble> {
    x.0.into_iter()
        .zip(y.0)
        .enumerate()
        .find(|(_, (x, y))| x != y)
        .map(|(i, (x, y))| i * 8 + usize::from(x & 15 == y & 15) * 4)
        .map(|x| Nibble(x.try_into().expect("256-bit hash")))

}
*/
// ^~~ has dumb codegen with loads of branches
// hash values are highly unpredictable, so conditional moves should be preferred
// v~~ this emits no branches
fn differing_nibble(Hash(x): &Hash, Hash(y): &Hash) -> Option<Nibble> {
    let i = 16 * cmp(to128, &x[..16], &y[..16]);
    let i = i + 8 * cmp(to64, &x[i..][..8], &y[i..][..8]);
    let i = i + 4 * cmp(to32, &x[i..][..4], &y[i..][..4]);
    let i = i + 2 * cmp(to16, &x[i..][..2], &y[i..][..2]);
    let i = i + 1 * cmp(|x| x[0], &x[i..][..1], &y[i..][..1]);
    let k = u8::try_from(i).expect("256 bits");
    (x[i] != y[i]).then_some(Nibble(k + u8::from(x[i] & 15 == y[i] & 15) * 4))
}
// rustc/LLVM has a very bad tendency to emit memcmp,
// so convert to integers first to avoid that.
#[inline(always)]
fn cmp<T, F>(f: F, x: &[u8], y: &[u8]) -> usize
where
    T: Eq,
    F: Fn(&[u8]) -> T,
{
    usize::from(f(x) == f(y))
}
#[inline(always)]
fn to128(x: &[u8]) -> u128 {
    u128::from_le_bytes(x.try_into().expect("128"))
}
#[inline(always)]
fn to64(x: &[u8]) -> u64 {
    u64::from_le_bytes(x.try_into().expect("64"))
}
#[inline(always)]
fn to32(x: &[u8]) -> u32 {
    u32::from_le_bytes(x.try_into().expect("32"))
}
#[inline(always)]
fn to16(x: &[u8]) -> u16 {
    u16::from_le_bytes(x.try_into().expect("16"))
}
