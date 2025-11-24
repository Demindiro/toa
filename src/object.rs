use crate::{Hash, ObjectPointer, SnapshotId, SnapshotOffset};
use core::mem;

#[derive(Debug, Default)]
pub struct ObjectTrie {
    root: Option<Node>,
}

pub enum Find<'a> {
    None(Insert<'a>),
    Object(ObjectPointer),
}

pub struct Insert<'a> {
    trie: &'a mut ObjectTrie,
    key: &'a Hash,
    nibble: Nibble,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Nibble(u8);

#[derive(Debug)]
enum Node {
    Parent(Parent),
    Leaf {
        hash: Hash,
        ptr: ObjectPointer,
    },
    External {
        id: SnapshotId,
        offset: SnapshotOffset,
    },
}

#[derive(Debug)]
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

    pub fn find<'a, E, F>(&'a mut self, key: &'a Hash, dev: F) -> Result<Find<'a>, E>
    where
        F: Fn(SnapshotOffset, &mut [u8]) -> Result<(), E>,
    {
        dbg!(&*self);
        let none = |trie, nibble| Find::None(Insert { trie, nibble, key });
        let Some(mut cur) = self.root.as_ref() else {
            return Ok(none(self, Nibble(0)));
        };
        Ok(loop {
            match cur {
                Node::Parent(x) => {
                    let Some(c) = x.get(key) else {
                        break none(self, x.nibble);
                    };
                    cur = c;
                }
                &Node::Leaf { hash, ptr } => {
                    break differing_nibble(&hash, key)
                        .map_or(Find::Object(ptr), |x| none(self, dbg!(x)));
                }
                &Node::External { id, offset } => todo!(),
            }
        })
    }

    pub fn dirty(&self) -> bool {
        !matches!(&self.root, None | Some(Node::External { .. }))
    }
}

impl Find<'_> {
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None(_))
    }

    pub fn into_object(self) -> Option<ObjectPointer> {
        match self {
            Self::Object(ptr) => Some(ptr),
            _ => None,
        }
    }
}

impl Insert<'_> {
    pub fn insert<E, F>(self, ptr: ObjectPointer, dev: F) -> Result<(), E>
    where
        F: Fn(SnapshotOffset, &mut [u8]) -> Result<(), E>,
    {
        let leaf = Node::Leaf {
            hash: *self.key,
            ptr,
        };
        let Some(mut cur) = self.trie.root.as_mut() else {
            self.trie.root = Some(leaf);
            return Ok(());
        };
        loop {
            match cur {
                Node::Parent(x) if self.nibble <= x.nibble => break,
                Node::Parent(x) => cur = x.get_mut(self.key).unwrap(),
                Node::Leaf { .. } => break,
                Node::External { id, offset } => todo!(),
            }
        }
        match cur {
            Node::Parent(x) => x.insert(self.key, leaf),
            Node::Leaf { hash, ptr: p } => {
                let f = |hash, ptr| (self.nibble.get(&hash), Node::Leaf { hash, ptr });
                dbg!(self.nibble.0, self.nibble.get(self.key), self.nibble.get(hash));
                let branches = [f(*self.key, ptr), f(*hash, *p)];
                *cur = Node::Parent(Parent::new(self.nibble, branches));
            }
            &mut Node::External { .. } => todo!(),
        }
        Ok(())
    }
}

impl Parent {
    fn new<const N: usize>(nibble: Nibble, mut branches: [(u8, Node); N]) -> Self {
        const {
            assert!(N < 16);
        }
        let population = branches.iter().fold(0u16, |s, x| s | 1 << x.0);
        assert_eq!(population.count_ones(), branches.len() as u32, "{population:016b}  {branches:#?}");
        branches.sort_by_key(|x| x.0);
        Self {
            population,
            nibble,
            branches: branches.map(|x| x.1).into(),
        }
    }

    fn insert(&mut self, key: &Hash, node: Node) {
        assert!(!self.contains_key(key));
        let bit = 1 << self.nibble.get(key);
        self.population |= bit;
        let mut branches = mem::take(&mut self.branches).into_vec();
        let i = self.key_to_index(key).expect("population bit set");
        branches.reserve(1);
        branches.insert(i, node);
        self.branches = branches.into();
    }

    fn contains_key(&self, key: &Hash) -> bool {
        self.key_to_index(key).is_some()
    }

    fn get(&self, key: &Hash) -> Option<&Node> {
        self.key_to_index(key).map(|i| &self.branches[i])
    }

    fn get_mut(&mut self, key: &Hash) -> Option<&mut Node> {
        self.key_to_index(key).map(|i| &mut self.branches[i])
    }

    fn key_to_index(&self, key: &Hash) -> Option<usize> {
        let bit = 1 << self.nibble.get(key);
        (self.population & bit != 0).then(|| (self.population & (bit - 1)).count_ones() as usize)
    }
}

impl Nibble {
    fn get(&self, key: &Hash) -> u8 {
        debug_assert!(self.0 & 3 == 0, "{}", self.0);
        let (i, b) = (self.0 >> 3, self.0 & 4);
        (key.0[usize::from(i)] >> b) & 0xf
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
    let k = u8::try_from(i * 8).expect("256 bits");
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
