use crate::{Hash, ObjectPointer, SnapshotOffset, SnapshotRoot};
use core::mem;

#[derive(Debug)]
pub(crate) struct ObjectTrie {
    root: Node,
}

pub(crate) enum Find<'a, 'h> {
    None(Insert<'a, 'h>),
    Object(SnapshotRoot, ObjectPointer),
}

pub(crate) struct Insert<'a, 'h> {
    replace: InsertNode<'a>,
    key: &'h Hash,
}

enum InsertNode<'a> {
    Parent(Nibble, &'a mut Parent),
    Leaf(NibbleIndex, &'a mut Node),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Nibble(u8);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NibbleIndex(u8);

#[derive(Debug)]
enum Node {
    Parent(Parent),
    Leaf(Leaf),
    External(External),
}

#[derive(Debug)]
struct Parent {
    population: u16,
    branches: Box<[Node]>,
}

#[derive(Clone, Copy, Debug)]
struct Leaf {
    hash: Hash,
    ptr: ObjectPointer,
}

#[derive(Clone, Copy, Debug)]
struct External {
    snapshot: SnapshotRoot,
    offset: SnapshotOffset,
}

#[repr(C)]
struct Leaf2 {
    hash: [u8; 32],
    offset: u64,
    len: u64,
}

#[repr(C)]
struct ExternalNode {
    snapshot: u64,
    offset: u64,
}

const _: () = assert!(mem::size_of::<Leaf2>() == 48);
const _: () = assert!(mem::size_of::<ExternalNode>() == 16);

impl ObjectTrie {
    pub fn with_external_root(snapshot: SnapshotRoot, offset: SnapshotOffset) -> Self {
        Self {
            root: Node::External(External { snapshot, offset }),
        }
    }

    pub fn with_leaf(key: &Hash, ptr: ObjectPointer) -> Self {
        Self {
            root: Node::Leaf(Leaf { hash: *key, ptr }),
        }
    }

    pub fn find<'a, 'h, E, F>(&'a mut self, key: &'h Hash, mut dev: F) -> Result<Find<'a, 'h>, E>
    where
        F: FnMut(SnapshotRoot, SnapshotOffset, &mut [u8]) -> Result<(), E>,
    {
        let none = |replace| Find::None(Insert { replace, key });
        let mut cur = &mut self.root;
        let mut index = NibbleIndex(0);
        Ok(loop {
            match cur {
                Node::Parent(x) => {
                    let nibble = index.get(key);
                    // https://github.com/rust-lang/rust/issues/21906
                    if !x.contains_nibble(nibble) {
                        break Find::None(Insert {
                            replace: InsertNode::Parent(nibble, x),
                            key,
                        });
                    }
                    cur = x.get_mut(nibble).expect("contains nibble");
                }
                Node::Leaf(Leaf { hash, ptr }) => {
                    break if hash == key {
                        Find::Object(SnapshotRoot(u64::MAX), *ptr)
                    } else {
                        none(InsertNode::Leaf(index, cur))
                    };
                }
                &mut Node::External(External {
                    mut snapshot,
                    mut offset,
                }) => loop {
                    let (offt, ty) = (offset.0 & !7, offset.0 & 7);
                    let mut f = |o, buf: &mut _| (dev)(snapshot, SnapshotOffset(offt + o), buf);
                    match ty {
                        0 => {
                            let nibble = index.get(key);
                            let buf = &mut [0; 8];
                            f(0, buf)?;
                            let population =
                                u16::from_le_bytes(buf[..2].try_into().expect("2 bytes"));
                            let i = (population % (1 << nibble.0)).count_ones();
                            dbg!(population, nibble.0, i);
                            f(8 * (1 + u64::from(i)), buf)?;
                            offset = SnapshotOffset(u64::from_le_bytes(*buf));
                        }
                        1 => {
                            let buf = &mut [0; 48];
                            f(0, buf)?;
                            let Leaf2 { hash, offset, len } = Leaf2::from_bytes(buf);
                            return Ok(if hash == key.0 {
                                Find::Object(
                                    snapshot,
                                    ObjectPointer {
                                        offset: SnapshotOffset(offset),
                                        len,
                                    },
                                )
                            } else {
                                todo!()
                            });
                        }
                        2 => {
                            let buf = &mut [0; 16];
                            f(0, buf)?;
                            let node = ExternalNode::from_bytes(buf);
                            snapshot = SnapshotRoot(node.snapshot);
                            offset = SnapshotOffset(node.offset);
                            continue; // don't increment index
                        }
                        _ => todo!("panic: todo"),
                    }
                    index = index.next();
                },
            }
            index = index.next();
        })
    }

    pub fn dirty(&self) -> bool {
        !matches!(&self.root, Node::External { .. })
    }

    pub fn serialize<E, F>(&self, mut f: F) -> Result<SnapshotOffset, E>
    where
        F: FnMut(&[u8]) -> Result<SnapshotOffset, E>,
    {
        self.root.serialize(&mut |x| {
            assert_eq!(x.len() % 8, 0);
            (f)(x).inspect(|x| assert_eq!(x.0 % 8, 0))
        })
    }
}

impl Find<'_, '_> {
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None(_))
    }

    pub fn into_object(self) -> Option<(SnapshotRoot, ObjectPointer)> {
        match self {
            Self::Object(snapshot, ptr) => Some((snapshot, ptr)),
            _ => None,
        }
    }
}

impl Insert<'_, '_> {
    pub fn insert<E, F>(self, ptr: ObjectPointer, dev: F) -> Result<(), E>
    where
        F: Fn(SnapshotRoot, SnapshotOffset, &mut [u8]) -> Result<(), E>,
    {
        let new = Leaf {
            hash: *self.key,
            ptr,
        };
        match self.replace {
            InsertNode::Parent(nibble, x) => x.insert(nibble, Node::Leaf(new)),
            InsertNode::Leaf(index, x) => {
                let Node::Leaf(y) = *x else { unreachable!() };
                *x = Node::Parent(Parent::new_pair(index, [new, y]))
            }
        }
        Ok(())
    }
}

impl Node {
    fn serialize<E, F>(&self, f: &mut F) -> Result<SnapshotOffset, E>
    where
        F: FnMut(&[u8]) -> Result<SnapshotOffset, E>,
    {
        let (mut offt, ty) = match self {
            Node::Parent(x) => (x.serialize(f)?, 0),
            Node::Leaf(x) => (x.serialize(f)?, 1),
            Node::External(x) => (x.serialize(f)?, 2),
        };
        dbg!(offt);
        assert_eq!(offt.0 & 7, 0);
        offt.0 |= ty;
        Ok(offt)
    }
}

impl Parent {
    fn new_pair(index: NibbleIndex, [a, b]: [Leaf; 2]) -> Self {
        let [x, y] = [a, b].map(|x| (index.get(&x.hash), Node::Leaf(x)));
        if x.0 != y.0 {
            Self::new([x, y])
        } else {
            let p = Self::new_pair(index.next(), [a, b]);
            Self::new([(x.0, Node::Parent(p))])
        }
    }

    fn new<const N: usize>(mut branches: [(Nibble, Node); N]) -> Self {
        const { assert!(N < 16) }
        let population = branches.iter().fold(0u16, |s, x| s | 1 << x.0.0);
        assert_eq!(
            population.count_ones(),
            branches.len() as u32,
            "{population:016b}  {branches:#?}"
        );
        branches.sort_by_key(|x| x.0);
        Self {
            population,
            branches: branches.map(|x| x.1).into(),
        }
    }

    fn insert(&mut self, nibble: Nibble, node: Node) {
        assert!(!self.contains_nibble(nibble));
        let bit = 1 << nibble.0;
        self.population |= bit;
        let mut branches = mem::take(&mut self.branches).into_vec();
        let i = self.nibble_to_index(nibble).expect("population bit set");
        branches.reserve(1);
        branches.insert(i, node);
        self.branches = branches.into();
    }

    fn contains_nibble(&self, nibble: Nibble) -> bool {
        self.nibble_to_index(nibble).is_some()
    }

    fn get(&self, nibble: Nibble) -> Option<&Node> {
        self.nibble_to_index(nibble).map(|i| &self.branches[i])
    }

    fn get_mut(&mut self, nibble: Nibble) -> Option<&mut Node> {
        self.nibble_to_index(nibble).map(|i| &mut self.branches[i])
    }

    fn nibble_to_index(&self, nibble: Nibble) -> Option<usize> {
        let bit = 1 << nibble.0;
        (self.population & bit != 0).then(|| (self.population & (bit - 1)).count_ones() as usize)
    }

    fn serialize<E, F>(&self, f: &mut F) -> Result<SnapshotOffset, E>
    where
        F: FnMut(&[u8]) -> Result<SnapshotOffset, E>,
    {
        debug_assert_eq!(self.branches.len(), self.population.count_ones() as usize);
        let mut buf = [0; 8 * (1 + 16)];
        buf[..2].copy_from_slice(&self.population.to_le_bytes());
        for (x, y) in buf[8..].chunks_exact_mut(8).zip(&self.branches) {
            x.copy_from_slice(&y.serialize(f)?.0.to_le_bytes())
        }
        (f)(&buf[..8 * (1 + self.branches.len())])
    }
}

impl Leaf {
    fn serialize<E, F>(&self, f: &mut F) -> Result<SnapshotOffset, E>
    where
        F: FnMut(&[u8]) -> Result<SnapshotOffset, E>,
    {
        (f)(&Leaf2 {
            hash: self.hash.0,
            offset: self.ptr.offset.0,
            len: self.ptr.len,
        }
        .into_bytes())
    }
}

impl External {
    fn serialize<E, F>(&self, f: &mut F) -> Result<SnapshotOffset, E>
    where
        F: FnMut(&[u8]) -> Result<SnapshotOffset, E>,
    {
        (f)(&ExternalNode {
            snapshot: self.snapshot.0,
            offset: self.offset.0,
        }
        .into_bytes())
    }
}

impl NibbleIndex {
    fn get(&self, key: &Hash) -> Nibble {
        debug_assert!(self.0 & 3 == 0, "{}", self.0);
        let (i, b) = (self.0 >> 3, self.0 & 4);
        Nibble((key.0[usize::from(i)] >> b) & 0xf)
    }

    fn next(self) -> Self {
        Self(self.0 + 4)
    }
}

impl Leaf2 {
    fn into_bytes(self) -> [u8; 48] {
        let mut buf = [0; 48];
        buf[..32].copy_from_slice(&self.hash);
        buf[32..40].copy_from_slice(&self.offset.to_le_bytes());
        buf[40..].copy_from_slice(&self.len.to_le_bytes());
        buf
    }

    fn from_bytes(b: &[u8; 48]) -> Self {
        Self {
            hash: b[..32].try_into().unwrap(),
            offset: u64::from_le_bytes(b[32..40].try_into().unwrap()),
            len: u64::from_le_bytes(b[40..].try_into().unwrap()),
        }
    }
}

impl ExternalNode {
    fn into_bytes(self) -> [u8; 16] {
        let mut buf = [0; 16];
        buf[..8].copy_from_slice(&self.snapshot.to_le_bytes());
        buf[8..].copy_from_slice(&self.offset.to_le_bytes());
        buf
    }

    fn from_bytes(b: &[u8; 16]) -> Self {
        Self {
            snapshot: u64::from_le_bytes(b[..8].try_into().unwrap()),
            offset: u64::from_le_bytes(b[8..].try_into().unwrap()),
        }
    }
}
