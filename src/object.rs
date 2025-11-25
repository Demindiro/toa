use crate::{Hash, ObjectPointer, SnapshotId, SnapshotOffset};
use core::mem;

#[derive(Debug, Default)]
pub(crate) struct ObjectTrie {
    root: Option<Node>,
}

pub(crate) enum Find<'a, 'h> {
    None(Insert<'a, 'h>),
    Object(ObjectPointer),
}

pub(crate) struct Insert<'a, 'h> {
    replace: InsertNode<'a>,
    key: &'h Hash,
}

enum InsertNode<'a> {
    Root(&'a mut Option<Node>),
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
    External {
        id: SnapshotId,
        offset: SnapshotOffset,
    },
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

#[repr(C)]
struct Leaf2 {
    hash: [u8; 32],
    offset: u64,
    length: u64,
}

#[repr(C)]
struct ParentHead {
    populated: u16,
    _zero: [u8; 6],
    //branches: [u64],
}

#[repr(C)]
struct ExternalNode {
    snapshot_id: u64,
    offset: u64,
}

const _: () = assert!(mem::size_of::<Leaf2>() == 48);
const _: () = assert!(mem::size_of::<ParentHead>() == 8);
const _: () = assert!(mem::size_of::<ExternalNode>() == 16);

impl ObjectTrie {
    pub fn reset(&mut self, id: SnapshotId, offset: SnapshotOffset) {
        self.root = Some(Node::External { id, offset });
    }

    pub fn find<'a, 'h, E, F>(&'a mut self, key: &'h Hash, dev: F) -> Result<Find<'a, 'h>, E>
    where
        F: Fn(SnapshotOffset, &mut [u8]) -> Result<(), E>,
    {
        let none = |replace| Find::None(Insert { replace, key });
        // https://github.com/rust-lang/rust/issues/21906
        if self.root.is_none() {
            return Ok(none(InsertNode::Root(&mut self.root)));
        }
        let mut cur = self.root.as_mut().expect("not None");
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
                        Find::Object(*ptr)
                    } else {
                        none(InsertNode::Leaf(index, cur))
                    };
                }
                Node::External { .. } => todo!(),
            }
            index = index.next();
        })
    }

    pub fn dirty(&self) -> bool {
        !matches!(&self.root, None | Some(Node::External { .. }))
    }
}

impl Find<'_, '_> {
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

impl Insert<'_, '_> {
    pub fn insert<E, F>(self, ptr: ObjectPointer, dev: F) -> Result<(), E>
    where
        F: Fn(SnapshotOffset, &mut [u8]) -> Result<(), E>,
    {
        let new = Leaf {
            hash: *self.key,
            ptr,
        };
        match self.replace {
            InsertNode::Root(x) => *x = Some(Node::Leaf(new)),
            InsertNode::Parent(nibble, x) => x.insert(nibble, Node::Leaf(new)),
            InsertNode::Leaf(index, x) => {
                let Node::Leaf(y) = *x else { unreachable!() };
                *x = Node::Parent(Parent::new_pair(index, [new, y]))
            }
        }
        Ok(())
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
        buf[40..].copy_from_slice(&self.length.to_le_bytes());
        buf
    }
}

impl ParentHead {
    fn into_bytes(self) -> [u8; 8] {
        let mut buf = [0; 8];
        buf[..2].copy_from_slice(&self.populated.to_le_bytes());
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
