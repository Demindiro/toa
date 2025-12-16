use super::{Leaf2, Nibble, NibbleIndex};
use crate::{Hash, ObjectPointer, PackOffset};
use alloc::boxed::Box;
use core::{fmt, mem};

#[derive(Debug)]
pub(crate) struct ObjectTrie {
    root: Node,
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
    Parent(Nibble, &'a mut Parent),
    Leaf(NibbleIndex, &'a mut Node),
}

enum Node {
    Parent(Parent),
    Leaf(Leaf),
}

struct Parent {
    population: u16,
    branches: Box<[Node]>,
}

#[derive(Clone, Copy)]
struct Leaf {
    hash: Hash,
    ptr: ObjectPointer,
}

impl ObjectTrie {
    pub fn with_leaf(key: &Hash, ptr: ObjectPointer) -> Self {
        Self {
            root: Node::Leaf(Leaf { hash: *key, ptr }),
        }
    }

    pub fn find<'a, 'h>(&'a mut self, key: &'h Hash) -> Find<'a, 'h> {
        let none = |replace| Find::None(Insert { replace, key });
        let mut cur = &mut self.root;
        let mut index = NibbleIndex(0);
        loop {
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
            }
            index = index.next();
        }
    }

    pub fn serialize<E, F>(self, mut f: F) -> Result<PackOffset, E>
    where
        F: FnMut(&[u8]) -> Result<PackOffset, E>,
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

    pub fn into_object(self) -> Option<ObjectPointer> {
        match self {
            Self::Object(ptr) => Some(ptr),
            _ => None,
        }
    }
}

impl Insert<'_, '_> {
    pub fn insert(self, ptr: ObjectPointer) {
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
    }
}

impl Node {
    fn serialize<E, F>(self, f: &mut F) -> Result<PackOffset, E>
    where
        F: FnMut(&[u8]) -> Result<PackOffset, E>,
    {
        let (mut offt, ty) = match self {
            Node::Parent(x) => (x.serialize(f)?, 0),
            Node::Leaf(x) => (x.serialize(f)?, 1),
        };
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

    fn serialize<E, F>(self, f: &mut F) -> Result<PackOffset, E>
    where
        F: FnMut(&[u8]) -> Result<PackOffset, E>,
    {
        debug_assert_eq!(self.branches.len(), self.population.count_ones() as usize);
        let len = self.branches.len();
        let buf = &mut [0; 1 + 16];
        buf[0] = u64::from(self.population);
        buf[1..]
            .iter_mut()
            .zip(self.branches)
            .try_for_each(|(x, y)| Ok(*x = y.serialize(f)?.0))?;
        let buf = buf.map(u64::to_le_bytes);
        (f)(buf[..1 + len].as_flattened())
    }
}

impl Leaf {
    fn serialize<E, F>(self, f: &mut F) -> Result<PackOffset, E>
    where
        F: FnMut(&[u8]) -> Result<PackOffset, E>,
    {
        (f)(&Leaf2 {
            hash: self.hash.0,
            offset: self.ptr.offset.0,
            len: self.ptr.len,
        }
        .into_bytes())
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leaf(x) => x.fmt(f),
            Self::Parent(x) => x.fmt(f),
        }
    }
}

impl fmt::Debug for Parent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_map();
        // to panic or not to?
        // probably not, given Debug is especially useful when panicking
        let mut it = self.branches.iter();
        for i in (0..16).filter(|i| self.population >> i & 1 != 0) {
            let Some(x) = it.next() else {
                let _ = f.entry(&format_args!("{:x}", i), &"<???>");
                continue;
            };
            f.entry(&format_args!("{:x}", i), &x);
        }
        for x in it {
            f.entry(&"<???>", &x);
        }
        f.finish()
    }
}

impl fmt::Debug for Leaf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} -> {:#x}+{}",
            self.hash, self.ptr.offset.0, self.ptr.len
        )
    }
}
