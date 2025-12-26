use super::{Byte, ByteIndex, Leaf2, U256};
use crate::{Hash, ObjectRaw, PackOffset};
use alloc::boxed::Box;
use core::{fmt, mem};

#[derive(Debug)]
pub(crate) struct ObjectTrie {
    root: Node,
}

pub(crate) struct Insert<'a, 'h> {
    replace: InsertNode<'a>,
    key: &'h Hash,
}

enum InsertNode<'a> {
    Parent(Byte, &'a mut Parent),
    Leaf(ByteIndex, &'a mut Node),
}

enum Node {
    Parent(Parent),
    Leaf(Leaf),
}

struct Parent {
    population: U256,
    branches: Box<[Node]>,
}

#[derive(Clone, Copy)]
struct Leaf {
    hash: Hash,
    ptr: ObjectRaw,
}

impl ObjectTrie {
    pub fn with_leaf(key: &Hash, ptr: ObjectRaw) -> Self {
        Self {
            root: Node::Leaf(Leaf { hash: *key, ptr }),
        }
    }

    pub fn try_insert<'a, 'h>(&'a mut self, key: &'h Hash) -> Option<Insert<'a, 'h>> {
        let mut cur = &mut self.root;
        let mut index = ByteIndex(0);
        loop {
            match cur {
                Node::Parent(x) => {
                    let byte = index.get(&key);
                    // https://github.com/rust-lang/rust/issues/21906
                    if !x.contains_byte(byte) {
                        break Some(Insert {
                            replace: InsertNode::Parent(byte, x),
                            key,
                        });
                    }
                    cur = x.get_mut(byte).expect("contains nibble");
                }
                Node::Leaf(Leaf { hash, .. }) => {
                    break if hash == key {
                        None
                    } else {
                        let replace = InsertNode::Leaf(index, cur);
                        Some(Insert { replace, key })
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

impl Insert<'_, '_> {
    pub fn insert(self, ptr: ObjectRaw) {
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
    fn new_pair(index: ByteIndex, [a, b]: [Leaf; 2]) -> Self {
        let [x, y] = [a, b].map(|x| (index.get(&x.hash), Node::Leaf(x)));
        if x.0 != y.0 {
            Self::new([x, y])
        } else {
            let p = Self::new_pair(index.next(), [a, b]);
            Self::new([(x.0, Node::Parent(p))])
        }
    }

    fn new<const N: usize>(mut branches: [(Byte, Node); N]) -> Self {
        const { assert!(N <= 256) }
        let population = branches.iter().fold(U256::ZERO, |s, x| s.with_bit(x.0.0));
        assert_eq!(
            usize::from(population.count_ones()),
            branches.len(),
            "{population:016x?}  {branches:#?}"
        );
        branches.sort_by_key(|x| x.0);
        Self {
            population,
            branches: branches.map(|x| x.1).into(),
        }
    }

    fn insert(&mut self, byte: Byte, node: Node) {
        assert!(!self.contains_byte(byte));
        self.population.set_bit(byte.0);
        let mut branches = mem::take(&mut self.branches).into_vec();
        let i = self.byte_to_index(byte).expect("population bit set");
        branches.reserve(1);
        branches.insert(i, node);
        self.branches = branches.into();
    }

    fn contains_byte(&self, byte: Byte) -> bool {
        self.byte_to_index(byte).is_some()
    }

    fn get_mut(&mut self, byte: Byte) -> Option<&mut Node> {
        self.byte_to_index(byte).map(|i| &mut self.branches[i])
    }

    fn byte_to_index(&self, Byte(byte): Byte) -> Option<usize> {
        self.population.test_bit(byte).then(|| {
            (self.population & U256::trailing_mask(byte))
                .count_ones()
                .into()
        })
    }

    fn serialize<E, F>(self, f: &mut F) -> Result<PackOffset, E>
    where
        F: FnMut(&[u8]) -> Result<PackOffset, E>,
    {
        debug_assert_eq!(self.branches.len(), self.population.count_ones() as usize);
        // stack-allocated buffer would take 2080 bytes -- too much!
        let capacity = 32 + 8 * self.branches.len();
        let mut buf = Vec::with_capacity(capacity);
        buf.extend(self.population.to_le_bytes());
        for x in self.branches {
            buf.extend(x.serialize(f)?.0.to_le_bytes());
        }
        assert_eq!(buf.len(), capacity);
        (f)(&buf)
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
        for i in (0..=u8::MAX).filter(|&i| self.population.test_bit(i)) {
            let Some(x) = it.next() else {
                let _ = f.entry(&format_args!("{:02x}", i), &"<???>");
                continue;
            };
            f.entry(&format_args!("{:02x}", i), &x);
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
