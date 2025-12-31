use core::{
    cell::{Cell, Ref, RefCell},
    fmt,
};

pub trait Cache<V> {
    type Get<'a>: core::ops::Deref<Target = V>
    where
        Self: 'a;

    /// Insert a new entry.
    ///
    /// This method does *not* check for duplicate keys.
    /// It is up to the caller to ensure no duplicate keys are present.
    fn insert(&self, key: Key, value: V) -> Self::Get<'_>;

    /// Get a previously inserted entry.
    ///
    /// Entries may get removed spuriously.
    fn get(&self, key: Key) -> Option<Self::Get<'_>>;
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(pub u64);

/// Very simple LRU cache primarily intended for testing.
pub struct MicroLru<V> {
    keys: [Cell<Key>; 16],
    values: RefCell<[V; 16]>,
    chain: [Link; 16],
    link: Link,
}

#[derive(Default)]
struct Link {
    head: Cell<u8>,
    tail: Cell<u8>,
}

impl Key {
    pub(crate) fn from_depth_index(depth: u8, index: u64) -> Self {
        const MAX_OFFSET_P2: u8 = crate::PITCH
            + (crate::PITCH - crate::record::Entry::LEN.trailing_zeros() as u8) * crate::DEPTH;
        const MAX_OFFSET: u64 = 1 << MAX_OFFSET_P2;
        assert!(depth <= crate::DEPTH);
        assert!(index <= MAX_OFFSET);
        Self(u64::from(depth) << MAX_OFFSET_P2 | index)
    }
}

impl Default for Key {
    fn default() -> Self {
        Self(u64::MAX)
    }
}

impl<V> MicroLru<V> {
    fn find(&self, key: Key) -> Option<usize> {
        self.keys.iter().position(|k| k.get() == key)
    }

    fn last(&self) -> usize {
        usize::from(self.link.tail.get())
    }

    fn bump(&self, index: usize) {
        let x = &self.chain[index];
        if let Some(a) = self.chain.get(usize::from(x.head.get())) {
            a.tail.set(x.tail.get());
        } else {
            // already at head
            return;
        }
        if let Some(b) = &self.chain.get(usize::from(x.tail.get())) {
            b.head.set(x.head.get());
        } else {
            self.link.tail.set(x.head.get());
        }
        x.head.set(u8::MAX);
        x.tail.set(self.link.head.get());
        self.chain[usize::from(self.link.head.get())]
            .head
            .set(index as u8);
        self.link.head.set(index as u8);
    }
}

impl<V> Cache<V> for MicroLru<V> {
    type Get<'a>
        = Ref<'a, V>
    where
        Self: 'a;

    fn insert(&self, key: Key, value: V) -> Self::Get<'_> {
        let i = self.last();
        self.keys[i].set(key);
        self.values.borrow_mut()[i] = value;
        self.bump(i);
        Ref::map(self.values.borrow(), |x| &x[i])
    }

    fn get(&self, key: Key) -> Option<Self::Get<'_>> {
        self.find(key)
            .map(|i| Ref::map(self.values.borrow(), |x| &x[i]))
    }
}

impl<V> Default for MicroLru<V>
where
    V: Default,
{
    fn default() -> Self {
        let s = Self {
            keys: Default::default(),
            values: Default::default(),
            chain: Default::default(),
            link: Link {
                head: Cell::new(0),
                tail: Cell::new(15),
            },
        };
        for (i, l) in s.chain.iter().enumerate() {
            let i = i as u8;
            l.head.set(i.wrapping_sub(1));
            l.tail.set(i + 1);
        }
        s
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key({:016x})", self.0)
    }
}

impl<V> fmt::Debug for MicroLru<V>
where
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct FmtCells<'a, T>(&'a [Cell<T>]);

        impl<T> fmt::Debug for FmtCells<'_, T>
        where
            T: fmt::Debug + Copy,
        {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_list()
                    .entries(self.0.iter().map(Cell::get))
                    .finish()
            }
        }

        f.debug_struct(stringify!(MicroLru))
            .field("keys", &FmtCells(&self.keys))
            .field("values", &self.values.try_borrow())
            .field("chain", &self.chain)
            .field("link", &self.link)
            .finish()
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.head.get(), self.tail.get())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn microlru_add_one() {
        let lru = MicroLru::default();
        assert!(lru.get(Key(0)).is_none());
        assert_eq!(&**lru.insert(Key(0), "yo"), "yo");
        assert_eq!(&**lru.get(Key(0)).unwrap(), "yo");
    }

    #[test]
    fn microlru_add_many() {
        let lru = MicroLru::default();
        for i in 0..16 {
            lru.insert(Key(i), i);
        }
        for i in 0..16 {
            assert_eq!(*lru.get(Key(i)).unwrap(), i);
        }
    }

    #[test]
    fn microlru_add_many_overflow() {
        let lru = MicroLru::default();
        dbg!(&lru);
        for i in (0..32).rev() {
            lru.insert(Key(i), i);
            dbg!(&lru);
        }
        for i in 0..16 {
            assert_eq!(*lru.get(Key(i)).unwrap(), i);
        }
        for i in 16..32 {
            assert!(lru.get(Key(i)).is_none());
        }
    }
}
