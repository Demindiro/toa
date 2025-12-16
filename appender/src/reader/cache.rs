use core::cell::{Cell, Ref, RefCell};

pub trait Cache<V> {
    type Get<'a>: core::ops::Deref<Target = V>
    where
        Self: 'a;

    fn insert(&self, key: Key, value: V) -> Self::Get<'_>;
    fn get(&self, key: Key) -> Option<Self::Get<'_>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(pub u64);

/// Very simple direct-mapped cache primarily intended for testing.
#[derive(Default)]
pub struct MicroLru<V> {
    keys: [Cell<Key>; 32],
    values: RefCell<[V; 32]>,
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

impl<V> Cache<V> for MicroLru<V> {
    type Get<'a>
        = Ref<'a, V>
    where
        Self: 'a;

    // TODO this isn't an LRU...
    fn insert(&self, key: Key, value: V) -> Self::Get<'_> {
        let i = key.0 as usize % self.keys.len();
        self.keys[i].set(key);
        self.values.borrow_mut()[i] = value;
        Ref::map(self.values.borrow(), |x| &x[i])
    }

    fn get(&self, key: Key) -> Option<Self::Get<'_>> {
        let i = key.0 as usize % self.keys.len();
        (self.keys[i].get() == key).then(|| Ref::map(self.values.borrow(), |x| &x[i]))
    }
}
