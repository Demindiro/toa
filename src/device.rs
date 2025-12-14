use alloc::vec::Vec;
use core::cell::{self, RefCell};

pub trait Device {
    type Read<'a>: AsRef<[u8]>
    where
        Self: 'a;
    type Write<'a>: Write<Error = Self::Error>
    where
        Self: 'a;
    type Error;

    fn read<'a>(&'a self, offset: u64, bytes: usize) -> Result<Self::Read<'a>, Self::Error>;
    fn write<'a>(&'a self, bytes: usize) -> Result<Self::Write<'a>, Self::Error>;

    fn len(&self) -> u64;
    fn optimal_alignment(&self) -> Alignment;

    fn sync(&self) -> Result<(), Self::Error>;
    fn wipe(&self) -> Result<(), Self::Error>;
}

pub trait Write {
    type Error;

    fn append(&mut self, data: &[u8]) -> Result<(), Self::Error>;

    fn offset(&self) -> u64;
}

pub enum Alignment {
    N0 = 0,
    N1 = 1,
    N2 = 2,
    N3 = 3,
    N4 = 4,
    N5 = 5,
    N6 = 6,
    N7 = 7,
    N8 = 8,
    N9 = 9,
    N10 = 10,
    N11 = 11,
    N12 = 12,
    N13 = 13,
    N14 = 14,
    N15 = 15,
    N16 = 16,
    N17 = 17,
    N18 = 18,
    N19 = 19,
    N20 = 20,
    N21 = 21,
    N22 = 22,
    N23 = 23,
    N24 = 24,
    N25 = 25,
    N26 = 26,
    N27 = 27,
    N28 = 28,
    N29 = 29,
    N30 = 30,
    N31 = 31,
    N32 = 32,
}

pub struct VecWrite<'a, T> {
    vec: cell::RefMut<'a, Vec<T>>,
    remaining: usize,
    offset: u64,
}

// retardation
pub struct RefAsRef<'a, T: ?Sized>(cell::Ref<'a, T>);

impl Device for RefCell<Vec<u8>> {
    type Read<'a>
        = RefAsRef<'a, [u8]>
    where
        Self: 'a;
    type Write<'a>
        = VecWrite<'a, u8>
    where
        Self: 'a;
    type Error = &'static str;

    fn read(&self, offset: u64, bytes: usize) -> Result<RefAsRef<'_, [u8]>, Self::Error> {
        let vec = self.borrow();
        usize::try_from(offset)
            .ok()
            .and_then(|x| x.checked_add(bytes).map(|y| x..y))
            .filter(|x| x.end <= vec.len())
            .map(|x| cell::Ref::map(vec, |y| &y[x]))
            .map(RefAsRef)
            .ok_or("out of bounds")
    }

    fn write(&self, bytes: usize) -> Result<VecWrite<'_, u8>, Self::Error> {
        let mut vec = self.borrow_mut();
        let offset = u64::try_from(vec.len()).unwrap();
        vec.reserve(bytes);
        Ok(VecWrite {
            vec,
            remaining: bytes,
            offset,
        })
    }

    fn len(&self) -> u64 {
        self.borrow().len().try_into().unwrap_or(u64::MAX)
    }

    fn optimal_alignment(&self) -> Alignment {
        Alignment::N0
    }

    fn wipe(&self) -> Result<(), Self::Error> {
        Ok(self.borrow_mut().fill(0))
    }

    fn sync(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Write for VecWrite<'_, u8> {
    type Error = &'static str;

    fn append(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        self.remaining
            .checked_sub(data.len())
            .ok_or("too much data")
            .map(|x| {
                self.remaining = x;
                self.vec.extend_from_slice(data)
            })
    }

    fn offset(&self) -> u64 {
        self.offset
    }
}

impl<T> AsRef<T> for RefAsRef<'_, T>
where
    T: ?Sized,
{
    fn as_ref(&self) -> &T {
        &*self.0
    }
}
