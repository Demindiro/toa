use alloc::vec::Vec;
use core::cell::{self, RefCell};

pub trait Device {
    type Read<'a>: AsRef<[u8]>
    where
        Self: 'a;
    type Error;

    fn read<'a>(&'a self, offset: u64, bytes: usize) -> Result<Self::Read<'a>, Self::Error>;
    /// # Returns
    ///
    /// The offset to the written data.
    fn write<'a>(&'a self, data: &[u8]) -> Result<u64, Self::Error>;

    fn len(&self) -> u64;

    fn sync(&self) -> Result<(), Self::Error>;
    fn wipe(&self) -> Result<(), Self::Error>;
}

// retardation
pub struct RefAsRef<'a, T: ?Sized>(cell::Ref<'a, T>);

impl Device for RefCell<Vec<u8>> {
    type Read<'a>
        = RefAsRef<'a, [u8]>
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

    fn write(&self, data: &[u8]) -> Result<u64, Self::Error> {
        let mut vec = self.borrow_mut();
        let offset = u64::try_from(vec.len()).unwrap();
        vec.extend_from_slice(data);
        Ok(offset)
    }

    fn len(&self) -> u64 {
        self.borrow().len().try_into().unwrap_or(u64::MAX)
    }

    fn wipe(&self) -> Result<(), Self::Error> {
        Ok(self.borrow_mut().fill(0))
    }

    fn sync(&self) -> Result<(), Self::Error> {
        Ok(())
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
