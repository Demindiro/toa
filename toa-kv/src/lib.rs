#[cfg_attr(not(feature = "std"), no_std)]
#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "sled")]
pub use sled;

pub trait ToaKv {
    type Error;
    type Get<'a>: AsRef<[u8]>
    where
        Self: 'a;
    type Key: AsRef<[u8]>;

    fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;
    fn get<'a>(&'a self, key: &[u8]) -> Result<Option<Self::Get<'a>>, Self::Error>;
    fn iter_prefix_with(
        &self,
        prefix: &[u8],
        f: &mut dyn FnMut(Self::Key),
    ) -> Result<(), Self::Error>;

    fn size_on_disk(&self) -> Result<u128, Self::Error>;

    fn has<'a>(&'a self, key: &[u8]) -> Result<bool, Self::Error> {
        self.get(key).map(|x| x.is_some())
    }
}

pub struct StupidRefCell<T>(pub T);

impl<T> AsRef<T> for StupidRefCell<core::cell::Ref<'_, T>>
where
    T: ?Sized,
{
    fn as_ref(&self) -> &T {
        &self.0
    }
}

#[cfg(feature = "alloc")]
impl ToaKv for core::cell::RefCell<alloc::collections::BTreeMap<Box<[u8]>, Box<[u8]>>> {
    type Error = core::convert::Infallible;
    type Get<'a>
        = StupidRefCell<core::cell::Ref<'a, [u8]>>
    where
        Self: 'a;
    type Key = Box<[u8]>;

    fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.borrow_mut().insert(key.into(), value.into());
        Ok(())
    }

    fn get<'a>(&'a self, key: &[u8]) -> Result<Option<Self::Get<'a>>, Self::Error> {
        Ok(
            core::cell::Ref::filter_map(self.borrow(), |x| x.get(key).map(|x| &**x))
                .ok()
                .map(StupidRefCell),
        )
    }

    fn iter_prefix_with(
        &self,
        prefix: &[u8],
        f: &mut dyn FnMut(Self::Key),
    ) -> Result<(), Self::Error> {
        // FIXME idkwtfeven
        //self.borrow().range(prefix..).map(|x| x.0).take_while(|x| x.starts_with(prefix)).for_each(|x| (f)(x));
        self.borrow()
            .iter()
            .map(|x| x.0)
            .filter(|x| x.starts_with(prefix))
            .for_each(|x| (f)(x.clone()));
        Ok(())
    }

    fn size_on_disk(&self) -> Result<u128, Self::Error> {
        // TODO rough estimate
        let x = self.borrow().iter().fold(core::mem::size_of_val(self), |s, x| s + core::mem::size_of::<[usize; 4]>() + x.0.len() + x.1.len());
        Ok(x as u128)
    }
}

#[cfg(feature = "sled")]
impl ToaKv for sled::Tree {
    type Error = sled::Error;
    type Get<'a>
        = sled::IVec
    where
        Self: 'a;
    type Key = sled::IVec;

    fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        sled::Tree::insert(self, key, value).map(|_| ())
    }

    fn get<'a>(&'a self, key: &[u8]) -> Result<Option<Self::Get<'a>>, Self::Error> {
        sled::Tree::get(self, key)
    }

    fn iter_prefix_with(
        &self,
        prefix: &[u8],
        f: &mut dyn FnMut(Self::Key),
    ) -> Result<(), Self::Error> {
        sled::Tree::scan_prefix(self, prefix).try_for_each(|x| Ok((f)(x?.0)))
    }

    fn size_on_disk(&self) -> Result<u128, Self::Error> {
        // FIXME
        //sled::Db::size_on_disk(self)
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
