#[cfg(feature = "std")]
pub mod fs;

pub trait Read {
    type Error;

    fn read<'a>(&'a self, offset: u64, out: &mut [u8]) -> Result<(), Self::Error>;
}

pub trait Write {
    type Error;

    fn append(&mut self, data: &[u8]) -> Result<u64, Self::Error>;
    fn sync(&mut self) -> Result<(), Self::Error>;
}

impl Read for &[u8] {
    type Error = &'static str;

    fn read<'a>(&'a self, offset: u64, out: &mut [u8]) -> Result<(), Self::Error> {
        read_slice(self, offset, out)
    }
}

#[cfg(any(feature = "alloc", test))]
impl Read for alloc::vec::Vec<u8> {
    type Error = <&'static [u8] as Read>::Error;

    fn read<'a>(&'a self, offset: u64, out: &mut [u8]) -> Result<(), Self::Error> {
        read_slice(self, offset, out)
    }
}

#[cfg(any(feature = "alloc", test))]
impl Write for alloc::vec::Vec<u8> {
    type Error = &'static str;

    fn append(&mut self, data: &[u8]) -> Result<u64, Self::Error> {
        let offset = self.len().try_into().map_err(|_| "offset out of bounds")?;
        self.extend_from_slice(data);
        Ok(offset)
    }

    fn sync(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

// rust is doing something very stupid with lifetimes.
fn read_slice(data: &[u8], offset: u64, out: &mut [u8]) -> Result<(), &'static str> {
    usize::try_from(offset)
        .ok()
        .and_then(|x| x.checked_add(out.len()).map(|y| x..y))
        .and_then(|x| data.get(x))
        .ok_or("out of bounds")
        .map(|x| out.copy_from_slice(x))
}
