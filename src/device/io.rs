use std::{io::{self, Read}, cell::{Cell, RefCell}};

pub struct IoDevice<T, F> {
    dev: RefCell<T>,
    len: Cell<u64>,
    sync: F,
}

impl<T, F> IoDevice<T, F> {
    pub fn new(device: T, len: u64, sync: F) -> Self {
        Self { dev: RefCell::new(device), len: Cell::new(len), sync }
    }
}

impl<T, F> super::Device for IoDevice<T, F>
where
    T: io::Read + io::Write + io::Seek,
    F: Fn(&mut T) -> io::Result<()>,
{
    type Read<'a> = Box<[u8]>
    where
        Self: 'a;
    type Error = io::Error;

    fn read(&self, offset: u64, len: usize) -> io::Result<Self::Read<'_>> {
        let len64 = u64::try_from(len).expect("usize <= u64");
        let mut buf = Vec::with_capacity(len);
        let mut dev = self.dev.borrow_mut();
        dev.seek(io::SeekFrom::Start(offset))?;
        let real_len = (&mut *dev).take(len64).read_to_end(&mut buf)?;
        if real_len != len {
            todo!("error read_len != len");
        }
        Ok(buf.into())
    }

    fn write(&self, data: &[u8]) -> io::Result<u64> {
        let mut dev = self.dev.borrow_mut();
        let offset = self.len.get();
        let data_len = u64::try_from(data.len()).expect("usize <= u64");
        self.len.set(offset.checked_add(data_len).expect("file length overflow"));
        dev.seek(io::SeekFrom::Start(offset))?;
        dev.write_all(data).map(|()| offset)
    }

    fn len(&self) -> u64 {
        self.len.get()
    }

    fn sync(&self) -> io::Result<()> {
        (self.sync)(&mut self.dev.borrow_mut())
    }
}
