use std::{fs::File, io};

impl super::Read for File {
    type Error = io::Error;

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        // TODO potential race condition
        io::Seek::seek(&mut (&*self), io::SeekFrom::Start(offset))?;
        io::Read::read_exact(&mut (&*self), out)
    }
}

impl super::Write for File {
    type Error = io::Error;

    fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let offset = io::Seek::stream_position(self)?;
        io::Write::write_all(self, data)?;
        Ok(offset)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}
