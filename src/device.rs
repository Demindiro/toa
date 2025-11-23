pub trait Device {
    type Read<'a>: AsRef<[u8]>
    where
        Self: 'a;
    type Write<'a>: Write<Error = Self::Error>
    where
        Self: 'a;
    type Error;

    fn read<'a>(
        &'a mut self,
        offset: u64,
        bytes: usize,
    ) -> Result<Self::Read<'a>, Self::Error>;
    fn write<'a>(
        &'a mut self,
        bytes: usize,
    ) -> Result<Self::Write<'a>, Self::Error>;

    fn len(&self) -> u64;
    fn optimal_alignment(&self) -> Alignment;

    fn sync(&mut self) -> Result<(), Self::Error>;
    fn wipe(&mut self) -> Result<(), Self::Error>;
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
    vec: &'a mut Vec<T>,
    remaining: usize,
    offset: u64,
}

impl Device for Vec<u8> {
    type Read<'a>
        = &'a [u8]
    where
        Self: 'a;
    type Write<'a>
        = VecWrite<'a, u8>
    where
        Self: 'a;
    type Error = &'static str;

    fn read(
        &mut self,
        offset: u64,
        bytes: usize,
    ) -> Result<&[u8], Self::Error> {
        usize::try_from(offset)
            .ok()
            .and_then(|x| x.checked_add(bytes).map(|y| x..y))
            .and_then(|x| self.get(x))
            .ok_or("out of bounds")
    }

    fn write(&mut self, bytes: usize) -> Result<VecWrite<'_, u8>, Self::Error> {
        let offset = u64::try_from(self.len()).unwrap();
        Vec::reserve(self, bytes);
        Ok(VecWrite {
            vec: self,
            remaining: bytes,
            offset,
        })
    }

    fn len(&self) -> u64 {
        <[u8]>::len(self).try_into().unwrap_or(u64::MAX)
    }

    fn optimal_alignment(&self) -> Alignment {
        Alignment::N0
    }

    fn wipe(&mut self) -> Result<(), Self::Error> {
        Ok(<[u8]>::fill(self, 0))
    }

    fn sync(&mut self) -> Result<(), Self::Error> {
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
