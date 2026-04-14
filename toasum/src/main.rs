use std::io;
use toa_hash::{Domain, Hash};

#[derive(Default)]
struct Program {
    verbose: bool,
}

fn usage(progname: Option<&str>) -> ! {
    let progname = progname.unwrap_or("toasum");
    eprintln!("usage: {progname} [FILE]...");
    std::process::exit(1);
}

impl Program {
    fn hash_file(&self, path: &str) -> io::Result<Hash> {
        let data = std::fs::OpenOptions::new().read(true).open(path)?;
        // FIXME other processes *can* modify "CoW" mappings,
        // so that's a very big problem...
        let data = unsafe {
            memmap2::MmapOptions::new()
                .populate()
                .map_copy_read_only(&data)?
        };
        let hash = toa_hash::hash(Domain::Data, &data);
        Ok(hash)
    }

    fn hash_dir(&self, path: &str) -> io::Result<Hash> {
        let mut items = vec![];

        let e = || io::Error::new(io::ErrorKind::InvalidData, "filename must be UTF-8");

        for x in std::fs::read_dir(path)? {
            let x = x?;
            let path = x.path();
            let path = path.to_str().ok_or_else(e)?;
            let name = x.file_name().into_string().map_err(|_| e())?;
            if name.len() > usize::from(u8::MAX) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "filename is too long"));
            }
            let hash = self.hash_one(path)?;
            items.push((name, hash));
        }

        items.sort_by(|x, y| x.0.cmp(&y.0));
        let mut names = Vec::with_capacity(items.iter().fold(items.len(), |s, x| s + x.0.len()));
        let mut hashes = Vec::with_capacity(1 + items.len());
        hashes.push(Default::default());
        for x in items {
            names.push(x.0.len() as u8);
            names.extend(x.0.bytes());
            hashes.push(x.1);
        }

        hashes[0] = toa_hash::hash(Domain::Data, &names);
        let hash = toa_hash::hash(Domain::Refs, bytemuck::cast_slice(&hashes));
        Ok(hash)
    }

    fn hash_one(&self, path: &str) -> io::Result<Hash> {
        let hash = if std::fs::metadata(path)?.is_dir() {
            self.hash_dir(path)?
        } else {
            self.hash_file(path)?
        };
        if self.verbose {
            println!("{hash}  {path}");
        }
        Ok(hash)
    }

    fn cmd_hash<I>(&self, files: I) -> io::Result<()>
    where
        I: IntoIterator<Item = String>,
    {
        for x in files {
            let hash = self.hash_one(&x)?;
            if !self.verbose {
                println!("{hash}  {x}");
            }
        }
        Ok(())
    }
}

fn main() -> io::Result<()> {
    let mut args = std::env::args();
    let progname = args.next();
    let progname = progname.as_deref();

    let mut args = args.peekable();
    let mut program = Program::default();

    while let Some(x) = args.peek() {
        if !x.starts_with("-") {
            break;
        }
        match args.next().expect("peek").as_ref() {
            "-v" => program.verbose = true,
            _ => usage(progname),
        }
    }

    if args.peek().is_none() {
        program.cmd_hash(["-".to_string()])?
    } else {
        program.cmd_hash(args)?
    }

    Ok(())
}
