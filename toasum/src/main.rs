use std::io::{self, BufRead};
use toa_hash::Hash;

#[derive(Default)]
struct Program {
    check: bool,
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
        let hash = toa_hash::hash(&data);
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "filename is too long",
                ));
            }
            let hash = self.hash_one(path)?;
            items.push((name, hash));
        }

        items.sort_by(|x, y| x.0.cmp(&y.0));
        let mut names = Vec::with_capacity(items.iter().fold(items.len(), |s, x| s + x.0.len()));
        let mut hash = Hash::NIL;
        for (_, x) in items.iter().rev() {
            hash = toa_hash::hash_refs(*x, hash);
        }
        for x in items {
            names.push(x.0.len() as u8);
            names.extend(x.0.bytes());
        }

        hash = toa_hash::hash_refs(toa_hash::hash(&names), hash);
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

    fn check_one(&self, path: &str) -> io::Result<u64> {
        let mut num_fail = 0;
        let x = std::fs::OpenOptions::new().read(true).open(path)?;
        for x in io::BufReader::new(x).lines() {
            let x = x?;
            // require exactly 2 spaces as separator to ensure we don't fumble paths with spaces in them.
            let (file, sep, expect_hash) = (&x[66..], &x[64..66], &x[..64]);
            if sep != "  " {
                todo!("malformed line");
            }
            let expect_hash = expect_hash.parse::<Hash>().unwrap();
            let hash = self.hash_one(file)?;
            let is_ok = hash == expect_hash;
            let status = is_ok.then_some("OK").unwrap_or("FAILED");
            num_fail += u64::from(!is_ok);
            println!("{file}: {status}");
        }
        Ok(num_fail)
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

    fn cmd_check<I>(&self, files: I) -> io::Result<()>
    where
        I: IntoIterator<Item = String>,
    {
        let mut num_fail = 0;
        for x in files {
            num_fail += self.check_one(&x)?;
        }
        if num_fail != 0 {
            let s = (num_fail == 1).then_some("").unwrap_or("s");
            eprintln!("{num_fail} check{s} failed");
            std::process::exit(1);
        }
        Ok(())
    }

    fn run<I>(&self, files: I) -> io::Result<()>
    where
        I: IntoIterator<Item = String>,
    {
        if self.check {
            self.cmd_check(files)
        } else {
            self.cmd_hash(files)
        }
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
            "-c" => program.check = true,
            _ => usage(progname),
        }
    }

    if args.peek().is_none() {
        program.run(["-".to_string()])?
    } else {
        program.run(args)?
    }

    Ok(())
}
