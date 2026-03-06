#[cfg(feature = "magic")]
mod magic;
mod unix;

use std::{
    collections::BTreeMap,
    error::Error,
    fs, io,
    io::Write,
    ops,
    path::{Path, PathBuf},
};
use toa::{Hash, Object};

type Result<T> = core::result::Result<T, Box<dyn Error>>;
type InnerToa = toa::Toa<toa::Blob<fs::File>>;

struct Toa {
    inner: InnerToa,
    meta: BTreeMap<Box<str>, Hash>,
}

#[derive(Default)]
struct Stat {
    size_sum: u64,
}

impl Toa {
    fn open(path: &Path) -> Result<Self> {
        let inner = toa::Toa::open(path)?;

        let root = inner.root();
        let mut meta = BTreeMap::default();

        if root != Hash::default() {
            let refs = inner
                .get(&root)
                .map_err(|e| format!("failed to get root from store: {e:?}"))?
                .ok_or("root is missing from store")?;
            let Object::Refs(refs) = refs else { todo!() };
            let Ok([data]) = refs.read_array(0) else {
                todo!()
            };
            let Ok(Some(data)) = inner.get(&data) else {
                todo!()
            };
            let Object::Data(data) = data else { todo!() };
            let data = {
                let mut b = vec![0; data.len()? as usize];
                data.read_exact(0, &mut b)
                    .map_err(|e| format!("root: failed to read data: {e:?}"))?;
                b
            };
            let mut offset = 0;
            for i in 1..refs.len()? {
                let kl = usize::from(data[offset]);
                offset += 1;
                let k = &data[offset..][..kl];
                let k = core::str::from_utf8(k).unwrap();
                offset += kl;
                let [v] = refs
                    .read_array(i)
                    .map_err(|e| format!("root: failed to read ref: {e:?}"))?;
                meta.insert(k.into(), v);
            }
        }

        Ok(Self { inner, meta })
    }

    fn get(&self, key: &Hash) -> Result<toa::Object<toa::Blob<fs::File>>> {
        self.inner
            .get(&key)
            .map_err(|e| format!("failed to query store: {e:?}"))?
            .ok_or_else(|| format!("no object with key {key:?}").into())
    }

    fn save_root(&mut self) -> Result<()> {
        let mut data =
            Vec::with_capacity(self.meta.keys().fold(self.meta.len(), |s, x| s + x.len()));
        for k in self.meta.keys() {
            let kl = u8::try_from(k.len()).map_err(|_| format!("meta key {k:?} too long"))?;
            data.push(kl);
            data.extend(k.bytes());
        }
        let root = self
            .add_data(&data)
            .map_err(|e| format!("failed to create meta data: {e:?}"))?;

        let mut hashes = Vec::with_capacity(1 + self.meta.len());
        hashes.push(root);
        hashes.extend(self.meta.values());
        let root = self
            .add_refs(&hashes)
            .map_err(|e| format!("failed to create meta refs: {e:?}"))?;

        self.set_root(root)
            .map_err(|e| format!("failed to set root: {e:?}"))?;
        Ok(())
    }

    fn meta(&self, name: &str) -> Option<Hash> {
        self.meta.get(name.into()).copied()
    }

    fn set_meta(&mut self, name: &str, value: &Hash) {
        self.meta.insert(name.into(), *value);
    }
}

impl ops::Deref for Toa {
    type Target = InnerToa;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ops::DerefMut for Toa {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Stat {
    fn summarize(self, toa: &Toa) {
        let toa_size = toa.inner.size_on_disk();
        let Self { size_sum } = self;
        let ratio = size_sum as f64 / toa_size as f64;
        println!("pack size: {toa_size}, files size: {size_sum}, ratio: {ratio}");
    }
}

fn usage(procname: &str) -> Box<dyn Error> {
    let s = format!(
        "\
usage: {procname} <add|get|list>
    get <pack> <key>
        dump object data to stdout (may contain raw bytes!)
    list <pack>
        list all known objects
    scrub <pack>
        verify pack integrity
    unix new <pack> <directory>
    unix get <pack> <path>
    unix ls <pack> [path]"
    );
    #[cfg(feature = "magic")]
    let s = s + "
    magic all <pack>
        list all objects along with detected file type";
    s.into()
}

fn args_end<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    args.next()
        .is_none()
        .then_some(())
        .ok_or_else(|| usage(procname))
}

fn parse_hex<const N: usize>(key: &str) -> Result<[u8; N]> {
    if key.len() != const { N * 2 } {
        return Err("key doesn't have expected length".into());
    }
    let mut k = [0; N];
    for (xy, w) in key.as_bytes().chunks_exact(2).zip(k.iter_mut()) {
        let &[x, y] = xy.try_into().expect("exactly 2 bytes");
        let f = |x| match x {
            b'0'..=b'9' => Ok(x - b'0'),
            b'a'..=b'f' => Ok(x - b'a' + 10),
            b'A'..=b'F' => Ok(x - b'A' + 10),
            c => Err(format!("invalid hex char {:?}", c as char)),
        };
        *w = f(x)? << 4 | f(y)?;
    }
    Ok(k)
}

fn add_files<A>(dev: &mut Toa, args: A) -> Result<Stat>
where
    A: Iterator<Item = String>,
{
    let mut stat = Stat::default();
    for file in args {
        let key = add_file(dev, &file, &mut stat)?;
        println!("{key:?} {file}");
    }
    Ok(stat)
}

fn add_file(dev: &mut Toa, path: &str, stat: &mut Stat) -> Result<Hash> {
    let data = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|e| format!("failed to open {path:?}: {e}"))?;
    // FIXME other processes *can* modify "CoW" mappings,
    // so that's a very big problem...
    let data = unsafe {
        memmap2::MmapOptions::new()
            .populate()
            .map_copy_read_only(&data)
            .map_err(|e| format!("failed to memory-map {path:?}: {e}"))?
    };
    stat.size_sum += u64::try_from(data.len()).expect("usize <= u64");
    let key = dev
        .add_data(&data)
        .map_err(|e| format!("failed to add {path:?} to store: {e:?}"))?;
    Ok(key)
}

fn dump_object(dev: &Toa, key: &Hash) -> Result<()> {
    let obj = dev.get(&key)?;
    let Object::Data(obj) = obj else {
        todo!("dump refs?")
    };
    let mut out = io::stdout().lock();
    let buf = &mut [0; 1 << 13];
    let mut offt = 0;
    loop {
        let n = obj
            .read(offt, buf)
            .map_err(|e| format!("failed to read object: {e:?}"))?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n])
            .map_err(|e| format!("failed to write to stdout: {e:?}"))?;
        offt += n as u128;
    }
    Ok(())
}

fn cmd_get<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let key = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let store = PathBuf::from(store);

    let key = toa::Hash::from_bytes(parse_hex(&key)?);
    let dev = Toa::open(&store)?;
    dump_object(&dev, &key)?;

    Ok(())
}

fn cmd_list<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let store = PathBuf::from(store);

    let dev = Toa::open(&store)?;
    dev.iter_with(|key| {
        println!("{key:?}");
        false
    })
    .map_err(|e| format!("failure during store iteration: {e:?}"))?;

    Ok(())
}

fn cmd_scrub<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let store = PathBuf::from(store);

    let dev = Toa::open(&store)?;
    todo!("implement Toa::scrub");
}

fn start() -> Result<()> {
    let mut args = std::env::args();
    let procname = args.next();
    let procname = procname.as_deref().unwrap_or("toa-cli");
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "get" => cmd_get(procname, args),
        "list" => cmd_list(procname, args),
        "scrub" => cmd_scrub(procname, args),
        "unix" => unix::cmd(procname, args),
        #[cfg(feature = "magic")]
        "magic" => magic::cmd(procname, args),
        _ => Err(usage(procname)),
    }
}

fn main() {
    match start() {
        Ok(()) => {}
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
}
