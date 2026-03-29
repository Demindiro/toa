#[cfg(feature = "magic")]
mod magic;
mod unix;

use std::{
    collections::BTreeMap,
    error::Error,
    fs, io,
    io::{Read, Write},
    ops,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use toa::{Compression, Hash, PageSize};
use toa_blob::{BlobStore, FileBlocks};

type Result<T> = core::result::Result<T, Box<dyn Error>>;
type Store = BlobStore<FileBlocks>;
type InnerToa = toa::Toa<Store>;
type Object<'a> = toa::Object<'a, Store>;

struct Toa {
    inner: InnerToa,
    meta: BTreeMap<Box<str>, Hash>,
}

struct Stat {
    original_disk_size: u64,
    size_sum: u64,
}

impl Toa {
    fn load(path: &Path, write: bool) -> Result<Self> {
        let inner = {
            let mut hdr = [0; 32];
            let dev = fs::OpenOptions::new().read(true).write(write).open(path)?;
            (&dev).read_exact(&mut hdr)?;
            let hdr = toa_blob::snoop_header(hdr).unwrap();
            let blk = match hdr.block_size {
                512 => toa_blob::BlockShift::N9,
                4096 => toa_blob::BlockShift::N12,
                x => todo!("block size {x}"),
            };
            let dev = FileBlocks::wrap(blk, hdr.zone_blocks, hdr.zone_count, dev);
            let store = BlobStore::load(dev)?;
            toa::Toa::load(store)?.ok_or("no store initialized")?
        };

        let root = inner.root();
        let mut meta = BTreeMap::default();

        if root != Hash::default() {
            let refs = inner
                .get(&root)
                .map_err(|e| format!("failed to get root from store: {e:?}"))?
                .ok_or("root is missing from store")?;
            let Object::Refs(refs) = refs else { todo!() };
            let Ok([data]) = refs.read_array(0) else {
                return Ok(Self { inner, meta });
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

    fn get(&self, key: &Hash) -> Result<Object<'_>> {
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

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
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
    fn new(toa: &Toa) -> Result<Self> {
        Ok(Self {
            original_disk_size: toa.size_on_disk()?,
            size_sum: 0,
        })
    }

    fn summarize(self, toa: &Toa) {
        let Self {
            original_disk_size,
            size_sum,
        } = self;
        let toa_size = toa.inner.size_on_disk().unwrap();
        let added = toa_size - original_disk_size;
        let ratio = size_sum as f64 / added as f64;
        println!("store size: {toa_size}, added: {added}, files size: {size_sum}, ratio: {ratio}");
    }
}

fn usage(procname: &str) -> Box<dyn Error> {
    let s = format!(
        "\
usage: {procname} <add|get|list>
    init <store>
        initialize a store
    get <store> <key>
        dump object data to stdout (may contain raw bytes!)
    list <store>
        list all known objects
    scrub <store>
        verify store integrity
    unix add <store> <name> <directory>
    unix get <store> <name> <path>
    unix ls <store> <name> [path]"
    );
    #[cfg(feature = "magic")]
    let s = s + "
    magic all <store>
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

fn cmd_init<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    eprint!("Will overwrite {store:?}. Continue? [y/N] ");
    let proceed = std::io::stdin()
        .lines()
        .next()
        .transpose()?
        .is_some_and(|x| matches!(&*x.trim().to_lowercase(), "y" | "yes"));
    if !proceed {
        eprintln!("aborting formatting");
        return Ok(());
    }

    eprintln!("continuing with formatting...");

    let store = PathBuf::from(store);

    let dev = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(store)?;
    let meta = dev.metadata()?;

    let block_size = match meta.blksize() {
        512 => toa_blob::BlockShift::N9,
        4096 => toa_blob::BlockShift::N12,
        x => panic!(
            "unsupported block size {x}. Please report a bug along with filesystem and disk information."
        ),
    };
    eprintln!("using {} blocks", fmt_size_si(block_size.into()));

    let mut len = meta.len();
    if len == 0 {
        eprintln!("file appears to be empty");
        eprint!("Please enter the desired file size (suffixes: K, M, G, T, P, E): ");
        let n = std::io::stdin().lines().next().transpose()?.unwrap();
        len = parse_size_si(&n).ok_or("invalid size")?;
        dev.set_len(len)?;
    }

    // default to 256MiB zone size
    // https://146a55aca6f00848c565-a7635525d40ac1c70300198708936b4e.ssl.cf1.rackcdn.com/images/133059501b4dfbcabffde7b8d0e3427481af62f1.pdf
    // > Initial de facto zone size chosen was 256MiB for all zones.
    // It works out to about 30k zones for a 8TB drive and ~2.5s for full zone copies. Seems reasonable?
    //
    // Simultaneously, ensure we have at least a couple hundred zones in case of very small drives
    // (or files).
    let mut zone_size = 1 << 28;
    const MIN_ZONES: u64 = 100;

    while len / zone_size < MIN_ZONES {
        zone_size >>= 1;
    }

    let zone_blocks = u32::try_from(zone_size / u64::from(block_size)).unwrap();
    eprintln!(
        "using {} zones ({zone_blocks} blocks)",
        fmt_size_si(zone_size.into())
    );

    let zone_count = u32::try_from(len / zone_size).unwrap();
    eprintln!("{zone_count} zones");

    eprintln!(
        "{} of slack at end of file",
        fmt_size_si(len - u64::from(zone_count) * zone_size)
    );

    let dev = toa_blob::FileBlocks::wrap(block_size, zone_blocks, zone_count, dev);

    let store = BlobStore::init(dev)?;
    let mut toa = toa::Toa::init(store, PageSize::K128, Compression::Zstd, 200)?
        .map_err(|_| "store already initialized")?;
    toa.flush()?;

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
    let dev = Toa::load(&store, false)?;
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

    let dev = Toa::load(&store, false)?;
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

    let dev = Toa::load(&store, false)?;
    todo!("implement Toa::scrub");
}

fn fmt_size_si(n: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    for (i, suffix) in units.into_iter().enumerate().rev() {
        let shift = 1 << (i * 10);
        if n >= shift {
            let n = n as f64 / shift as f64;
            let n = (n * 1e3).round() / 1e3;
            return format!("{n}{suffix}");
        }
    }
    "0B".into()
}

fn parse_size_si(s: &str) -> Option<u64> {
    let (s, mul) = match s.chars().last()? {
        '0'..='9' => (s, 0),
        'K' => (&s[..s.len() - 1], 1),
        'M' => (&s[..s.len() - 1], 2),
        'G' => (&s[..s.len() - 1], 3),
        'T' => (&s[..s.len() - 1], 4),
        'E' => (&s[..s.len() - 1], 5),
        'P' => (&s[..s.len() - 1], 6),
        _ => return None,
    };
    let mul = 1 << (mul * 10);
    let n = s.parse::<u64>().ok()?;
    n.checked_mul(mul)
}

fn start() -> Result<()> {
    let mut args = std::env::args();
    let procname = args.next();
    let procname = procname.as_deref().unwrap_or("toa-cli");
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "init" => cmd_init(procname, args),
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
