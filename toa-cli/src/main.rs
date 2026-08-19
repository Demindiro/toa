mod blob;
mod unix;

use std::{
    collections::BTreeMap,
    error::Error,
    fs, io,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use toa::{Compression, Hash, PageSize};
use toa_blob::{BlobStore, FileBlocks};

const TYPE_ID_DIR: [u8; 16] = [0; 16];
const VERSION: &str = env!("CARGO_PKG_VERSION");
const REVISION: &str = env!("GIT_HASH");

type Result<T> = core::result::Result<T, Box<dyn Error>>;
type Store = BlobStore<FileBlocks>;
type Accel = toa::accel::sled::Db;
type InnerToa = toa::Toa<Store, Accel>;
type Object<'a> = toa::Object<'a, Store, Accel>;

// FIXME bro
struct ToaToa {
    inner: InnerToa,
}

struct Toa {
    toa: ToaToa,
    meta: BTreeMap<Box<str>, Hash>,
}

struct Stat {
    original_disk_size: u64,
    size_sum: u64,
    dropped: u64,
    skipped: u64,
}

impl ToaToa {
    fn load(path: &Path, accel: &Path, write: bool) -> Result<Self> {
        let store = load_store(path, write)?;
        let accel = toa::accel::sled::open(accel)
            .map_err(|e| format!("failed to open accelerator: {e:?}"))?;
        let inner = toa::Toa::load(store, accel)?.ok_or("no store initialized")?;
        Ok(Self { inner })
    }

    fn get(&self, key: &Hash) -> Result<Object<'_>> {
        self.inner
            .get(&key)
            .map_err(|e| format!("failed to query store: {e:?}"))?
            .ok_or_else(|| format!("no object with key {key:?}").into())
    }

    fn add_dir<'a, I>(&mut self, items: I) -> Result<Hash>
    where
        I: DoubleEndedIterator<Item = (&'a str, Hash)> + Clone,
    {
        let e = |e| format!("failed to add dir: {e:?}");
        let mut hash = Hash::NIL;
        for (_, x) in items.clone().rev() {
            hash = self.inner.add_refs(x, hash)?;
        }
        let mut dir = Vec::new();
        dir.extend(TYPE_ID_DIR);
        for (name, _) in items {
            dir.push(name.len() as u8);
            dir.extend(name.bytes());
        }
        let dir = self.inner.add_data(&dir).map_err(e)?;
        hash = self.inner.add_refs(dir, hash)?;
        Ok(hash)
    }

    fn iter_dir(
        &self,
        obj: [Hash; 2],
    ) -> Result<impl ExactSizeIterator<Item = Result<(String, Hash)>>> {
        self.dir_to_btree(obj)
            .map(|x| x.into_iter().map(|(k, v)| Ok((k.into(), v))))
    }

    fn dir_to_btree(&self, [data, refs]: [Hash; 2]) -> Result<BTreeMap<Box<str>, Hash>> {
        let mut map = BTreeMap::default();
        let Ok(Some(data)) = self.inner.get(&data) else {
            todo!()
        };
        let Object::Data(data) = data else { todo!() };
        let type_id = data.read_array::<16>(0).unwrap();
        assert_eq!(type_id, TYPE_ID_DIR);
        let data = {
            let mut b = vec![0; data.len()? as usize - 16];
            data.read_exact(16, &mut b)
                .map_err(|e| format!("root: failed to read directory data: {e:?}"))?;
            b
        };
        let mut offset = 0;
        let mut next = refs;
        while next != Hash::NIL {
            let kl = usize::from(data[offset]);
            offset += 1;
            let k = &data[offset..][..kl];
            let k = core::str::from_utf8(k).unwrap();
            offset += kl;
            let v;
            [v, next] = self
                .inner
                .get(&next)
                .map_err(|e| format!("root: failed to read directory ref: {e:?}"))?
                .unwrap()
                .into_refs()
                .unwrap();
            map.insert(k.into(), v);
        }

        Ok(map)
    }
}

impl Toa {
    fn load(path: &Path, accel: &Path, write: bool) -> Result<Self> {
        let toa = ToaToa::load(path, accel, write)?;
        let root = toa.inner.root();
        let root = (root != Hash::default())
            .then(|| toa.get(&root))
            .transpose()?;
        let meta = if let Some(Object::Refs(root)) = root {
            toa.dir_to_btree(root)?
        } else if let Some(_) = root {
            eprintln!("warning: meta/root is not a refs object");
            Default::default()
        } else {
            Default::default()
        };
        Ok(Self { toa, meta })
    }

    fn root(&self) -> Hash {
        self.toa.inner.root()
    }

    fn save_root(&mut self) -> Result<()> {
        let root = self.toa.add_dir(self.meta.iter().map(|x| (&**x.0, *x.1)));
        self.toa
            .inner
            .set_root(root?)
            .map_err(|e| format!("failed to set root: {e:?}"))?;
        Ok(())
    }

    fn set_meta(&mut self, name: &str, value: &Hash) {
        self.meta.insert(name.into(), *value);
    }

    fn flush(&mut self) -> io::Result<()> {
        self.toa.inner.flush()
    }
}

impl Stat {
    fn new(toa: &Toa) -> Result<Self> {
        Ok(Self {
            original_disk_size: toa.toa.inner.size_on_disk()?,
            size_sum: 0,
            dropped: 0,
            skipped: 0,
        })
    }

    fn summarize(self, toa: &Toa) {
        let Self {
            original_disk_size,
            size_sum,
            dropped,
            skipped,
        } = self;
        let toa_size = toa.toa.inner.size_on_disk().unwrap();
        let added = toa_size - original_disk_size;
        let ratio = size_sum as f64 / added as f64;
        let f = |s, x| println!("{s}: {x} ({})", fmt_size_iec(x));
        f("store size", toa_size);
        f("added", added);
        f("files size", size_sum);
        println!("ratio: {ratio}");
        println!("dropped: {dropped}");
        println!("skipped: {skipped}");
    }
}

fn usage(procname: &str) -> Box<dyn Error> {
    let s = format!(
        "toa-cli, version {VERSION}, revision {REVISION}
usage: {procname} <cmd> [...]
    init <store>
        initialize a store
    get <store> <accel> <key>
        dump object data to stdout (may contain raw bytes!)
    scrub <store> <accel>
        verify store integrity
    blob ls <store> [--iec]
        list all blobs
        --iec     print sizes in IEC units.
    blob debug log <store>
        dump log
    unix add <store> <accel> <name> <directory> [-e <skip>]
    unix get <store> <accel> <path>
    unix ls <store> <accel> [path]"
    );
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

fn dump_object(dev: &Toa, key: &Hash) -> Result<()> {
    let obj = dev.toa.get(&key)?;
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
    eprintln!("using {} blocks", fmt_size_iec(block_size.into()));

    // len() doesn't work because Linux reports 0 for block devices
    //
    // "but but this makes sense!!" no it fucking doesn't
    use io::Seek;
    let mut len = (&dev).seek(io::SeekFrom::End(0))?;
    (&dev).seek(io::SeekFrom::Start(0))?;
    if len == 0 {
        eprintln!("file appears to be empty");
        eprint!("Please enter the desired file size (suffixes: K, M, G, T, P, E): ");
        let n = std::io::stdin().lines().next().transpose()?.unwrap();
        len = parse_size_iec(&n).ok_or("invalid size")?;
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
        fmt_size_iec(zone_size.into())
    );

    let zone_count = u32::try_from(len / zone_size).unwrap();
    eprintln!("{zone_count} zones");

    eprintln!(
        "{} of slack at end of file",
        fmt_size_iec(len - u64::from(zone_count) * zone_size)
    );

    let dev = toa_blob::FileBlocks::wrap(block_size, zone_blocks, zone_count, dev);

    // TODO do some benchmarks to find a good default
    // given the lack of in-place writes, a size larger than ZFS's default 128K likely makes sense.
    let page_size = PageSize::M4;
    eprintln!("using {page_size} page size");

    let store = BlobStore::init(dev)?;
    let accel = BTreeMap::default();
    let mut toa = toa::Toa::init(store, accel, page_size, Compression::Zstd, 200)?
        .map_err(|_| "store already initialized")?;
    toa.flush()?;

    Ok(())
}

fn cmd_get<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let [store, accel] = arg_store_accel(procname, &mut args)?;
    let key = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let key = toa::Hash::from_bytes(parse_hex(&key)?);
    let dev = Toa::load(&store, &accel, false)?;
    dump_object(&dev, &key)?;

    Ok(())
}

fn cmd_scrub<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let [store, accel] = arg_store_accel(procname, &mut args)?;
    args_end(procname, args)?;

    let dev = Toa::load(&store, &accel, false)?;
    todo!("implement Toa::scrub");
}

fn fmt_size_iec(n: u64) -> String {
    fmt_size_iec_opt(n, 3, false, 1)
}

fn fmt_size_iec_short(n: u64) -> String {
    fmt_size_iec_opt(n, 0, true, 10)
}

fn fmt_size_iec_opt(n: u64, round_digits: u8, short_units: bool, cutoff: u64) -> String {
    let round = 10f64.powi(round_digits.into());
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    for (i, suffix) in units.into_iter().enumerate().rev() {
        let shift = 1 << (i * 10);
        if n >= (cutoff * shift) {
            let n = n as f64 / shift as f64;
            let n = (n * round).round() / round;
            let suffix = if short_units { &suffix[..1] } else { suffix };
            return format!("{n}{suffix}");
        }
    }
    "0B".into()
}

fn parse_size_iec(s: &str) -> Option<u64> {
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

fn load_dev(path: &Path, write: bool) -> Result<FileBlocks> {
    let mut hdr = [0; 32];
    let dev = fs::OpenOptions::new().read(true).write(write).open(path)?;
    (&dev).read_exact(&mut hdr)?;
    let hdr = toa_blob::snoop_header(hdr).unwrap();
    let blk = match hdr.block_size {
        512 => toa_blob::BlockShift::N9,
        4096 => toa_blob::BlockShift::N12,
        x => todo!("block size {x}"),
    };
    Ok(FileBlocks::wrap(blk, hdr.zone_blocks, hdr.zone_count, dev))
}

fn load_store(path: &Path, write: bool) -> Result<Store> {
    let dev = load_dev(path, write)?;
    let store = Store::load(dev)?;
    Ok(store)
}

fn arg_store_accel<A>(procname: &str, mut args: A) -> Result<[PathBuf; 2]>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let accel = args.next().ok_or_else(|| usage(procname))?;
    Ok([store, accel].map(PathBuf::from))
}

fn start() -> Result<()> {
    let mut args = std::env::args();
    let procname = args.next();
    let procname = procname.as_deref().unwrap_or("toa-cli");
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "init" => cmd_init(procname, args),
        "get" => cmd_get(procname, args),
        "scrub" => cmd_scrub(procname, args),
        "unix" => unix::cmd(procname, args),
        "blob" => blob::cmd(procname, args),
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
