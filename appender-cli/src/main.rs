#[cfg(feature = "magic")]
mod magic;
mod unix;

use appender::{Hash, cache::MicroLru, worker};
use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    fs::File,
    io,
    io::{Read, Seek, Write},
    ops,
};

const MAGIC: [u8; 16] = *b"Appender\x20\x26\x02\x06\0\0\0\0";

type Result<T> = core::result::Result<T, Box<dyn Error>>;
type Builder = appender::Builder<File, worker::ThreadPool<worker::Work>>;
type InnerReader = appender::Reader<File, MicroLru<Box<[u8]>>>;

struct Reader {
    inner: InnerReader,
}

#[derive(Default)]
struct Stat {
    size_sum: u64,
}

#[derive(Default)]
struct Meta {
    map: BTreeMap<Box<str>, Box<[u8]>>,
}

impl Reader {
    fn new(path: &str) -> Result<(Self, Meta)> {
        let mut dev = fs::OpenOptions::new().read(true).open(path)?;
        let mut buf = [0; 16];
        dev.read_exact(&mut buf)?;
        if buf != MAGIC {
            return Err("bad magic".into());
        }

        let mut buf = [0; 76];
        dev.seek(io::SeekFrom::End(-76))
            .map_err(|e| format!("seek to trailer failed: {e}"))?;
        dev.read_exact(&mut buf)
            .map_err(|e| format!("read of trailer failed: {e}"))?;

        let [a, b, c, d, buf @ ..] = buf;
        let len = u32::from_le_bytes([a, b, c, d]);
        dev.seek(io::SeekFrom::End(-76 - i64::from(len)))
            .map_err(|e| format!("seek to meta table failed: {e}"))?;
        let mut meta = Vec::new();
        (&mut dev)
            .take(u64::from(len))
            .read_to_end(&mut meta)
            .map_err(|e| format!("seek of meta table failed: {e}"))?;

        let meta = parse_meta(&meta).map_err(|e| format!("parsing of meta table failed: {e}"))?;

        let packref = appender::PackRef(buf);
        let inner = InnerReader::new(dev, Default::default(), packref)
            .map_err(|e| format!("failed to initialize reader: {e:?}"))?;
        Ok((Reader { inner }, meta))
    }

    fn get(&self, key: &Hash) -> Result<appender::Object<'_, InnerReader>> {
        self.inner
            .get(&key)
            .map_err(|e| format!("failed to query store: {e:?}"))?
            .ok_or_else(|| format!("no object with key {key:?}").into())
    }
}

impl ops::Deref for Reader {
    type Target = InnerReader;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Stat {
    fn summarize(self, dev: &File) -> Result<()> {
        let pack_size = dev
            .metadata()
            .map_err(|e| format!("failed to get store size: {e:?}"))?
            .len();
        let Self { size_sum } = self;
        let ratio = size_sum as f64 / pack_size as f64;
        println!("pack size: {pack_size}, files size: {size_sum}, ratio: {ratio}");
        Ok(())
    }
}

fn usage(procname: &str) -> Box<dyn Error> {
    let s = format!(
        "\
usage: {procname} <add|get|list>
    new <pack> [files...]
        initialize a new pack
    get <pack> <key>
        dump object data to stdout (may contain raw bytes!)
    list <pack>
        list all known objects
    meta <pack>
        list meta table
    scrub <pack>
        verify pack integrity
    unix new <pack> <directory>
    unix get <pack> <path>
    unix ls <pack> [path]
    unix scrub <pack>
        verify directory tree integrity
        this does not check file contents, only whether they're present"
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

fn new_builder(store: &str) -> Result<Builder> {
    let mut dev = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(store)?;
    dev.write_all(&MAGIC)?;
    let dev = Builder::new(dev, Default::default(), rand::thread_rng());
    Ok(dev)
}

fn new_reader(store: &str) -> Result<(Reader, Meta)> {
    Reader::new(store).map_err(|e| format!("failed to open store {store:?}: {e}").into())
}

fn parse_meta(mut buf: &[u8]) -> Result<Meta> {
    let mut meta = Meta::default();
    while let [key_len, b @ ..] = buf {
        let (key, b) = b.split_at(usize::from(*key_len));
        let [x, y, b @ ..] = b else { todo!() };
        let value_len = u16::from_le_bytes([*x, *y]);
        let (value, b) = b.split_at(usize::from(value_len));
        buf = b;
        let key = core::str::from_utf8(key).expect("key is not UTF-8");
        let prev = meta.map.insert(key.into(), value.into());
        assert!(prev.is_none(), "duplicate key {key:?}");
    }
    Ok(meta)
}

fn add_files<A>(dev: &mut Builder, args: A) -> Result<Stat>
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

fn add_file(dev: &mut Builder, path: &str, stat: &mut Stat) -> Result<Hash> {
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
        .add(&data, &[])
        .map_err(|e| format!("failed to add {path:?} to store: {e:?}"))?;
    Ok(key)
}

fn finish(dev: Builder, meta: Meta) -> Result<fs::File> {
    let (mut dev, packref) = dev
        .finish()
        .map_err(|e| format!("failed to finalize store builder: {e:?}"))?;
    let packref = packref.ok_or("no objects added to store")?;
    let mut meta_size = 0;
    for (k, v) in meta.map.iter() {
        let kl = u8::try_from(k.len()).map_err(|_| "meta key too large")?;
        dev.write_all(&kl.to_le_bytes())?;
        dev.write_all(k.as_bytes())?;
        let vl = u16::try_from(v.len()).map_err(|_| "meta value too large")?;
        dev.write_all(&vl.to_le_bytes())?;
        dev.write_all(v)?;
        meta_size += 1 + k.len() + 2 + v.len();
    }
    let meta_size = u32::try_from(meta_size).map_err(|_| "meta table too large")?;
    dev.write_all(&meta_size.to_le_bytes())
        .and_then(|()| dev.write_all(&packref.0))
        .map_err(|e| format!("failed to append trailer: {e}"))?;
    dev.sync_all()
        .map_err(|e| format!("failed to synchronize data: {e}"))?;
    Ok(dev)
}

fn dump_object(dev: &Reader, key: &Hash) -> Result<()> {
    let obj = dev.get(&key)?;
    let mut out = io::stdout().lock();
    let e = |e| format!("failed to read object: {e:?}");
    for data in obj.read_exact(0, usize::MAX).map_err(e)? {
        let data = data.map_err(e)?;
        out.write_all(&data)
            .map_err(|e| format!("failed to write to stdout: {e:?}"))?;
    }
    Ok(())
}

fn cmd_new<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;

    let mut dev =
        new_builder(&store).map_err(|e| format!("failed to create store builder: {e}"))?;
    let stat = add_files(&mut dev, args)?;
    let dev = finish(dev, Meta::default())
        .map_err(|e| format!("failed to finalize store builder: {e:?}"))?;

    stat.summarize(&dev)?;

    Ok(())
}

fn cmd_get<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let key = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let key = appender::Hash(parse_hex(&key)?);
    let (dev, _meta) = new_reader(&store)?;
    dump_object(&dev, &key)?;

    Ok(())
}

fn cmd_list<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, _meta) = new_reader(&store)?;
    dev.iter_with(|key| {
        println!("{key:?}");
        false
    })
    .map_err(|e| format!("failure during store iteration: {e:?}"))?;

    Ok(())
}

fn cmd_meta<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (_dev, meta) = new_reader(&store)?;
    let plural = if meta.map.len() == 1 {
        "entry"
    } else {
        "entries"
    };
    println!("{} {}", meta.map.len(), plural);
    meta.map.iter().for_each(|(k, v)| println!("{k:?}: {v:?}"));

    Ok(())
}

fn cmd_scrub<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, _meta) = new_reader(&store)?;
    // first collect keys,
    // then sort based on offset to ensure we iterate over all data linearly
    let mut objects = Vec::new();
    eprintln!("collecting keys...");
    dev.iter_with(|key| {
        let obj = dev.get(&key).unwrap().to_raw();
        objects.push((key, obj));
        false
    })
    .map_err(|e| format!("failure during store iteration: {e:?}"))?;

    eprintln!("sorting keys...");
    objects.sort_by_key(|x| x.1.offset());

    eprintln!("traversing objects...");
    let mut n_ok @ mut n_fail = 0;
    objects.into_iter().for_each(|(key, obj)| {
        let obj = appender::Object::from_raw(obj, &*dev);
        let mut hasher = toa_core::DataHasher::default();
        for x in obj.read_exact(0, usize::MAX).unwrap() {
            let x = x.unwrap();
            hasher.update(&x);
        }
        let hash = hasher.finalize();
        if &key.0 == hash.as_bytes() {
            n_ok += 1;
        } else {
            println!("fail ({key:?} != {hash})");
            n_fail += 1;
        }
    });

    println!("ok:{n_ok}, fail:{n_fail}");

    (n_fail == 0)
        .then_some(())
        .ok_or_else(|| "some objects are corrupt".into())
}

fn start() -> Result<()> {
    let mut args = std::env::args();
    let procname = args.next();
    let procname = procname.as_deref().unwrap_or("appender-cli");
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "new" => cmd_new(procname, args),
        "get" => cmd_get(procname, args),
        "list" => cmd_list(procname, args),
        "meta" => cmd_meta(procname, args),
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
