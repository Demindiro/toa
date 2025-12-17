mod unix;

use appender::Hash;
use std::{
    collections::BTreeMap,
    fs,
    fs::File,
    io,
    io::{Read, Seek, Write},
};

const MAGIC: [u8; 16] = *b"Plainey Appender";

type Builder = appender::Builder<File>;
type Reader = appender::Reader<File, appender::cache::MicroLru<Box<[u8]>>>;

#[derive(Default)]
struct Stat {
    size_sum: u64,
}

#[derive(Default)]
struct Meta {
    map: BTreeMap<Box<str>, Box<[u8]>>,
}

fn usage(procname: &str) -> i32 {
    eprint!(
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
    unix new <pack> <directory>
    unix get <pack> <path>
    unix ls <pack> [path]
"
    );
    1
}

fn args_end<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    args.next()
        .is_none()
        .then_some(())
        .ok_or_else(|| usage(procname))
}

fn parse_hex<const N: usize>(key: &str) -> Result<[u8; N], &'static str> {
    if key.len() != const { N * 2 } {
        return Err("key doesn't have expected length");
    }
    let mut k = [0; N];
    for (xy, w) in key.as_bytes().chunks_exact(2).zip(k.iter_mut()) {
        let &[x, y] = xy.try_into().unwrap();
        let f = |x| match x {
            b'0'..=b'9' => x - b'0',
            b'a'..=b'f' => x - b'a' + 10,
            b'A'..=b'F' => x - b'A' + 10,
            c => todo!("invalid hex char {:?}", c as char),
        };
        *w = f(x) << 4 | f(y);
    }
    Ok(k)
}

fn new_builder(store: &str) -> io::Result<Builder> {
    let mut dev = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(store)?;
    dev.write_all(&MAGIC)?;
    let dev = Builder::new(dev, rand::thread_rng());
    Ok(dev)
}

fn new_reader(store: &str) -> io::Result<(Reader, Meta)> {
    let mut dev = fs::OpenOptions::new().read(true).open(store)?;
    let mut buf = [0; 16];
    dev.read_exact(&mut buf)?;
    if buf != MAGIC {
        todo!("bad magic");
    }

    let mut buf = [0; 76];
    dev.seek(io::SeekFrom::End(-76)).unwrap();
    dev.read_exact(&mut buf).unwrap();

    let [a, b, c, d, buf @ ..] = buf;
    let len = u32::from_le_bytes([a, b, c, d]);
    dev.seek(io::SeekFrom::End(-76 - i64::from(len))).unwrap();
    let mut meta = Vec::new();
    (&mut dev).take(u64::from(len)).read_to_end(&mut meta)?;

    let meta = parse_meta(&meta).unwrap();

    let packref = appender::PackRef(buf);
    let dev = Reader::new(dev, Default::default(), packref).unwrap();
    Ok((dev, meta))
}

fn parse_meta(mut buf: &[u8]) -> Result<Meta, i32> {
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

fn add_files<A>(dev: &mut Builder, args: A) -> Result<Stat, i32>
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

fn add_file(dev: &mut Builder, path: &str, stat: &mut Stat) -> Result<Hash, i32> {
    let data = fs::OpenOptions::new().read(true).open(path).unwrap();
    // SAFETY: other processes cannot modify CoW mappings
    let data = unsafe {
        memmap2::MmapOptions::new()
            .map_copy_read_only(&data)
            .unwrap()
    };
    stat.size_sum += u64::try_from(data.len()).unwrap();
    Ok(dev.add(&data).unwrap())
}

fn finish(dev: Builder, meta: Meta) -> io::Result<fs::File> {
    let (mut dev, packref) = dev.finish().unwrap();
    let packref = packref.unwrap();
    let mut meta_size = 0;
    for (k, v) in meta.map.iter() {
        let kl = u8::try_from(k.len()).expect("meta key too large");
        dev.write_all(&kl.to_le_bytes())?;
        dev.write_all(k.as_bytes())?;
        let vl = u16::try_from(v.len()).expect("meta value too large");
        dev.write_all(&vl.to_le_bytes())?;
        dev.write_all(v)?;
        meta_size += 1 + k.len() + 2 + v.len();
    }
    let meta_size = u32::try_from(meta_size).expect("meta table too large");
    dev.write_all(&meta_size.to_le_bytes())?;
    dev.write_all(&packref.0)?;
    dev.sync_all()?;
    Ok(dev)
}

fn cmd_new<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;

    let mut dev = new_builder(&store).unwrap();
    let stat = add_files(&mut dev, args)?;
    let dev = finish(dev, Meta::default()).unwrap();

    let pack_size = dev.metadata().unwrap().len();
    let Stat { size_sum } = stat;
    let ratio = size_sum as f64 / pack_size as f64;
    println!("pack size: {pack_size}, files size: {size_sum}, ratio: {ratio}");

    Ok(())
}

fn cmd_get<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let key = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let key = appender::Hash(parse_hex(&key).unwrap());
    let (dev, _meta) = new_reader(&store).unwrap();
    match dev.get(&key).unwrap() {
        None => {
            eprintln!("no object with key {key:?}");
            Err(1)
        }
        Some(mut obj) => {
            let mut out = io::stdout().lock();
            for data in obj.read_exact(0, usize::MAX).unwrap() {
                let data = data.unwrap();
                use io::Write;
                out.write_all(&data).unwrap();
            }
            Ok(())
        }
    }
}

fn cmd_list<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, _meta) = new_reader(&store).unwrap();
    dev.iter_with(|key| {
        println!("{key:?}");
        false
    })
    .unwrap();

    Ok(())
}

fn cmd_meta<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (_dev, meta) = new_reader(&store).unwrap();
    let plural = if meta.map.len() == 1 {
        "entry"
    } else {
        "entries"
    };
    println!("{} {}", meta.map.len(), plural);
    meta.map.iter().for_each(|(k, v)| println!("{k:?}: {v:?}"));

    Ok(())
}

fn start() -> Result<(), i32> {
    let mut args = std::env::args();
    let procname = args.next();
    let procname = procname.as_deref().unwrap_or("appender-cli");
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "new" => cmd_new(procname, args),
        "get" => cmd_get(procname, args),
        "list" => cmd_list(procname, args),
        "meta" => cmd_meta(procname, args),
        "unix" => unix::cmd(procname, args),
        _ => Err(usage(procname)),
    }
}

fn main() -> ! {
    std::process::exit(start().map_or_else(|x| x, |()| 0))
}
