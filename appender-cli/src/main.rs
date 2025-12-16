use std::{
    fs,
    fs::File,
    io,
    io::{Read, Seek, Write},
};

type Builder = appender::Builder<File>;
type Reader = appender::Reader<File, appender::cache::MicroLru<Box<[u8]>>>;

#[derive(Default)]
struct Stat {
    size_sum: u64,
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
environment:
    APPENDER_CLI_KEY=<64 hex bytes>
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
    let dev = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(store)?;
    let dev = Builder::new(dev, rand::thread_rng());
    Ok(dev)
}

fn new_reader(store: &str) -> io::Result<Reader> {
    let key = std::env::var("APPENDER_CLI_KEY").unwrap();
    let key = parse_hex::<32>(&key).unwrap().into();
    let mut dev = fs::OpenOptions::new().read(true).open(store)?;
    dev.seek(io::SeekFrom::End(-64)).unwrap();
    let mut packref = appender::PackRef([0; 64]);
    dev.read_exact(&mut packref.0).unwrap();
    let dev = Reader::new(dev, Default::default(), key, packref).unwrap();
    Ok(dev)
}

fn add_files<A>(dev: &mut Builder, args: A) -> Result<Stat, i32>
where
    A: Iterator<Item = String>,
{
    let mut stat = Stat::default();
    for file in args {
        // TODO don't read huge files in one go
        let data = fs::read(&file).unwrap();
        let key = dev.add(&data).unwrap();
        println!("{key:?} {file}");
        stat.size_sum += u64::try_from(data.len()).unwrap();
    }
    Ok(stat)
}

fn cmd_new<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;

    let mut dev = new_builder(&store).unwrap();
    let stat = add_files(&mut dev, args)?;
    let (mut dev, key, packref) = dev.finish().unwrap();
    let packref = packref.unwrap();
    dev.write_all(&packref.0).unwrap();

    println!("APPENDER_CLI_KEY={key:064x}");

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
    let dev = new_reader(&store).unwrap();
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

    let dev = new_reader(&store).unwrap();
    dev.iter_with(|key| {
        println!("{key:?}");
        false
    })
    .unwrap();

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
        _ => Err(usage(procname)),
    }
}

fn main() -> ! {
    std::process::exit(start().map_or_else(|x| x, |()| 0))
}
