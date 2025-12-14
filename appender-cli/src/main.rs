use appender::{SnapshotRoot, device::io::IoDevice};
use rand::{CryptoRng, RngCore};
use std::{fs, fs::File, io};

struct Meta {
    store: String,
    key: appender::Key,
    pitch: u8,
    root: SnapshotRoot,
}

// TODO avoid Box
type Appender = appender::Appender<IoDevice<File, Box<dyn Fn(&mut File) -> io::Result<()>>>>;

fn usage(procname: &str) -> i32 {
    eprint!(
        "\
usage: {procname} <add|get|list>
    init <meta> <store> <pitch> [files...]
        initialize a new store
    add  <meta> [files...]
        read data from files and store as objects
    get  <meta> <key>
        dump object data to stdout (may contain raw bytes!)
    list <meta>
        list all known objects
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

fn read_meta(path: &str) -> io::Result<Meta> {
    let meta = fs::read_to_string(path)?;

    let mut store = None;
    let mut key = None;
    let mut pitch = None;
    let mut root = None;

    for line in meta.lines() {
        let (k, v) = line.split_once(" ").unwrap();
        let [k, v] = [k, v].map(|x| x.trim());
        let value = match k {
            "store" => &mut store,
            "key" => &mut key,
            "pitch" => &mut pitch,
            "root" => &mut root,
            x if x.starts_with("#") => continue,
            x => todo!("bad key {x:?}"),
        };
        // if only we had unwrap_none...
        let prev = value.replace(v);
        assert!(prev.is_none(), "duplicate definition of {key:?}");
    }

    let store = store.expect("no \"store\" defined");
    let key = key.expect("no \"key\" defined");
    let pitch = pitch.expect("no \"pitch\" defined");
    let root = root.expect("no \"root\" defined");

    Ok(Meta {
        store: store.into(),
        key: parse_hex::<32>(&key).unwrap().into(),
        pitch: pitch.parse().unwrap(),
        root: root.parse().map(SnapshotRoot).unwrap(),
    })
}

fn open(meta_path: &str, write: bool) -> io::Result<(Appender, Meta)> {
    let meta = read_meta(meta_path)?;
    let rng = &mut rand::thread_rng();
    let sync = Box::new(|x: &mut File| x.sync_all()) as Box<dyn Fn(&mut _) -> _>;
    let dev = fs::OpenOptions::new()
        .read(true)
        .write(write)
        .open(&meta.store)?;
    let len = dev.metadata()?.len();
    let dev = IoDevice::new(dev, len, sync);
    let dev = Appender::mount(dev, meta.key, meta.root, meta.pitch).unwrap();
    Ok((dev, meta))
}

fn add_files<A, R>(dev: &mut Appender, mut args: A, rng: &mut R) -> Result<(), i32>
where
    A: Iterator<Item = String>,
    R: CryptoRng + RngCore,
{
    for file in args {
        // TODO don't read huge files in one go
        let data = fs::read(&file).unwrap();
        let key = dev.add(rng, &data).unwrap();
        println!("{key:?} {file}");
    }
    Ok(())
}

fn cmd_init<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let meta = args.next().ok_or_else(|| usage(procname))?;
    let store = args.next().ok_or_else(|| usage(procname))?;
    let pitch = args.next().ok_or_else(|| usage(procname))?;

    let pitch = pitch.parse::<u8>().unwrap();

    let mut meta = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(meta)
        .unwrap();

    let rng = &mut rand::thread_rng();
    let sync = Box::new(|x: &mut File| x.sync_all()) as Box<dyn Fn(&mut _) -> _>;
    let dev = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(true)
        .create(true)
        .open(&store)
        .unwrap();
    let dev = IoDevice::new(dev, 0, sync);
    let mut dev = Appender::init(dev, pitch, &mut *rng);

    add_files(&mut dev, args, rng)?;

    let SnapshotRoot(root) = dev.commit(rng).unwrap().unwrap();
    let key = dev.key();

    let cfg = format!(
        "\
store {store}
key   {key:064x}
pitch {pitch}
root  {root}
"
    );
    use io::Write;
    meta.write_all(cfg.as_bytes()).unwrap();

    Ok(())
}

fn cmd_add<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let meta_n = args.next().ok_or_else(|| usage(procname))?;

    let new_meta_n = meta_n.clone() + "~";
    let mut new_meta = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&new_meta_n)
        .unwrap();

    let (mut dev, meta) = open(&meta_n, true).unwrap();
    let rng = &mut rand::thread_rng();
    add_files(&mut dev, args, rng)?;

    let Meta {
        store, key, pitch, ..
    } = meta;
    let root = dev.commit(rng).unwrap().unwrap().0;

    let cfg = format!(
        "\
store {store}
key   {key:064x}
pitch {pitch}
root  {root}
"
    );
    use io::Write;
    new_meta.write_all(cfg.as_bytes()).unwrap();

    fs::rename(new_meta_n, meta_n).unwrap();

    Ok(())
}

fn cmd_get<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let meta = args.next().ok_or_else(|| usage(procname))?;
    let key = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let key = appender::Hash(parse_hex::<32>(&key).unwrap());
    let (mut dev, _) = open(&meta, false).unwrap();

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
    let meta = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (mut dev, _) = open(&meta, false).unwrap();

    dev.iter_with(|key| {
        println!("{key:?}");
        false
    });

    Ok(())
}

fn start() -> Result<(), i32> {
    let mut args = std::env::args();
    let procname = args.next();
    let procname = procname.as_deref().unwrap_or("appender-cli");
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "init" => cmd_init(procname, args),
        "add" => cmd_add(procname, args),
        "get" => cmd_get(procname, args),
        "list" => cmd_list(procname, args),
        _ => Err(usage(procname)),
    }
}

fn main() -> ! {
    std::process::exit(start().map_or_else(|x| x, |()| 0))
}
