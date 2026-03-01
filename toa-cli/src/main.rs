#[cfg(feature = "magic")]
mod magic;
mod unix;

use std::{
    error::Error,
    fs,
    io,
    io::Write,
    ops,
};
use toa::{Hash, ToaStore};

type Result<T> = core::result::Result<T, Box<dyn Error>>;
type InnerToa = toa::Toa<toa::ToaKvStore<toa_kv::sled::Tree>>;

struct Toa {
    inner: InnerToa,
}

#[derive(Default)]
struct Stat {
    size_sum: u64,
}

struct Meta {
    map: toa_kv::sled::Tree,
}

impl Toa {
    fn open(store: &str) -> Result<(Self, Meta)> {
        let db = toa_kv::sled::open(store)
            .map_err(|e| format!("failed to open store {store:?}: {e}"))?;
        let toa = db.open_tree("toa")?;
        let meta = db.open_tree("meta")?;
        let inner = toa::Toa::new(toa::ToaKvStore(toa));
        Ok((Self { inner }, Meta { map: meta }))
    }

    fn get(&self, key: &Hash) -> Result<toa::Object<&InnerToa>> {
        self.inner
            .get(&key)
            .map_err(|e| format!("failed to query store: {e:?}"))?
            .ok_or_else(|| format!("no object with key {key:?}").into())
    }
}

impl ops::Deref for Toa {
    type Target = InnerToa;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Stat {
    fn summarize(self, toa: &Toa) {
        match toa.inner.store().size_on_disk() {
            Err(e) => eprintln!("failed to get on-disk size: {e:?}"),
            Ok(toa_size) => {
                let Self { size_sum } = self;
                let ratio = size_sum as f64 / toa_size as f64;
                println!("pack size: {toa_size}, files size: {size_sum}, ratio: {ratio}");
            }
        }
    }
}

impl Meta {
    fn len(&self) -> usize {
        self.map.len()
    }

    fn get(&self, key: &str) -> Option<impl ops::Deref<Target = [u8]>> {
        self.map.get(key).unwrap()
    }

    fn insert(&self, key: &str, value: &[u8]) {
        self.map.insert(key, value).unwrap();
    }

    fn iter(
        &self,
    ) -> impl Iterator<
        Item = (
            impl ops::Deref<Target = str> + std::fmt::Debug,
            impl ops::Deref<Target = [u8]> + std::fmt::Debug,
        ),
    > {
        self.map
            .iter()
            .map(|x| x.unwrap())
            .map(|(k, v)| (String::from_utf8((*k).into()).unwrap(), v))
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

fn add_files<A>(dev: &Toa, args: A) -> Result<Stat>
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

fn add_file(dev: &Toa, path: &str, stat: &mut Stat) -> Result<Hash> {
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

fn dump_object(dev: &Toa, key: &Hash) -> Result<()> {
    let obj = dev.get(&key)?;
    let mut out = io::stdout().lock();
    let buf = &mut [0; 1 << 13];
    let mut offt = 0;
    loop {
        let n = obj
            .data()
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

fn cmd_new<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;

    let (mut dev, _) =
        Toa::open(&store).map_err(|e| format!("failed to create store builder: {e}"))?;
    let stat = add_files(&mut dev, args)?;

    stat.summarize(&dev);

    Ok(())
}

fn cmd_get<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let key = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let key = toa::Hash::from_bytes(parse_hex(&key)?);
    let (dev, _meta) = Toa::open(&store)?;
    dump_object(&dev, &key)?;

    Ok(())
}

fn cmd_list<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, _meta) = Toa::open(&store)?;
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

    let (_dev, meta) = Toa::open(&store)?;
    let plural = if meta.len() == 1 { "entry" } else { "entries" };
    println!("{} {}", meta.len(), plural);
    meta.iter().for_each(|(k, v)| println!("{k:?}: {v:?}"));

    Ok(())
}

fn cmd_scrub<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, _meta) = Toa::open(&store)?;
    // first collect keys,
    // then sort based on offset to ensure we iterate over all data linearly
    let mut objects = Vec::new();
    eprintln!("collecting keys...");
    dev.iter_with(|key| {
        //let obj = dev.get(&key).unwrap().into_root();
        //objects.push((key, obj));
        objects.push(key);
        false
    })
    .map_err(|e| format!("failure during store iteration: {e:?}"))?;

    eprintln!("sorting keys...");
    // TODO we just lost this capability :(
    //objects.sort_by_key(|x| x.1.offset());

    eprintln!("traversing objects...");
    let mut n_ok @ mut n_fail = 0;
    //objects.into_iter().for_each(|(key, obj)| {
    //let obj = toa::Object::from_root(&*dev, obj);
    objects.into_iter().for_each(|key| {
        let obj = dev.get(&key).unwrap();
        let mut hasher = toa_core::DataHasher::default();
        let mut buf = [0; 8192];
        let mut offt = 0;
        loop {
            let n = obj.data().read(offt, &mut buf).unwrap();
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            offt += n as u128;
        }
        let data = hasher.finalize();
        let hash = toa_core::root_hash(data, toa_core::RefsHasher::default().finalize());
        if key == hash {
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
    let procname = procname.as_deref().unwrap_or("toa-cli");
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
