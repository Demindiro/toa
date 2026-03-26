use super::Object;
use crate::{InnerToa, Result, Stat, Toa, args_end, usage};
use chrono::prelude::*;
use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use toa::Hash;
use toa_unix::{DirItem, DirItemType};

type Dir<'a> = toa_unix::Dir<'a, super::Store>;
type Sink = toa::Sink<Result<Action>>;

#[derive(Default)]
struct DirEntry {
    type_perms: u16,
    name: Box<str>,
    uid: u32,
    gid: u32,
    modified: i64,
}

struct DirBuilder {
    parent: u64,
    entries: Vec<DirEntry>,
    /// # Note
    ///
    /// refs[0] is reserved for this directory
    refs: Vec<Hash>,
    count: u64,
    entry: DirEntry,
}

enum Action {
    DirAdd {
        dir: u64,
        entry: DirEntry,
    },
    DirEnd {
        dir: u64,
        count: u64,
        parent: u64,
        entry: DirEntry,
    },
    DirEndData {
        parent: u64,
        entry: DirEntry,
        refs: Vec<Hash>,
    },
}

pub fn cmd<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "new" => cmd_new(procname, args),
        "get" => cmd_get(procname, args),
        "ls" => cmd_ls(procname, args),
        _ => Err(usage(procname)),
    }
}

fn cmd_new<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let root = args.next().ok_or_else(|| usage(procname))?;
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

    // default to 256MiB zone size
    // https://146a55aca6f00848c565-a7635525d40ac1c70300198708936b4e.ssl.cf1.rackcdn.com/images/133059501b4dfbcabffde7b8d0e3427481af62f1.pdf
    // > Initial de facto zone size chosen was 256MiB for all zones.
    // It works out to about 30k zones for a 8TB drive and ~2.5s for full zone copies. Seems reasonable?
    let zone_size = 1 << 28;
    let zone_blocks = u32::try_from(zone_size / u64::from(block_size)).unwrap();
    eprintln!(
        "using {} zones ({zone_blocks} blocks)",
        fmt_size_si(zone_size.into())
    );

    let mut len = meta.len();
    if len == 0 {
        eprintln!("file appears to be empty");
        eprint!("Please enter the desired file size (suffixes: K, M, G, T, P, E): ");
        let n = std::io::stdin().lines().next().transpose()?.unwrap();
        len = parse_size_si(&n).ok_or("invalid size")?;
        dev.set_len(len)?;
    }
    let zone_count = u32::try_from(len / zone_size).unwrap();
    eprintln!("{zone_count} zones");

    eprintln!(
        "{} of slack at end of file",
        fmt_size_si(len - u64::from(zone_count) * zone_size)
    );

    let dev = toa_blob::FileBlocks::wrap(block_size, zone_blocks, zone_count, dev);

    let mut dev = Toa::init(dev)?;
    let mut stat = Stat::default();
    println!("d {root}");
    let root_key = std::thread::scope(|scope| -> Result<Hash> {
        let (mut cmd, res) = dev.dataflow(scope, 128);
        let stub_entry = DirEntry {
            type_perms: 0,
            name: Default::default(),
            uid: meta.uid(),
            gid: meta.gid(),
            modified: 0,
        };
        let stat = &mut stat;
        let mut cmd2 = cmd.clone();

        const ROOT_PARENT: u64 = u64::MAX;

        scope.spawn(move || add_dir(&mut cmd2, &root, stat, ROOT_PARENT, stub_entry));

        use std::collections::hash_map::HashMap;

        let mut dirs = HashMap::<u64, DirBuilder>::default();
        for (res, action) in res {
            let res = res?;
            match action? {
                Action::DirAdd { dir, entry } => {
                    let toa::ResultValue::Key(key) = res else {
                        unreachable!("expect key for DirAdd")
                    };
                    if dir == ROOT_PARENT {
                        return Ok(key);
                    }
                    let x = dirs.entry(dir).or_default();
                    x.entries.push(entry);
                    x.refs.push(key);
                    if x.count == x.entries.len() as u64 {
                        dirs.remove(&dir).expect("exists").finalize(&mut cmd);
                    }
                }
                Action::DirEnd {
                    dir,
                    count,
                    parent,
                    entry,
                } => {
                    let x = dirs.entry(dir).or_default();
                    x.parent = parent;
                    x.entry = entry;
                    x.count = count;
                    if x.count == x.entries.len() as u64 {
                        dirs.remove(&dir).expect("exists").finalize(&mut cmd);
                    }
                }
                Action::DirEndData {
                    parent,
                    entry,
                    mut refs,
                } => {
                    let toa::ResultValue::Key(key) = res else {
                        unreachable!("expect key for DirEndData")
                    };
                    refs[0] = key;
                    cmd.add_refs(Ok(Action::DirAdd { dir: parent, entry }), refs)
                        .unwrap_or_else(|_| unreachable!());
                }
            }
        }

        unreachable!("no root key")
    })?;
    dev.set_meta("unix.root", &root_key);
    dev.save_root()?;

    dev.flush()?;

    stat.summarize(&dev);

    Ok(())
}

fn cmd_get<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let path = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let store = PathBuf::from(store);

    let (dev, dir) = open(&store, false)?;
    let file = traverse_path(&dev, &path, dir)?;
    crate::dump_object(&dev, &file)?;

    Ok(())
}

fn cmd_ls<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let path = args.next();
    let path = path.as_deref().unwrap_or("/");
    args_end(procname, args)?;

    let store = PathBuf::from(store);

    let (dev, dir) = open(&store, false)?;
    let dir = traverse_path(&dev, path, dir)?;
    let dir = Dir::new(&dev, &dir)?;
    println!("items: {}", dir.len());
    for x in dir.iter() {
        let (i, x) = x.map_err(|e| format!("{e:?}"))?;
        let key = dir.get_ref(i).map_err(|e| format!("{e:?}"))?.unwrap();
        let fmt = fmt_item(&dev, &dir, &x, &key)?;
        println!("{key}  {fmt}");
    }

    Ok(())
}

fn add_file(
    cmd: &mut Sink,
    path: &str,
    stat: &mut Stat,
    parent: u64,
    entry: DirEntry,
) -> Result<()> {
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
    cmd.add_data(Ok(Action::DirAdd { dir: parent, entry }), data)
        .unwrap_or_else(|_| unreachable!());
    Ok(())
}

fn add_sym(
    cmd: &mut Sink,
    path: &str,
    stat: &mut Stat,
    parent: u64,
    entry: DirEntry,
) -> Result<()> {
    let link =
        fs::read_link(path).map_err(|e| format!("failed to read target of {path:?}: {e}"))?;
    let link = path_to_utf8(link)?;
    stat.size_sum += u64::try_from(link.len()).expect("usize <= u64");
    cmd.add_data(Ok(Action::DirAdd { dir: parent, entry }), link.into_bytes())
        .unwrap_or_else(|_| unreachable!());
    Ok(())
}

fn add_dir(
    cmd: &mut Sink,
    path: &str,
    stat: &mut Stat,
    parent: u64,
    entry: DirEntry,
) -> Result<()> {
    let e = |e| format!("failed to traverse {path:?}: {e}").into();

    let id = stat.num_directories;
    stat.num_directories += 1;

    let dir = fs::read_dir(path).map_err(e)?;

    let mut count = 0;
    for x in dir {
        match x.map_err(e).and_then(|x| add_dir_entry(cmd, x, stat, id)) {
            Ok(false) => {}
            Ok(true) => count += 1,
            Err(x) => return Err(x),
        }
    }

    cmd.passthrough(Ok(Action::DirEnd {
        dir: id,
        count,
        parent,
        entry,
    }))
    .unwrap_or_else(|_| unreachable!());
    Ok(())
}

fn add_dir_entry(
    cmd: &mut Sink,
    entry: fs::DirEntry,
    stat: &mut Stat,
    parent: u64,
) -> Result<bool> {
    // TODO support other platforms
    use std::os::unix::fs::MetadataExt;

    enum Type {
        File,
        Dir,
        Sym,
    }

    let path = entry.path();
    let path = path_to_utf8(path)?;
    let ty = entry
        .file_type()
        .map_err(|e| format!("failed to get file type of {path:?}: {e}"))?;
    let ty = match (ty.is_file(), ty.is_dir(), ty.is_symlink()) {
        (true, _, _) => Type::File,
        (_, true, _) => Type::Dir,
        (_, _, true) => Type::Sym,
        _ => {
            eprintln!("skipping {path} (unknown format)");
            return Ok(false);
        }
    };
    let (ty_s, ty_n) = match ty {
        Type::File => ('f', 0),
        Type::Dir => ('d', 1),
        Type::Sym => ('s', 2),
    };

    println!("{ty_s} {path}");

    let name = entry
        .file_name()
        .to_str()
        .expect("already validated before")
        .to_string()
        .into_boxed_str();
    if name.len() > usize::from(u8::MAX) {
        return Err(format!("entry name {name:?} too long").into());
    }
    // rough estimate
    stat.size_sum += u64::from(2 + 2 * 4 + 8 + name.len() as u8);
    let meta = entry
        .metadata()
        .map_err(|e| format!("failed to get metadata of {path:?}: {e}"))?;
    let modified = i128::from(meta.mtime()) * 1_000_000 + i128::from(meta.mtime_nsec() / 1000);
    // not my problem
    let modified = i64::try_from(modified)
        .expect("You have permission to dig up my grave and slap me (if you can find it)");

    let entry = DirEntry {
        type_perms: (meta.mode() as u16 & 0o777) | ty_n << 9,
        name,
        uid: meta.uid(),
        gid: meta.gid(),
        modified,
    };

    match ty {
        Type::File => add_file(cmd, &path, stat, parent, entry)?,
        Type::Dir => add_dir(cmd, &path, stat, parent, entry)?,
        Type::Sym => add_sym(cmd, &path, stat, parent, entry)?,
    }

    Ok(true)
}

impl DirBuilder {
    fn finalize(mut self, cmd: &mut Sink) {
        // RUUUUUUUUUUUUUUST
        // WHY DO YOU STILL HAVE NO CO-SORT IN THE STANDARD LIBRARY
        // AAAAAAAAAAAAAAAAAAAAAAAAAA

        // fuck it lotsofalloc
        {
            let mut both = self
                .entries
                .into_iter()
                .zip(self.refs.into_iter().skip(1))
                .collect::<Vec<_>>();
            both.sort_by(|x, y| x.0.name.cmp(&y.0.name));
            self.refs = [Hash::default()]
                .into_iter()
                .chain(both.iter().map(|x| x.1))
                .collect();
            self.entries = both.into_iter().map(|x| x.0).collect();
        }

        let names_offset = 32 * self.entries.len();
        let data = self
            .entries
            .iter()
            .fold(names_offset, |s, x| s + x.name.len());
        let mut data = Vec::with_capacity(data);
        let mut names_offset = u64::try_from(names_offset).expect("usize <= u64");
        for e in &self.entries {
            let prev_len = data.len();
            data.extend(e.type_perms.to_le_bytes());
            data.push(e.name.len() as u8);
            data.extend([0; 5]);
            data.extend(e.uid.to_le_bytes());
            data.extend(e.gid.to_le_bytes());
            data.extend(names_offset.to_le_bytes());
            data.extend(e.modified.to_le_bytes());
            assert_eq!(prev_len, data.len() - 32);
            names_offset += e.name.len() as u64;
        }
        for e in &self.entries {
            data.extend(e.name.as_bytes());
        }

        cmd.add_data(
            Ok(Action::DirEndData {
                parent: self.parent,
                entry: self.entry,
                refs: self.refs,
            }),
            data,
        )
        .unwrap_or_else(|_| unreachable!());
    }
}

impl Default for DirBuilder {
    fn default() -> Self {
        Self {
            parent: u64::MAX,
            entries: Default::default(),
            refs: vec![Hash::default()],
            count: u64::MAX,
            entry: Default::default(),
        }
    }
}

fn open(store: &Path, write: bool) -> Result<(Toa, Hash)> {
    let dev = Toa::open(store, write)?;
    let key = dev
        .meta("unix.root")
        .ok_or("meta key \"unix.root\" not found")?;
    Ok((dev, key))
}

fn traverse_path(dev: &Toa, path: &str, mut start: Hash) -> Result<Hash> {
    let mut is_dir = true;
    'f: for p in path.split("/").filter(|x| !x.is_empty()) {
        if !is_dir {
            return Err(format!("{p:?} is not a directory").into());
        }
        let dir = Dir::new(dev, &start)?;
        for x in dir.iter() {
            let (i, x) = x.map_err(|e| format!("{e:?}"))?;
            if x.name.len() != p.len() as u64 {
                continue;
            }
            let name = &mut vec![0; p.len()];
            dir.read_data(x.name, name).map_err(|e| format!("{e:?}"))?;
            if name == p.as_bytes() {
                is_dir = matches!(&x.ty, DirItemType::Dir);
                start = dir.get_ref(i).map_err(|e| format!("{e:?}"))?.unwrap();
                continue 'f;
            }
        }
        return Err(format!("entry {p:?} not found").into());
    }
    Ok(start)
}

fn path_to_utf8(path: PathBuf) -> Result<String> {
    path.into_os_string()
        .into_string()
        .map_err(|x| format!("{x:?} is invalid UTF-8").into())
}

fn fmt_item(dev: &InnerToa, dir: &Dir<'_>, item: &DirItem, key: &Hash) -> Result<String> {
    let DirItem {
        ty,
        len,
        name: _,
        uid,
        gid,
        permissions,
        modified,
    } = item;
    let len = if *len == 0 {
        let obj = dev
            .get(key)
            .map_err(|e| format!("fmt_item: {e:?}"))?
            .ok_or("fmt_item: object not found")?;
        match obj {
            Object::Data(x) => x.len()?,
            Object::Refs(x) => x.len()? - 1,
        }
    } else {
        (*len).into()
    };
    let ty = match ty {
        DirItemType::File => '-',
        DirItemType::Dir => 'd',
        DirItemType::SymLink => 'l',
        DirItemType::Unknown { .. } => '?',
    };
    let b = *permissions;
    let g = |b: u16, i: u8, c: u8| if b & 1 << i != 0 { c } else { b'-' };
    let g = |x| [g(x, 2, b'r'), g(x, 1, b'w'), g(x, 0, b'x')];
    let permissions = [g(b >> 6), g(b >> 3), g(b)];
    let permissions = core::str::from_utf8(permissions.as_flattened()).expect("ascii");
    let modified: DateTime<Utc> = DateTime::from_timestamp_micros(*modified).expect("in range");
    let mut name = vec![0; item.name.len() as usize];
    dir.read_data(item.name, &mut name)
        .map_err(|e| format!("name: {e:?}"))?;
    let name = String::from_utf8_lossy(&name); // TODO use BStr
    Ok(format!(
        "{ty}{permissions} {uid}:{gid} {modified:?} {len:>10} {name}"
    ))
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
