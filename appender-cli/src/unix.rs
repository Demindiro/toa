use crate::{
    Builder, InnerReader, Meta, Reader, Result, Stat, add_file, args_end, finish, new_builder,
    usage,
};
use appender::{Hash, Object};
use chrono::prelude::*;
use std::{fmt, fs, path::Path};

const MAGIC: [u8; 24] = *b"Appender UNIX directory\0";

struct DirIter<'a> {
    object: Object<'a, InnerReader>,
    total: u64,
    cur: u64,
}

struct DirItem {
    ty: DirItemType,
    permissions: u16,
    name: String,
    uid: u32,
    gid: u32,
    modified: i64,
    key: Hash,
}

enum DirItemType {
    File,
    Dir,
    SymLink,
}

struct ObjectRawOrd(appender::ObjectRaw);

impl<'a> DirIter<'a> {
    fn new(dev: &'a Reader, key: &Hash) -> Result<Self> {
        dev.get(key)
            .and_then(Self::from_object)
            .map_err(|e| format!("{key:?}: {e:?}").into())
    }

    fn from_object(object: Object<'a, InnerReader>) -> Result<Self> {
        let hdr = object
            .read_exact(0, 32)
            .and_then(|x| x.into_bytes())
            .map_err(|e| format!("failed to get directory header: {e:?}"))?;
        let hdr: [u8; 32] = hdr
            .try_into()
            .map_err(|_| format!("truncated (or invalid) directory header"))?;
        let [magic @ .., a, b, c, d, e, f, g, h] = hdr;
        if magic != MAGIC {
            return Err(format!("bad dir magic").into());
        }
        let total = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let cur = 0;
        Ok(Self { object, total, cur })
    }

    fn try_next(&mut self) -> Result<Option<DirItem>> {
        if self.cur >= self.total {
            return Ok(None);
        }
        let x = self
            .object
            .read_exact(32 + self.cur * 64, 64)
            .and_then(|x| x.into_bytes())
            .map_err(|e| format!("failed to get directory entry: {e:?}"))?;
        let x: [u8; 64] = x.try_into().map_err(|_| "directory entry is truncated")?;
        self.cur += 1;
        let [a, b, x @ ..] = x;
        let ty_perms = u16::from_le_bytes([a, b]);
        let ty = match ty_perms >> 9 {
            0 => DirItemType::File,
            1 => DirItemType::Dir,
            2 => DirItemType::SymLink,
            x => return Err(format!("invalid type {x} for directory entry"))?,
        };
        let [name_len, x @ ..] = x;
        let [_, _, _, _, _, x @ ..] = x;
        let [a, b, c, d, x @ ..] = x;
        let uid = u32::from_le_bytes([a, b, c, d]);
        let [a, b, c, d, x @ ..] = x;
        let gid = u32::from_le_bytes([a, b, c, d]);
        let [a, b, c, d, e, f, g, h, x @ ..] = x;
        let name_offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let [a, b, c, d, e, f, g, h, x @ ..] = x;
        let modified = i64::from_le_bytes([a, b, c, d, e, f, g, h]);
        let key = Hash(x);
        let name = self
            .object
            .read_exact(name_offset, name_len.into())
            .and_then(|x| x.into_bytes())
            .map_err(|e| format!("failed to get name of directory entry: {e:?}"))?;
        // TODO length check
        // also use a pretty-printer like BStr
        let name = String::from_utf8_lossy(&name).to_string();
        Ok(Some(DirItem {
            ty,
            permissions: ty_perms & 0o777,
            uid,
            gid,
            name,
            modified,
            key,
        }))
    }
}

impl Iterator for DirIter<'_> {
    type Item = Result<DirItem>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        usize::try_from(self.total - self.cur).map_or((usize::MAX, None), |x| (x, Some(x)))
    }
}

impl fmt::Display for DirItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let DirItem {
            ty,
            name,
            uid,
            gid,
            permissions,
            modified,
            key,
        } = self;
        let ty = match ty {
            DirItemType::File => '-',
            DirItemType::Dir => 'd',
            DirItemType::SymLink => 's',
        };
        let b = *permissions;
        let g = |b: u16, i: u8, c: u8| if b & 1 << i != 0 { c } else { b'-' };
        let g = |x| [g(x, 2, b'r'), g(x, 1, b'w'), g(x, 0, b'x')];
        let permissions = [g(b >> 6), g(b >> 3), g(b)];
        let permissions = core::str::from_utf8(permissions.as_flattened()).expect("ascii");
        let modified: DateTime<Utc> = DateTime::from_timestamp_micros(*modified).expect("in range");
        write!(
            f,
            "{key:?} {ty}{permissions} {uid}:{gid} {modified:?} {name}"
        )
    }
}

impl PartialEq for ObjectRawOrd {
    fn eq(&self, rhs: &Self) -> bool {
        self.0.offset() == rhs.0.offset()
    }
}

impl Eq for ObjectRawOrd {}

impl PartialOrd for ObjectRawOrd {
    fn partial_cmp(&self, rhs: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(rhs))
    }
}

impl Ord for ObjectRawOrd {
    fn cmp(&self, rhs: &Self) -> core::cmp::Ordering {
        self.0.offset().cmp(&rhs.0.offset())
    }
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
        "scrub" => cmd_scrub(procname, args),
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

    let mut dev = new_builder(&store)?;
    let mut stat = Stat::default();
    let root_key = add_dir(&mut dev, &root, &mut stat)?;
    println!("d {root_key:?} {root}");
    let mut meta = Meta::default();
    meta.map.insert("unix.root".into(), root_key.0.into());
    let dev = finish(dev, meta)?;

    stat.summarize(&dev)?;

    Ok(())
}

fn cmd_get<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let path = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, dir) = new_reader(&store)?;
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

    let (dev, dir) = new_reader(&store)?;
    let dir = traverse_path(&dev, path, dir)?;
    DirIter::new(&dev, &dir)?.try_for_each(|e| e.map(|e| println!("{e}")))?;

    Ok(())
}

fn cmd_scrub<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, dir) = new_reader(&store)?;
    // pre-collect + sorting makes a *huge* difference in performance
    // larger caches would help, but that ooesn't scale as well as
    // having a better iteration order.
    //
    // TODO binary heap will include duplicates
    // might want to use something else, like a BTreeSet?
    let dir = ObjectRawOrd(dev.get(&dir)?.to_raw());
    let mut stack = std::collections::BinaryHeap::from([dir]);
    let mut n_ok @ mut n_fail = 0;
    while let Some(dir) = stack.pop() {
        let mut items =
            DirIter::from_object(Object::from_raw(dir.0, &dev))?.collect::<Result<Vec<_>>>()?;
        items.sort_by_key(|x| x.key);
        for x in items {
            match x.ty {
                DirItemType::Dir => stack.push(ObjectRawOrd(dev.get(&x.key)?.to_raw())),
                DirItemType::File => {}
                DirItemType::SymLink => continue,
            }
            let Ok(has) = dev
                .contains_key(&x.key)
                .inspect_err(|e| eprintln!("failed to fetch {:?}: {e:?}", x.key))
            else {
                n_fail += 1;
                continue;
            };
            if !has {
                println!("missing {:?}", x.key);
                n_fail += 1;
                continue;
            } else {
                n_ok += 1;
            }
        }
    }

    eprintln!("ok:{n_ok} fail:{n_fail}");

    (n_fail == 0)
        .then_some(())
        .ok_or_else(|| "some objects missing".into())
}

fn add_dir(dev: &mut Builder, path: &str, stat: &mut Stat) -> Result<Hash> {
    // TODO support other platforms
    use std::os::unix::fs::MetadataExt;

    enum Data {
        Object(Hash),
        Sym(String),
    }

    struct Entry {
        type_perms: u16,
        name: Box<str>,
        uid: u32,
        gid: u32,
        modified: i64,
        key: Data,
    }

    impl fmt::Debug for Data {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Object(x) => x.fmt(f),
                Self::Sym(x) => x.fmt(f),
            }
        }
    }

    let mut entries = Vec::new();

    let e = |e| format!("failed to traverse {path:?}: {e}");
    for entry in fs::read_dir(path).map_err(e)? {
        let entry = entry.map_err(e)?;
        let path = entry.path();
        let path = path_to_utf8(&path)?;
        let ty = entry
            .file_type()
            .map_err(|e| format!("failed to get file type of {path:?}: {e}"))?;
        let (ty_s, ty_n, key) = if ty.is_file() {
            ("f", 0, Data::Object(add_file(dev, path, stat)?))
        } else if ty.is_dir() {
            ("d", 1, Data::Object(add_dir(dev, path, stat)?))
        } else if ty.is_symlink() {
            ("s", 2, Data::Sym(add_symlink(path, stat)?))
        } else {
            eprintln!("skipping {path} (unknown format)");
            continue;
        };
        println!("{ty_s} {key:?} {path}");
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
        entries.push(Entry {
            type_perms: (meta.mode() as u16 & 0o777) | ty_n << 9,
            name,
            uid: meta.uid(),
            gid: meta.gid(),
            modified,
            key,
        });
    }

    entries.sort_by(|x, y| x.name.cmp(&y.name));

    let names_offset = 32 + 64 * entries.len();
    let buf = entries.iter().fold(names_offset, |s, x| s + x.name.len());
    let mut buf = Vec::with_capacity(buf);
    let mut names_offset = u64::try_from(names_offset).expect("usize <= u64");

    buf.extend(MAGIC);
    buf.extend(
        &u64::try_from(entries.len())
            .expect("usize <= u64")
            .to_le_bytes(),
    );
    for e in &entries {
        let prev_len = buf.len();
        buf.extend(e.type_perms.to_le_bytes());
        buf.push(e.name.len() as u8);
        buf.extend([0; 5]);
        buf.extend(e.uid.to_le_bytes());
        buf.extend(e.gid.to_le_bytes());
        buf.extend(names_offset.to_le_bytes());
        buf.extend(e.modified.to_le_bytes());
        match &e.key {
            Data::Object(x) => buf.extend(x.0),
            Data::Sym(x) => {
                let len = x.len() as u64;
                // FIXME we're adding the name offset *after* this statement,
                // not *before*
                // whoopdadoop
                buf.extend(names_offset.to_le_bytes());
                buf.extend(len.to_le_bytes());
                buf.extend([0; 16]);
                names_offset += len;
            }
        }
        assert_eq!(prev_len, buf.len() - 64);
        names_offset += e.name.len() as u64;
    }
    for e in &entries {
        buf.extend(e.name.as_bytes());
        match &e.key {
            Data::Object(_) => {}
            Data::Sym(x) => buf.extend(x.as_bytes()),
        }
    }

    dev.add(&buf, &[])
        .map_err(|e| format!("failed to add : {e:?}").into())
}

fn add_symlink(path: &str, stat: &mut Stat) -> Result<String> {
    let link =
        fs::read_link(path).map_err(|e| format!("failed to read target of {path:?}: {e}"))?;
    let link = path_to_utf8(&link)?;
    stat.size_sum += u64::try_from(link.len()).expect("usize <= u64");
    Ok(link.into())
}

fn new_reader(store: &str) -> Result<(Reader, Hash)> {
    let (dev, meta) = super::new_reader(store)?;
    let key = meta
        .map
        .get("unix.root")
        .ok_or("meta key \"unix.root\" not found")?;
    let key = (&**key)
        .try_into()
        .map(Hash)
        .map_err(|_| "\"unix.root\" value is not 32 bytes")?;
    Ok((dev, key))
}

fn traverse_path(dev: &Reader, path: &str, mut start: Hash) -> Result<Hash> {
    let mut is_dir = true;
    for p in path.split("/").filter(|x| !x.is_empty()) {
        if !is_dir {
            return Err(format!("{p:?} is not a directory").into());
        }
        let Some(x) = DirIter::new(&dev, &start)?
            .find(|x| x.is_err() || x.as_ref().is_ok_and(|x| x.name == p))
        else {
            return Err(format!("entry {p:?} not found").into());
        };
        let x = x?;
        is_dir = matches!(&x.ty, DirItemType::Dir);
        start = x.key;
    }
    Ok(start)
}

fn path_to_utf8(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| format!("{path:?} is invalid UTF-8").into())
}
