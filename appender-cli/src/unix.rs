use crate::{args_end, new_builder, usage};
use appender::{Hash, Object};
use chrono::prelude::*;
use std::{
    collections::HashMap,
    fmt, fs,
    fs::File,
    io,
    io::{Read, Seek, Write},
    path::PathBuf,
};

const MAGIC: [u8; 24] = *b"Appender UNIX directory\0";

type Builder = appender::Builder<File>;
type Reader = appender::Reader<File, appender::cache::MicroLru<Box<[u8]>>>;

#[derive(Default)]
struct Stat {
    size_sum: u64,
}

struct DirIter<'a> {
    object: Object<'a, Reader>,
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

impl<'a> DirIter<'a> {
    fn new(dev: &'a Reader, key: &Hash) -> Self {
        let mut object = dev.get(key).unwrap().unwrap();
        let hdr = object.read_exact(0, 32).unwrap().into_bytes().unwrap();
        if hdr[..24] != MAGIC {
            todo!("bad dir magic");
        }
        let total = u64::from_le_bytes(hdr[24..].try_into().unwrap());
        Self {
            object,
            total,
            cur: 0,
        }
    }
}

impl Iterator for DirIter<'_> {
    type Item = DirItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur >= self.total {
            return None;
        }
        let x = self
            .object
            .read_exact(32 + self.cur * 64, 64)
            .unwrap()
            .into_bytes()
            .unwrap();
        self.cur += 1;
        let ty_perms = u16::from_le_bytes(x[..2].try_into().unwrap());
        let ty = match ty_perms >> 9 {
            0 => DirItemType::File,
            1 => DirItemType::Dir,
            2 => DirItemType::SymLink,
            _ => todo!("{ty_perms:o}"),
        };
        let name_len = x[2];
        let uid = u32::from_le_bytes(x[8..12].try_into().unwrap());
        let gid = u32::from_le_bytes(x[12..16].try_into().unwrap());
        let name_offset = u64::from_le_bytes(x[16..24].try_into().unwrap());
        let modified = i64::from_le_bytes(x[24..32].try_into().unwrap());
        let key = Hash(x[32..64].try_into().unwrap());
        let name = self
            .object
            .read_exact(name_offset, name_len.into())
            .unwrap()
            .into_bytes()
            .unwrap();
        let name = String::from_utf8_lossy(&name).to_string();
        Some(DirItem {
            ty,
            permissions: ty_perms & 0o777,
            uid,
            gid,
            name,
            modified,
            key,
        })
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
        use fmt::Write;
        write!(
            f,
            "{key:?} {ty}{permissions} {uid}:{gid} {modified:?} {name}"
        )
    }
}

pub fn cmd<A>(procname: &str, mut args: A) -> Result<(), i32>
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

fn cmd_new<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let root = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let mut dev = new_builder(&store).unwrap();
    let mut stat = Stat::default();
    let root_key = add_dir(&mut dev, &root, &mut stat)?;
    println!("d {root_key:?} {root}");
    let (mut dev, packref) = dev.finish().unwrap();
    let packref = packref.unwrap();
    dev.write_all(&root_key.0).unwrap();
    dev.write_all(&packref.0).unwrap();

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
    let path = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, dir) = new_reader(&store).unwrap();
    let file = traverse_path(&dev, &path, dir)?;
    let mut file = dev.get(&file).unwrap().unwrap();
    let mut io = std::io::stdout().lock();
    for x in file.read_exact(0, usize::MAX).unwrap() {
        let x = x.unwrap();
        io.write_all(&x).unwrap();
    }

    Ok(())
}

fn cmd_ls<A>(procname: &str, mut args: A) -> Result<(), i32>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    let path = args.next();
    let path = path.as_deref().unwrap_or("/");
    args_end(procname, args)?;

    let (dev, dir) = new_reader(&store).unwrap();
    let dir = traverse_path(&dev, path, dir)?;
    DirIter::new(&dev, &dir).for_each(|e| println!("{e}"));

    Ok(())
}

fn add_file(dev: &mut Builder, path: &str, stat: &mut Stat) -> Result<Hash, i32> {
    // TODO don't read huge files in one go
    let data = fs::read(path).unwrap();
    stat.size_sum += u64::try_from(data.len()).unwrap();
    Ok(dev.add(&data).unwrap())
}

fn add_dir(dev: &mut Builder, path: &str, stat: &mut Stat) -> Result<Hash, i32> {
    // TODO support other platforms
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    struct Entry {
        type_perms: u16,
        name: Box<str>,
        uid: u32,
        gid: u32,
        modified: i64,
        key: Hash,
    }

    let mut entries = Vec::new();

    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let path = path.to_str().unwrap_or_else(|| todo!("invalid UTF-8 path"));
        let ty = entry.file_type().unwrap();
        let (ty_s, ty_n, key) = if ty.is_file() {
            ("f", 0, add_file(dev, path, stat)?)
        } else if ty.is_dir() {
            ("d", 1, add_dir(dev, path, stat)?)
        } else if ty.is_symlink() {
            ("s", 2, add_symlink(dev, path, stat)?)
        } else {
            eprintln!("skipping {path} (unknown format)");
            continue;
        };
        println!("{ty_s} {key:?} {path}");
        let name = entry
            .file_name()
            .to_str()
            .unwrap()
            .to_string()
            .into_boxed_str();
        if name.len() > usize::from(u8::MAX) {
            todo!("name too long");
        }
        // rough estimate
        stat.size_sum += u64::from(2 + 2 * 4 + 8 + name.len() as u8);
        let meta = entry.metadata().unwrap();
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
        buf.extend(e.key.0);
        assert_eq!(prev_len, buf.len() - 64);
        names_offset += e.name.len() as u64;
    }
    for e in &entries {
        buf.extend(e.name.as_bytes());
    }

    Ok(dev.add(&buf).unwrap())
}

fn add_symlink(dev: &mut Builder, path: &str, stat: &mut Stat) -> Result<Hash, i32> {
    let link = fs::read_link(path).unwrap();
    let link = link.to_str().unwrap_or_else(|| todo!("invalid UTF-8 path"));
    stat.size_sum += u64::try_from(link.len()).expect("usize <= u64");
    Ok(dev.add(link.as_bytes()).unwrap())
}

fn new_reader(store: &str) -> io::Result<(Reader, Hash)> {
    let mut dev = fs::OpenOptions::new().read(true).open(store)?;
    dev.seek(io::SeekFrom::End(-72 - 32)).unwrap();
    let mut key = Hash([0; 32]);
    let mut packref = appender::PackRef([0; 72]);
    dev.read_exact(&mut key.0)?;
    dev.read_exact(&mut packref.0)?;
    let dev = Reader::new(dev, Default::default(), packref).unwrap();
    Ok((dev, key))
}

fn traverse_path(dev: &Reader, path: &str, mut start: Hash) -> Result<Hash, i32> {
    let mut is_dir = true;
    for p in path.split("/").filter(|x| !x.is_empty()) {
        if !is_dir {
            eprintln!("not a directory");
            return Err(1);
        }
        let Some(x) = DirIter::new(&dev, &start).find(|x| x.name == p) else {
            eprintln!("directory not found");
            return Err(1);
        };
        is_dir = matches!(&x.ty, DirItemType::Dir);
        start = x.key;
    }
    Ok(start)
}
