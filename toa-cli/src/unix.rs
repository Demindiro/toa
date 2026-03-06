use crate::{InnerToa, Result, Stat, Toa, add_file, args_end, usage};
use chrono::prelude::*;
use std::{
    fs,
    path::{Path, PathBuf},
};
use toa::{Hash, Object};
use toa_unix::{Dir, DirItem, DirItemType};

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

    let store = PathBuf::from(store);

    let mut dev = Toa::open(&store)?;
    let mut stat = Stat::default();
    let root_key = add_dir(&mut dev, &root, &mut stat)?;
    println!("d {root_key:?} {root}");
    dev.set_meta("unix.root", &root_key);
    dev.save_root()?;

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

    let (dev, dir) = open(&store)?;
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

    let (dev, dir) = open(&store)?;
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

fn add_dir(dev: &mut Toa, path: &str, stat: &mut Stat) -> Result<Hash> {
    // TODO support other platforms
    use std::os::unix::fs::MetadataExt;

    struct Entry {
        type_perms: u16,
        name: Box<str>,
        uid: u32,
        gid: u32,
        modified: i64,
        key: Hash,
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

    let names_offset = 32 * entries.len();
    let data = entries.iter().fold(names_offset, |s, x| s + x.name.len());
    let mut data = Vec::with_capacity(data);
    let mut names_offset = u64::try_from(names_offset).expect("usize <= u64");
    for e in &entries {
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
    for e in &entries {
        data.extend(e.name.as_bytes());
    }
    let data = dev
        .add_data(&data)
        .map_err(|e| format!("failed to add : {e:?}"))?;

    let mut refs = Vec::with_capacity(1 + entries.len());
    refs.push(data);
    refs.extend(entries.iter().map(|e| e.key));
    let refs = dev
        .add_refs(&refs)
        .map_err(|e| format!("failed to add : {e:?}"))?;
    Ok(refs)
}

fn add_symlink(dev: &mut Toa, path: &str, stat: &mut Stat) -> Result<Hash> {
    let link =
        fs::read_link(path).map_err(|e| format!("failed to read target of {path:?}: {e}"))?;
    let link = path_to_utf8(&link)?;
    stat.size_sum += u64::try_from(link.len()).expect("usize <= u64");
    let key = dev
        .add_data(link.as_bytes())
        .map_err(|e| format!("failed to add {path:?} to store: {e:?}"))?;
    Ok(key)
}

fn open(store: &Path) -> Result<(Toa, Hash)> {
    let dev = Toa::open(store)?;
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

fn path_to_utf8(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| format!("{path:?} is invalid UTF-8").into())
}

fn fmt_item(
    dev: &InnerToa,
    dir: &Dir<toa::Blob<fs::File>>,
    item: &DirItem,
    key: &Hash,
) -> Result<String> {
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
