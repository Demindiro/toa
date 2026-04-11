use crate::{Object, Result, Stat, Toa, arg_store_accel, args_end, usage};
use regex::Regex;
use std::{fs, io::Write, path::Path};
use toa::Hash;

pub fn cmd<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "add" => cmd_add(procname, args),
        "get" => cmd_get(procname, args),
        "ls" => cmd_ls(procname, args),
        _ => Err(usage(procname)),
    }
}

fn cmd_add<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let [store, accel] = arg_store_accel(procname, &mut args)?;
    let name = args.next().ok_or_else(|| usage(procname))?;
    let dir = args.next().ok_or_else(|| usage(procname))?;

    let mut skip = None;

    while let Some(a) = args.next() {
        match &*a {
            "-e" => {
                assert!(skip.is_none());
                let x = args.next().unwrap();
                let x = Regex::new(&x).unwrap();
                skip = Some(x);
            }
            a => todo!("unexpected argument {a}"),
        }
    }

    let skip = skip.unwrap_or_else(|| Regex::new("^$").unwrap());

    args_end(procname, args)?;

    let mut dev = Toa::load(&store, &accel, true)?;
    let mut stat = Stat::new(&dev)?;

    let t_start = std::time::Instant::now();
    let root_key = add_dir(&mut dev, &dir, &mut stat, &skip)?;
    let t_end = std::time::Instant::now();
    println!("{:?} elapsed", t_end.duration_since(t_start));

    dev.set_meta(&name, &root_key);
    dev.save_root()?;

    dev.flush()?;

    stat.summarize(&dev);

    Ok(())
}

fn cmd_get<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let [store, accel] = arg_store_accel(procname, &mut args)?;
    let name = args.next().ok_or_else(|| usage(procname))?;
    let path = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, dir) = open(&store, &accel, &name, false)?;
    let file = traverse_path(&dev, &path, dir)?;
    crate::dump_object(&dev, &file)?;

    Ok(())
}

fn cmd_ls<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let [store, accel] = arg_store_accel(procname, &mut args)?;
    let name = args.next().ok_or_else(|| usage(procname))?;
    let path = args.next();
    let path = path.as_deref().unwrap_or("/");
    args_end(procname, args)?;

    let (toa, dir) = open(&store, &accel, &name, false)?;
    let dir = traverse_path(&toa, path, dir)?;
    let dir = toa.toa.get(&dir)?;
    let Object::Refs(dir) = dir else {
        return Err(format!("{path:?} is not a directory").into());
    };
    let dir = toa.toa.iter_dir(dir)?;
    for x in dir {
        let (name, key) = x?;
        println!("{key}  {name}");
    }

    Ok(())
}

fn add_dir(dev: &mut Toa, path: &str, stat: &mut Stat, skip: &Regex) -> Result<Hash> {
    let mut entries = Vec::new();

    let e = |e| format!("failed to traverse {path:?}: {e}");
    for entry in fs::read_dir(path).map_err(e)? {
        let entry = entry.map_err(e)?;
        let path = entry.path();

        let path = path_to_utf8(&path)?;

        if skip.is_match(&*path) {
            eprintln!("skipping {path}");
            stat.skipped += 1;
            continue;
        }

        let res = (|| -> Result<()> {
            let ty = fs::metadata(&path)?.file_type();
            let key = if ty.is_file() {
                add_file(dev, path, stat)?
            } else if ty.is_dir() {
                add_dir(dev, path, stat, skip)?
            } else {
                eprintln!("skipping {path} (unknown format)");
                stat.dropped += 1;
                return Ok(());
            };
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
            entries.push((name, key));
            Ok(())
        })();
        match res {
            Ok(()) => {}
            Err(e) => {
                eprintln!("failed to read {path:?}: {e}");
                stat.dropped += 1;
            }
        }
    }

    print!("{path}/ -> ");
    let _ = std::io::stdout().flush();

    entries.sort_by(|x, y| x.0.cmp(&y.0));

    let dir = dev.toa.add_dir(entries.iter().map(|(k, v)| (&**k, *v)))?;

    println!("{dir}");
    Ok(dir)
}

fn add_file(dev: &mut Toa, path: &str, stat: &mut Stat) -> Result<Hash> {
    print!("{path} -> ");
    let _ = std::io::stdout().flush();

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
        .toa
        .inner
        .add_data(&data)
        .map_err(|e| format!("failed to add {path:?} to store: {e:?}"))?;

    println!("{key}");
    Ok(key)
}

fn open(store: &Path, accel: &Path, name: &str, write: bool) -> Result<(Toa, Hash)> {
    let dev = Toa::load(store, accel, write)?;
    let key = dev.meta(name).ok_or("meta key \"unix.root\" not found")?;
    Ok((dev, key))
}

fn traverse_path(dev: &Toa, path: &str, mut start: Hash) -> Result<Hash> {
    'f: for p in path.split("/").filter(|x| !x.is_empty()) {
        let dir = dev.toa.get(&start)?;
        let Object::Refs(dir) = dir else {
            return Err(format!("{p:?} is not a directory").into());
        };
        for x in dev.toa.iter_dir(dir)? {
            let (name, key) = x?;
            if name == p {
                start = key;
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
