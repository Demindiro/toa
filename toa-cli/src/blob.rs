use crate::{Result, args_end, load_store, usage};
use std::path::PathBuf;
use toa::BlobStore;

pub fn cmd<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "debug" => cmd_debug(procname, args),
        "ls" => cmd_ls(procname, args),
        _ => Err(usage(procname)),
    }
}

pub fn cmd_debug<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "log" => cmd_debug_log(procname, args),
        _ => Err(usage(procname)),
    }
}

pub fn cmd_debug_log<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let store = PathBuf::from(store);
    let dev = crate::load_dev(&store, false)?;
    let end = toa_blob::log::iter_with(&dev, |entry| {
        let mut s = format!("{entry}");
        if s.len() > 72 {
            let mut i = 69;
            while !s.is_char_boundary(i) {
                i -= 1;
            }
            s.truncate(i);
            s += "...";
        }
        println!("{s}");
        Ok(())
    })?;
    println!("{end}");

    Ok(())
}

pub fn cmd_ls<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let mut fmt_iec = false;

    let store = args.next().ok_or_else(|| usage(procname))?;
    while let Some(x) = args.next() {
        match &*x {
            "--iec" => fmt_iec = true,
            _ => return Err(usage(procname)),
        }
    }
    args_end(procname, args)?;

    let store = PathBuf::from(store);
    let store = load_store(&store, false)?;

    for blob in store.blobs()? {
        let blob = blob?;
        let name = store.name(&blob)?;
        let len = store.len(&blob)?;
        if fmt_iec {
            let len = crate::fmt_size_iec_short(len);
            println!("{len:>12} {name}");
        } else {
            println!("{len:>12} {name}");
        }
    }

    Ok(())
}
