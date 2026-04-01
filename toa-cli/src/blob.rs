use crate::{Result, args_end, load_store, usage};
use std::path::PathBuf;
use toa::BlobStore;

pub fn cmd<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "ls" => cmd_ls(procname, args),
        _ => Err(usage(procname)),
    }
}

pub fn cmd_ls<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let store = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let store = PathBuf::from(store);
    let store = load_store(&store, false)?;

    for blob in store.blobs()? {
        let blob = blob?;
        let name = store.name(&blob)?;
        let len = store.len(&blob)?;
        println!("{len:>12} {name}");
    }

    Ok(())
}
