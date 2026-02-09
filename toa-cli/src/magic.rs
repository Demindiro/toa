use crate::{Result, args_end, new_reader, usage};

pub fn cmd<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let cmd = args.next().ok_or_else(|| usage(procname))?;
    match &*cmd {
        "all" => cmd_all(procname, args),
        _ => Err(usage(procname)),
    }
}

fn cmd_all<A>(procname: &str, mut args: A) -> Result<()>
where
    A: Iterator<Item = String>,
{
    let pack = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let cookie = magic::cookie::Flags::MIME;
    let cookie = magic::Cookie::open(cookie)?;
    let db = magic::cookie::DatabasePaths::default();
    let cookie = cookie.load(&db)?;

    let (dev, _meta) = new_reader(&pack)?;
    dev.iter_with(|key| {
        // TODO we can't load the entire file in memory as it may be hundred of GBs in size
        // For now loading just the 64KiB is likely sufficient,
        // but not all file types necessarily put the magic at the start.
        //
        // For Linux, the easiest workaround would probably be a "fake" mmap():
        // unmap a large range and lazily load pages that causes a segfault.
        //
        // OTOH, it appears even `file` itself can't check the end of files,
        // so perhaps it doesn't matter?
        //println!("{key:?} {}");
        // "read_exact" is such a terrible name...
        let head = dev
            .get(&key)
            .expect("exists")
            .read_exact(0, 1 << 16)
            .unwrap()
            .into_bytes()
            .unwrap();
        let ty = cookie.buffer(&head).unwrap();
        println!("{key} {ty}");
        false
    })
    .map_err(|e| format!("failure during store iteration: {e:?}"))?;

    Ok(())
}
