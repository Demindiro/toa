use crate::{Result, Toa, args_end, usage};

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

    let dev = Toa::open(&std::path::PathBuf::from(pack), false)?;
    let buf = &mut [0; 1 << 13];
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
        let obj = dev.get(&key).expect("exists");
        let toa::Object::Data(obj) = obj else {
            return false;
        };
        let n = obj.read(0, buf).unwrap();
        let ty = cookie.buffer(&buf[..n]).unwrap();
        println!("{key} {ty}");
        false
    })
    .map_err(|e| format!("failure during store iteration: {e:?}"))?;

    Ok(())
}
