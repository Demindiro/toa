mod unix;

use appender::{Hash, Object, ObjectRaw, cache::MicroLru};
use std::{
    collections::{BTreeMap, btree_map},
    error::Error,
    ffi::OsStr,
    fs,
    fs::File,
    io,
    io::{Read, Seek},
    ops,
    time::{Duration, SystemTime},
};

const MAGIC: [u8; 16] = *b"Appender\x20\x25\x12\x25\0\0\0\0";

type Result<T> = core::result::Result<T, Box<dyn Error>>;
type InnerReader = appender::Reader<File, MicroLru<Box<[u8]>>>;

struct Reader {
    inner: InnerReader,
}

#[derive(Default)]
struct Meta {
    map: BTreeMap<Box<str>, Box<[u8]>>,
}

struct Fs {
    dev: Reader,
    root: ObjectRaw,
    nodes: BTreeMap<u64, Node>,
    nodes_rev: BTreeMap<u64, u64>,
    ino_counter: u64,
}

#[derive(Clone, Copy)]
struct Node {
    parent_ino: u64,
    obj: ObjectRaw,
    refcount: u64,
    ty: unix::DirItemType,
}

impl Reader {
    fn new(path: &str) -> Result<(Self, Meta)> {
        let mut dev = fs::OpenOptions::new().read(true).open(path)?;
        let mut buf = [0; 16];
        dev.read_exact(&mut buf)?;
        if buf != MAGIC {
            return Err("bad magic".into());
        }

        let mut buf = [0; 76];
        dev.seek(io::SeekFrom::End(-76))
            .map_err(|e| format!("seek to trailer failed: {e}"))?;
        dev.read_exact(&mut buf)
            .map_err(|e| format!("read of trailer failed: {e}"))?;

        let [a, b, c, d, buf @ ..] = buf;
        let len = u32::from_le_bytes([a, b, c, d]);
        dev.seek(io::SeekFrom::End(-76 - i64::from(len)))
            .map_err(|e| format!("seek to meta table failed: {e}"))?;
        let mut meta = Vec::new();
        (&mut dev)
            .take(u64::from(len))
            .read_to_end(&mut meta)
            .map_err(|e| format!("seek of meta table failed: {e}"))?;

        let meta = parse_meta(&meta).map_err(|e| format!("parsing of meta table failed: {e}"))?;

        let packref = appender::PackRef(buf);
        let inner = InnerReader::new(dev, Default::default(), packref)
            .map_err(|e| format!("failed to initialize reader: {e:?}"))?;
        Ok((Reader { inner }, meta))
    }

    fn get(&self, key: &Hash) -> Result<appender::Object<'_, InnerReader>> {
        self.inner
            .get(&key)
            .map_err(|e| format!("failed to query pack: {e:?}"))?
            .ok_or_else(|| format!("no object with key {key:?}").into())
    }
}

impl ops::Deref for Reader {
    type Target = InnerReader;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Fs {
    fn root_ino(&self) -> Node {
        Node {
            parent_ino: 0,
            obj: self.root,
            refcount: 1,
            ty: unix::DirItemType::Dir,
        }
    }

    fn get_ino(&self, ino: u64) -> Option<Node> {
        (ino == fuser::FUSE_ROOT_ID)
            .then(|| self.root_ino())
            .or_else(|| self.nodes.get(&ino).copied())
    }

    fn get_ino_dir(&self, ino: u64) -> Option<(Node, unix::Dir<'_>)> {
        self.get_ino(ino)
            .filter(|x| x.ty == unix::DirItemType::Dir)
            .map(|x| {
                let d = unix::Dir::new(Object::from_raw(x.obj, &self.dev)).unwrap();
                (x, d)
            })
    }

    fn get_ino_file(&self, ino: u64) -> Option<(Node, Object<'_, InnerReader>)> {
        self.get_ino(ino)
            .filter(|x| x.ty == unix::DirItemType::File)
            .map(|x| (x, Object::from_raw(x.obj, &*self.dev)))
    }

    fn get_ino_symlink(&self, ino: u64) -> Option<(Node, Object<'_, InnerReader>)> {
        self.get_ino(ino)
            .filter(|x| x.ty == unix::DirItemType::SymLink)
            .map(|x| (x, Object::from_raw(x.obj, &*self.dev)))
    }

    /// # Returns
    ///
    /// The current (or new) inode number of the object.
    fn increase_ref(&mut self, parent_ino: u64, obj: ObjectRaw, ty: unix::DirItemType) -> u64 {
        let ino = *self.nodes_rev.entry(obj.offset()).or_insert_with(|| {
            let ino = self.ino_counter;
            self.ino_counter += 1;
            ino
        });
        let node = self.nodes.entry(ino).or_insert_with(|| Node {
            parent_ino,
            obj,
            refcount: 0,
            ty,
        });
        node.refcount += 1;
        ino
    }

    fn decrease_ref(&mut self, ino: u64, num: u64) {
        match self.nodes.entry(ino) {
            btree_map::Entry::Occupied(mut e) => {
                let x = e.get_mut();
                x.refcount = x.refcount.saturating_sub(num);
                if x.refcount == 0 {
                    e.remove();
                }
            }
            // just ignore, whatever
            btree_map::Entry::Vacant(_) => {}
        }
    }
}

impl fuser::Filesystem for Fs {
    fn getattr(
        &mut self,
        _: &fuser::Request<'_>,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        let attr = if ino == fuser::FUSE_ROOT_ID {
            file_attr(
                ino,
                self.root.len(),
                unix::DirItemType::Dir,
                SystemTime::UNIX_EPOCH,
                0o777,
                0,
                0,
            )
        } else {
            // nonsense but w/e
            return reply.error(libc::ENOMEM);
        };
        reply.attr(&Duration::MAX, &attr)
    }

    fn opendir(&mut self, _: &fuser::Request<'_>, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        if self.get_ino_dir(ino).is_some() {
            reply.opened(0, 0)
        } else {
            reply.error(libc::ENOENT)
            //reply.error(libc::ENOTDIR)
        }
    }

    fn readdir(
        &mut self,
        _: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let Some((node, dir)) = self.get_ino_dir(ino) else {
            return reply.error(libc::ENOENT);
            //reply.error(libc::ENOTDIR)
        };
        for i in offset.. {
            let end = match i as u64 {
                0 => reply.add(ino, 1, fuser::FileType::Directory, "."),
                1 => reply.add(node.parent_ino, 2, fuser::FileType::Directory, ".."),
                2.. => {
                    let Some(e) = dir.get((i - 2) as u64).unwrap() else {
                        break;
                    };
                    let ty = match e.ty {
                        unix::DirItemType::File => fuser::FileType::RegularFile,
                        unix::DirItemType::Dir => fuser::FileType::Directory,
                        unix::DirItemType::SymLink => fuser::FileType::Symlink,
                    };
                    reply.add(u64::MAX, i + 1, ty, e.name)
                }
            };
            if end {
                break;
            }
        }
        reply.ok()
    }

    fn lookup(
        &mut self,
        _: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let Some((_, dir)) = self.get_ino_dir(parent) else {
            return reply.error(libc::ENOENT);
            //reply.error(libc::ENOTDIR)
        };
        let Some(name) = name.to_str() else {
            return reply.error(libc::ENOENT);
        };
        for i in 0.. {
            let Some(e) = dir.get(i).unwrap() else { break };
            if e.name != name {
                continue;
            }
            let obj = match e.ty {
                unix::DirItemType::File | unix::DirItemType::Dir => {
                    self.dev.get(&e.key).unwrap().to_raw()
                }
                unix::DirItemType::SymLink => dir.symlink_slice(&e),
            };
            let ino = self.increase_ref(parent, obj, e.ty);
            let mtime = SystemTime::UNIX_EPOCH;
            let mtime = match e.modified {
                ..0 => mtime - Duration::from_micros(-e.modified as u64),
                0.. => mtime + Duration::from_micros(e.modified as u64),
            };
            let attr = file_attr(ino, obj.len(), e.ty, mtime, e.permissions, e.uid, e.gid);
            return reply.entry(&Duration::MAX, &attr, 0);
        }
        reply.error(libc::ENOENT)
    }

    fn forget(&mut self, _: &fuser::Request<'_>, ino: u64, nlookup: u64) {
        self.decrease_ref(ino, nlookup);
    }

    fn read(
        &mut self,
        _: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let Some((_, file)) = self.get_ino_file(ino) else {
            return reply.error(libc::ENOENT);
            //reply.error(libc::ENOTDIR)
        };
        let size = usize::try_from(size).unwrap_or(usize::MAX);
        // kernel gets confused if you don't fill the entire buffer...
        let data = file
            .read_exact(offset as u64, size)
            .unwrap()
            .into_bytes()
            .unwrap();
        reply.data(&data)
    }

    fn readlink(&mut self, _: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        let Some((_, symlink)) = self.get_ino_symlink(ino) else {
            return reply.error(libc::ENOENT);
            //reply.error(libc::ENOTDIR)
        };
        let data = symlink
            .read_exact(0, usize::MAX)
            .unwrap()
            .into_bytes()
            .unwrap();
        reply.data(&data)
    }
}

fn file_attr(
    ino: u64,
    len: u64,
    ty: unix::DirItemType,
    mtime: SystemTime,
    perm: u16,
    uid: u32,
    gid: u32,
) -> fuser::FileAttr {
    let kind = match ty {
        unix::DirItemType::File => fuser::FileType::RegularFile,
        unix::DirItemType::Dir => fuser::FileType::Directory,
        unix::DirItemType::SymLink => fuser::FileType::Symlink,
    };
    fuser::FileAttr {
        ino,
        size: len,
        blocks: (len + 511) / 512,
        atime: SystemTime::UNIX_EPOCH,
        mtime,
        ctime: SystemTime::UNIX_EPOCH,
        crtime: SystemTime::UNIX_EPOCH,
        kind,
        perm,
        nlink: 1,
        uid,
        gid,
        rdev: 0,
        blksize: 1,
        flags: Default::default(),
    }
}

fn usage(procname: &str) -> Box<dyn Error> {
    format!("usage: {procname} <pack> <mount>").into()
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

fn new_reader(pack: &str) -> Result<(Reader, Meta)> {
    Reader::new(pack).map_err(|e| format!("failed to open pack {pack:?}: {e}").into())
}

fn parse_meta(mut buf: &[u8]) -> Result<Meta> {
    let mut meta = Meta::default();
    while let [key_len, b @ ..] = buf {
        let (key, b) = b.split_at(usize::from(*key_len));
        let [x, y, b @ ..] = b else { todo!() };
        let value_len = u16::from_le_bytes([*x, *y]);
        let (value, b) = b.split_at(usize::from(value_len));
        buf = b;
        let key = core::str::from_utf8(key).expect("key is not UTF-8");
        let prev = meta.map.insert(key.into(), value.into());
        assert!(prev.is_none(), "duplicate key {key:?}");
    }
    Ok(meta)
}

fn start() -> Result<()> {
    env_logger::init();

    let mut args = std::env::args();
    let procname = args.next();
    let procname = procname.as_deref().unwrap_or("appender-fuse");

    let pack = args.next().ok_or_else(|| usage(procname))?;
    let mount = args.next().ok_or_else(|| usage(procname))?;
    args_end(procname, args)?;

    let (dev, meta) = new_reader(&pack)?;
    let root = meta
        .map
        .get("unix.root")
        .map(|x| &**x)
        .ok_or("\"unix.root\" not present in meta table")?
        .try_into()
        .map(Hash)
        .map_err(|_| "\"unix.root\" value is not 32 bytes")?;
    let root = dev
        .get(&root)
        .map_err(|e| format!("failed to get root object: {e}"))?
        .to_raw();
    let fs = Fs {
        dev,
        root,
        nodes: Default::default(),
        nodes_rev: Default::default(),
        ino_counter: 2,
    };
    let opt = [
        fuser::MountOption::FSName("appender".into()),
        //fuser::MountOption::AllowOther,
        //fuser::MountOption::AutoUnmount,
        fuser::MountOption::DefaultPermissions,
        fuser::MountOption::NoDev,
        fuser::MountOption::Suid,
        fuser::MountOption::RO,
        fuser::MountOption::Exec,
        fuser::MountOption::NoAtime,
        fuser::MountOption::Sync, // TODO not correct? Should be async (eventually)?
    ];
    fuser::mount2(fs, mount, &opt).map_err(|e| format!("failed to mount pack: {e}"))?;
    Ok(())
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
