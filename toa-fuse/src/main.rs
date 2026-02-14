use std::{
    collections::{BTreeMap, btree_map},
    error::Error,
    ffi::{OsStr, OsString},
    fs,
    fs::File,
    io,
    io::{Read, Seek},
    ops,
    os::unix::ffi::OsStringExt,
    time::{Duration, SystemTime},
};
use toa::{Hash, Object};
use toa_unix::DirItemType;

const XATTR_NAME_LIST: &[u8] = b"user.hash.toa\0";
const XATTR_NAME_HASH_TOA: &[u8] = b"user.hash.toa";

type Result<T> = core::result::Result<T, Box<dyn Error>>;
type InnerToa = toa::Toa<toa::ToaKvStore<toa_kv::sled::Tree>>;

struct Toa {
    inner: InnerToa,
}

struct Meta {
    map: toa_kv::sled::Tree,
}

struct Fs {
    dev: Toa,
    root: Node,
    nodes: BTreeMap<u64, Node>,
    nodes_rev: BTreeMap<Hash, u64>,
    ino_counter: u64,
}

struct Node {
    parent_ino: u64,
    refcount: u64,
    ty: DirItemType,
    len: u64,
    key: Hash,
    mtime: SystemTime,
    perm: u16,
    uid: u32,
    gid: u32,
}

impl Toa {
    fn new(path: &str) -> Result<(Self, Meta)> {
        let db = toa_kv::sled::Db::open(path)?;
        let toa = db.open_tree("toa")?;
        let meta = db.open_tree("meta")?;
        let inner = toa::Toa::new(toa::ToaKvStore(toa));
        Ok((Self { inner }, Meta { map: meta }))
    }

    fn get(&self, key: &Hash) -> Result<toa::Object<&InnerToa>> {
        self.inner
            .get(&key)
            .map_err(|e| format!("failed to query pack: {e:?}"))?
            .ok_or_else(|| format!("no object with key {key:?}").into())
    }
}

impl ops::Deref for Toa {
    type Target = InnerToa;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Fs {
    fn get_ino(&self, ino: u64) -> Option<&Node> {
        (ino == fuser::FUSE_ROOT_ID)
            .then(|| &self.root)
            .or_else(|| self.nodes.get(&ino))
    }

    fn get_ino_dir(&self, ino: u64) -> Option<(&Node, toa_unix::Dir<Object<&InnerToa>>)> {
        self.get_ino(ino)
            .filter(|x| matches!(&x.ty, DirItemType::Dir))
            .map(|x| (x, toa_unix::Dir::new(self.dev.get(&x.key).unwrap())))
    }

    fn get_ino_file(&self, ino: u64) -> Option<(&Node, Object<&InnerToa>)> {
        self.get_ino(ino)
            .filter(|x| matches!(&x.ty, DirItemType::File))
            .map(|x| (x, self.dev.get(&x.key).unwrap()))
    }

    fn get_ino_symlink(&self, ino: u64) -> Option<(&Node, Object<&InnerToa>)> {
        self.get_ino(ino)
            .filter(|x| matches!(&x.ty, DirItemType::SymLink))
            .map(|x| (x, self.dev.get(&x.key).unwrap()))
    }

    /// # Returns
    ///
    /// The current (or new) inode number of the object.
    fn increase_ref(
        &mut self,
        parent_ino: u64,
        ty: DirItemType,
        len: u64,
        key: Hash,
        perm: u16,
        mtime: SystemTime,
        uid: u32,
        gid: u32,
    ) -> u64 {
        let ino = *self.nodes_rev.entry(key).or_insert_with(|| {
            let ino = self.ino_counter;
            self.ino_counter += 1;
            ino
        });
        let node = self.nodes.entry(ino).or_insert_with(|| Node {
            parent_ino,
            ty,
            len,
            key,
            refcount: 0,
            perm,
            mtime,
            uid,
            gid,
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
        let node = self
            .get_ino(ino)
            .unwrap_or_else(|| panic!("ino {ino} not found"));
        let attr = file_attr(
            ino, node.ty, node.len, node.mtime, node.perm, node.uid, node.gid,
        );
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
                        toa_unix::DirItemType::File => fuser::FileType::RegularFile,
                        toa_unix::DirItemType::Dir => fuser::FileType::Directory,
                        toa_unix::DirItemType::SymLink => fuser::FileType::Symlink,
                        toa_unix::DirItemType::Unknown { .. } => todo!(),
                    };
                    let mut nam = vec![0; e.name.len() as usize];
                    dir.read_data(e.name, &mut nam).unwrap();
                    let nam = OsString::from_vec(nam);
                    reply.add(u64::MAX, i + 1, ty, nam)
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
            if e.name.len() != name.len() as u64 {
                continue;
            }
            let mut nam = vec![0; name.len()];
            dir.read_data(e.name, &mut nam).unwrap();
            if name.as_bytes() != &*nam {
                continue;
            }
            let key = dir.get_ref(i).unwrap().unwrap();
            let len = if e.len != 0 {
                e.len
            } else {
                self.dev
                    .get(&key)
                    .unwrap()
                    .data()
                    .len()
                    .try_into()
                    .unwrap_or(u64::MAX)
            };
            let mtime = SystemTime::UNIX_EPOCH;
            let mtime = match e.modified {
                ..0 => mtime - Duration::from_micros(-e.modified as u64),
                0.. => mtime + Duration::from_micros(e.modified as u64),
            };
            let perm = e.permissions;
            let perm = 0o777;
            let ino = self.increase_ref(parent, e.ty, len, key, perm, mtime, e.uid, e.gid);
            let attr = file_attr(ino, e.ty, len, mtime, perm, e.uid, e.gid);
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
        let mut buf = vec![0; size as usize];
        let n = file.data().read(offset as u128, &mut buf).unwrap();
        reply.data(&buf[..n])
    }

    fn readlink(&mut self, _: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        let Some((_, symlink)) = self.get_ino_symlink(ino) else {
            return reply.error(libc::ENOENT);
            //reply.error(libc::ENOTDIR)
        };
        let buf = &mut vec![0; symlink.data().len() as usize];
        symlink.data().read_exact(0, buf).unwrap();
        reply.data(buf)
    }

    fn listxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        if size == 0 {
            reply.size(XATTR_NAME_LIST.len() as u32)
        } else if (size as usize) < XATTR_NAME_LIST.len() {
            reply.error(libc::ERANGE)
        } else {
            reply.data(XATTR_NAME_LIST)
        }
    }

    fn getxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        match name.as_encoded_bytes() {
            self::XATTR_NAME_HASH_TOA => {
                let Some(x) = self.get_ino(ino) else {
                    return reply.error(libc::ENOENT);
                };
                match size {
                    0 => reply.size(64),
                    ..64 => reply.error(libc::ERANGE),
                    64.. => reply.data(&x.key.to_hex()),
                }
            }
            _ => reply.error(libc::ENODATA),
        }
    }
}

fn file_attr(
    ino: u64,
    ty: DirItemType,
    len: u64,
    mtime: SystemTime,
    perm: u16,
    uid: u32,
    gid: u32,
) -> fuser::FileAttr {
    let kind = match ty {
        DirItemType::File => fuser::FileType::RegularFile,
        DirItemType::Dir => fuser::FileType::Directory,
        DirItemType::SymLink => fuser::FileType::Symlink,
        DirItemType::Unknown { .. } => todo!(),
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
    format!("usage: {procname} <pack> <mount> [--allow-other]").into()
}

fn new_reader(pack: &str) -> Result<(Toa, Meta)> {
    Toa::new(pack).map_err(|e| format!("failed to open pack {pack:?}: {e}").into())
}

fn start() -> Result<()> {
    env_logger::init();

    let mut allow_other = false;

    let mut args = std::env::args();
    let procname = args.next();
    let procname = procname.as_deref().unwrap_or("toa-fuse");

    let pack = args.next().ok_or_else(|| usage(procname))?;
    let mount = args.next().ok_or_else(|| usage(procname))?;
    while let Some(x) = args.next() {
        match &*x {
            "--allow-other" => allow_other = true,
            _ => return Err(usage(procname)),
        }
    }

    let (dev, meta) = new_reader(&pack)?;
    let root_key = meta
        .map
        .get("unix.root")
        .unwrap()
        .ok_or("\"unix.root\" not present in meta table")?
        .as_ref()
        .try_into()
        .map(Hash::from_bytes)
        .map_err(|_| "\"unix.root\" value is not 32 bytes")?;
    let fs = Fs {
        dev,
        root: Node {
            key: root_key,
            ty: DirItemType::Dir,
            len: 0, // "directory size" is a meaningless metric on UNIX so don't even bother
            parent_ino: 0,
            refcount: 1,
            uid: 0,
            gid: 0,
            mtime: SystemTime::UNIX_EPOCH,
            perm: 0o555,
        },
        nodes: Default::default(),
        nodes_rev: Default::default(),
        ino_counter: 2,
    };
    let mut opt = vec![
        fuser::MountOption::FSName("toa".into()),
        //fuser::MountOption::AutoUnmount,
        fuser::MountOption::DefaultPermissions,
        fuser::MountOption::NoDev,
        fuser::MountOption::Suid,
        fuser::MountOption::RO,
        fuser::MountOption::Exec,
        fuser::MountOption::NoAtime,
        fuser::MountOption::Sync, // TODO not correct? Should be async (eventually)?
    ];
    if allow_other {
        opt.push(fuser::MountOption::AllowOther);
    }
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
