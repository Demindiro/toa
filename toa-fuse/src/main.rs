use std::{
    collections::{BTreeMap, btree_map},
    error::Error,
    ffi::OsStr,
    fs,
    io::Read,
    ops,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use toa::Hash;
use toa_blob::{BlobStore, FileBlocks};

const XATTR_NAME_LIST: &[u8] = b"user.hash.toa\0";
const XATTR_NAME_HASH_TOA: &[u8] = b"user.hash.toa";

type Result<T> = core::result::Result<T, Box<dyn Error>>;
type Store = BlobStore<FileBlocks>;
type Accel = toa::accel::sled::Db;
type InnerToa = toa::Toa<Store, Accel>;
type Object<'a> = toa::Object<'a, Store, Accel>;
type Data<'a> = toa::Data<'a, Store, Accel>;
type Refs<'a> = toa::Refs<'a, Store, Accel>;

struct Toa {
    inner: InnerToa,
    meta: BTreeMap<Box<str>, Hash>,
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
    key: Hash,
}

struct Dir<'a> {
    data: Vec<u8>,
    refs: Refs<'a>,
    index: u32,
    name_offset: u32,
}

impl Toa {
    fn new(path: &Path, accel: &Path) -> Result<Self> {
        let inner = {
            let mut hdr = [0; 32];
            let dev = fs::OpenOptions::new().read(true).open(path)?;
            (&dev).read_exact(&mut hdr)?;
            let hdr = toa_blob::snoop_header(hdr).unwrap();
            let blk = match hdr.block_size {
                512 => toa_blob::BlockShift::N9,
                4096 => toa_blob::BlockShift::N12,
                x => todo!("block size {x}"),
            };
            let dev = FileBlocks::wrap(blk, hdr.zone_blocks, hdr.zone_count, dev);
            let store = BlobStore::load(dev)?;
            let accel = toa::accel::sled::open(accel)?;
            toa::Toa::load(store, accel)?.ok_or("no Toa store initialized")?
        };
        let mut meta = BTreeMap::default();

        let root = inner.root();

        let refs = inner
            .get(&root)
            .map_err(|e| format!("failed to get root from store: {e:?}"))?
            .ok_or("root is missing from store")?;
        let Object::Refs(refs) = refs else { todo!() };

        let data = {
            let Ok([data]) = refs.read_array(0) else {
                todo!()
            };
            let Ok(Some(data)) = inner.get(&data) else {
                todo!()
            };
            let Object::Data(data) = data else { todo!() };
            let mut b = vec![0; data.len()? as usize];
            data.read_exact(0, &mut b)
                .map_err(|e| format!("root: failed to read data: {e:?}"))?;
            b
        };
        let mut offset = 0;
        for i in 1..refs.len()? {
            let kl = usize::from(data[offset]);
            offset += 1;
            let k = &data[offset..][..kl];
            let k = core::str::from_utf8(k).unwrap();
            offset += kl;
            let [v] = refs
                .read_array(i)
                .map_err(|e| format!("root: failed to read ref: {e:?}"))?;
            meta.insert(k.into(), v);
        }

        Ok(Self { inner, meta })
    }

    fn get(&self, key: &Hash) -> Result<Object<'_>> {
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

    fn get_ino_dir(&self, ino: u64) -> Option<(&Node, Refs<'_>)> {
        self.get_ino(ino)
            .and_then(|x| self.dev.get(&x.key).unwrap().into_refs().map(|y| (x, y)))
    }

    fn get_ino_file(&self, ino: u64) -> Option<(&Node, Data<'_>)> {
        self.get_ino(ino)
            .and_then(|x| self.dev.get(&x.key).unwrap().into_data().map(|y| (x, y)))
    }

    /// # Returns
    ///
    /// The current (or new) inode number of the object.
    fn increase_ref(&mut self, parent_ino: u64, key: Hash) -> u64 {
        let ino = *self.nodes_rev.entry(key).or_insert_with(|| {
            let ino = self.ino_counter;
            self.ino_counter += 1;
            ino
        });
        let node = self.nodes.entry(ino).or_insert_with(|| Node {
            parent_ino,
            key,
            refcount: 0,
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

    fn get_ty(&self, key: &Hash) -> fuser::FileType {
        match self.dev.get(key).unwrap() {
            Object::Data(_) => fuser::FileType::RegularFile,
            Object::Refs(_) => fuser::FileType::Directory,
        }
    }

    fn get_len_ty(&self, key: &Hash) -> (u128, fuser::FileType) {
        match self.dev.get(key).unwrap() {
            Object::Data(x) => (x.len().unwrap(), fuser::FileType::RegularFile),
            Object::Refs(x) => (x.len().unwrap(), fuser::FileType::Directory),
        }
    }

    fn open_dir<'a>(&'a self, refs: Refs<'a>) -> Dir<'a> {
        let [data] = refs.read_array(0).unwrap();
        let data = self.dev.get(&data).unwrap();
        let Object::Data(data) = data else { todo!() };

        let mut names = vec![0; data.len().unwrap().try_into().unwrap()];
        data.read_exact(0, &mut names).unwrap();

        Dir {
            data: names,
            refs,
            name_offset: 0,
            index: 0,
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
        let (len, ty) = self.get_len_ty(&node.key);
        let attr = file_attr(ino, ty, len as u64);
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

        let mut dir = self.open_dir(dir);

        // encode offset in two parts: index and name offset
        // this limits us to 4 billion entries, i.e. plenty
        let n;
        (n, dir.name_offset) = (offset as u32, (offset >> 32) as u32);

        for i in n.. {
            let end = match i {
                0 => reply.add(ino, 1, fuser::FileType::Directory, "."),
                1 => reply.add(node.parent_ino, 2, fuser::FileType::Directory, ".."),
                2.. => {
                    dir.index = i - 2;
                    let Some(key) = dir.get_key() else {
                        break;
                    };
                    let name = dir.get_name();
                    let ty = self.get_ty(&key);
                    let offset = i64::from(2 + dir.index + 1)
                        | i64::from(dir.name_offset + 1 + name.len() as u32) << 32;
                    let end = reply.add(u64::MAX, offset, ty, name);
                    dir.next();
                    end
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

        let mut dir = self.open_dir(dir);
        while let Some(key) = dir.get_key() {
            let eq_name = name == dir.get_name();
            dir.next();
            if !eq_name {
                continue;
            }
            let (len, ty) = self.get_len_ty(&key);
            let len = len.try_into().unwrap_or(u64::MAX);
            let ino = self.increase_ref(parent, key);
            let attr = file_attr(ino, ty, len);
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
        let n = file.read(offset as u128, &mut buf).unwrap();
        reply.data(&buf[..n])
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

impl<'a> Dir<'a> {
    fn get_key(&self) -> Option<Hash> {
        let e = &mut [Hash::default()];
        match self.refs.read((self.index + 1).into(), e).unwrap() {
            0 => None,
            _ => Some(e[0]),
        }
    }

    fn get_name(&self) -> &str {
        let name_len = self.data[self.name_offset as usize];
        let name = &self.data[(1 + self.name_offset) as usize..][..usize::from(name_len)];
        core::str::from_utf8(name).unwrap()
    }

    fn next(&mut self) {
        let name_len = self.data[self.name_offset as usize];
        self.index += 1;
        self.name_offset += 1 + u32::from(name_len);
    }
}

fn file_attr(ino: u64, kind: fuser::FileType, len: u64) -> fuser::FileAttr {
    fuser::FileAttr {
        ino,
        size: len,
        blocks: (len + 511) / 512,
        atime: SystemTime::UNIX_EPOCH,
        mtime: SystemTime::UNIX_EPOCH,
        ctime: SystemTime::UNIX_EPOCH,
        crtime: SystemTime::UNIX_EPOCH,
        kind,
        perm: 0o777,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 1,
        flags: Default::default(),
    }
}

fn usage(procname: &str) -> Box<dyn Error> {
    format!("usage: {procname} <store> <accel> <name> <mount> [--allow-other]").into()
}

fn start() -> Result<()> {
    env_logger::init();

    let mut allow_other = false;

    let mut args = std::env::args_os();
    let procname = args.next().map(|x| x.to_string_lossy().into_owned());
    let procname = procname.as_deref().unwrap_or("toa-fuse");

    let store = args.next().ok_or_else(|| usage(procname))?;
    let accel = args.next().ok_or_else(|| usage(procname))?;
    let name = args.next().ok_or_else(|| usage(procname))?;
    let mount = args.next().ok_or_else(|| usage(procname))?;
    while let Some(x) = args.next() {
        match x.to_str() {
            Some("--allow-other") => allow_other = true,
            _ => return Err(usage(procname)),
        }
    }

    let name = name
        .into_string()
        .map_err(|e| format!("name {e:?} is not valid UTF-8"))?;

    let store = PathBuf::from(store);
    let accel = PathBuf::from(accel);
    let dev =
        Toa::new(&store, &accel).map_err(|e| format!("failed to open store {store:?}: {e}"))?;
    let root_key = *dev
        .meta
        .get(&*name)
        .ok_or("{name:?} not present in meta table")?;
    let fs = Fs {
        dev,
        root: Node {
            key: root_key,
            parent_ino: 0,
            refcount: 1,
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
    fuser::mount2(fs, mount, &opt).map_err(|e| format!("failed to mount store: {e}"))?;
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
