use std::{
    error::Error,
    fs,
    io::Read,
    net::{SocketAddr, UdpSocket},
    path::Path,
};
use toa::Hash;
use toa_blob::{BlobStore, FileBlocks};

mod cmd {
    pub const STATUS: u8 = 1;
    // Get the chunk if at or below 8KiB or the key and length of the root node.
    //
    // A path of 32-bit indices may follow immediately after the key.
    // Requires each path component to be a refs object.
    pub const FETCH: u8 = 2;
    // Get the chunk at an offset associated with a key.
    //
    // Offsets are in terms of chunks, with each offset being a `u112` (14 bytes).
    pub const CHUNK: u8 = 3;
}

mod result {
    pub const OK: u8 = 0;
}

mod ty {
    pub const IS_PARENT: u8 = 1 << 0;
    pub const IS_REFS: u8 = 1 << 1;
    pub const IS_VALID: u8 = 1 << 2;
}

type Result<T> = core::result::Result<T, Box<dyn Error>>;
type Store = BlobStore<FileBlocks>;
type Accel = toa::accel::sled::Db;
type Toa = toa::Toa<Store, Accel>;

struct Server {
    toa: Toa,
    buf: Box<[u8; 4 + 8192]>,
}

struct Request<'a> {
    server: &'a mut Server,
    socket: &'a UdpSocket,
    packet_len: usize,
    addr: SocketAddr,
}

macro_rules! trace {
    ($($arg:tt)*) => {
        if true || cfg!(feature = "trace") {
            eprintln!($($arg)*);
        }
    };
}

impl Server {
    fn handle_socket(&mut self, socket: &mut UdpSocket) -> Result<()> {
        let (packet_len, addr) = socket.recv_from(&mut *self.buf).unwrap();
        Request {
            server: self,
            socket,
            packet_len,
            addr,
        }
        .handle()
    }
}

impl Request<'_> {
    fn handle(&mut self) -> Result<()> {
        if self.packet_len < 4 {
            trace!("packet too short (packet_len = {})", self.packet_len);
            return Ok(());
        }
        let [cmd, _, _, _, data @ ..] = &mut *self.server.buf;
        let data = &mut data[..self.packet_len - 4];
        trace!("<-  {} | {data:x?}", self.addr);
        match *cmd {
            cmd::STATUS => {
                let root = self.server.toa.root();
                let data = &mut self.server.buf[..36];
                data[0] = result::OK;
                data[4..36].copy_from_slice(root.as_bytes());
                self.send(32)?;
            }
            cmd::FETCH => {
                let (hash, data) = data.split_first_chunk::<32>().unwrap();
                let (path, data) = data.as_chunks::<4>();
                assert!(data.is_empty(), "trailing garbage {data:?}");
                let hash = Hash::from_bytes(*hash);
                let path = path.iter().map(|x| u32::from_le_bytes(*x));
                // TODO try to avoid clone
                let path = path.collect::<Vec<_>>().into_iter();
                let path = path.map(u128::from);
                self.handle_fetch(&hash, path)?;
            }
            cmd::CHUNK => {
                let (hash, data) = data.split_first_chunk::<32>().unwrap();
                let (offset, data) = data.split_first_chunk::<14>().unwrap();
                assert!(data.is_empty(), "trailing garbage {data:?}");
                let hash = Hash::from_bytes(*hash);
                let mut offt = [0; 16];
                offt[..14].copy_from_slice(offset);
                let offt = u128::from_le_bytes(offt);
                self.handle_chunk(hash, offt)?;
            }
            n => todo!("invalid command {n}"),
        }
        Ok(())
    }

    fn send_node(&mut self, hash: &Hash) -> Result<()> {
        trace!("fetch {hash}");
        let [ty, _, _, _, out @ ..] = &mut *self.server.buf;
        let Some(obj) = self.server.toa.get(hash).unwrap() else {
            return self.send_error("");
        };
        assert!(out.len() == 8192); // TODO wrap in const {  }
        let serialize_parent = |len_bits: u128, ty: &mut u8, out: &mut [_], is_refs| {
            *ty = ty::IS_VALID | (u8::from(is_refs) * ty::IS_REFS) | ty::IS_PARENT;
            out[..32].copy_from_slice(hash.as_bytes());
            out[32..48].copy_from_slice(&len_bits.to_le_bytes());
            48
        };
        let serialize_chunk = |ty: &mut u8, is_refs, len| {
            *ty = ty::IS_VALID | (u8::from(is_refs) * ty::IS_REFS);
            len
        };
        let n = match obj {
            toa::Object::Data(x) => match x.read_node(out).unwrap() {
                toa::DataNode::Chunk { len } => serialize_chunk(ty, false, len),
                toa::DataNode::Pair { pair } => serialize_parent(pair.len_bits(), ty, out, false),
            },
            toa::Object::Refs([head, tail]) => {
                *ty = ty::IS_VALID | ty::IS_REFS;
                out[00..32].copy_from_slice(head.as_bytes());
                out[32..64].copy_from_slice(tail.as_bytes());
                64
            }
        };
        self.send(n)
    }

    fn handle_chunk(&mut self, key: Hash, offset: u128) -> Result<()> {
        trace!("chunk {key} {offset}");
        let [ty, _, _, _, out @ ..] = &mut *self.server.buf;
        let Some(obj) = self.server.toa.get(&key).unwrap() else {
            return self.send_error("");
        };
        assert!(out.len() == 8192); // TODO wrap in const {  }
        match obj {
            toa::Object::Data(x) => {
                *ty = ty::IS_VALID;
                let n = x.read(offset << 13, out).unwrap();
                self.send(n)
            }
            toa::Object::Refs(x) => {
                todo!();
            }
        }
    }

    fn handle_fetch<I>(&mut self, start: &Hash, path: I) -> Result<()>
    where
        I: IntoIterator<Item = u128>,
    {
        trace!("fetch {start}");
        let [ty, _, _, _, out @ ..] = &mut *self.server.buf;
        assert!(out.len() == 8192); // TODO wrap in const {  }

        let mut key = *start;
        for x in path {
            trace!("  /{x}");
            let Some(obj) = self.server.toa.get(&key).unwrap() else {
                return self.send_error("");
            };
            match obj {
                toa::Object::Data(_) => return self.send_error(""),
                toa::Object::Refs(y) => key = y[x as usize],
            }
        }

        self.send_node(&key)
    }

    fn send(&mut self, n: usize) -> Result<()> {
        self.socket.send_to(&self.server.buf[..4 + n], self.addr)?;
        Ok(())
    }

    fn send_error(&mut self, msg: &str) -> Result<()> {
        assert!(msg.is_empty(), "todo error msg");
        self.server.buf[0] = 0;
        self.send(0)
    }
}

fn usage(procname: &str) -> Box<dyn Error> {
    let s = format!(
        "\
usage: {procname} <store> <accel> [address...]

address is one of:
    udp4:port        (e.g. 127.0.0.1:1234)
    udp6:port        (e.g. [1234::abcd]:1234"
    );
    s.into()
}

fn load_store(path: &Path, write: bool) -> Result<Store> {
    let mut hdr = [0; 32];
    let dev = fs::OpenOptions::new().read(true).write(write).open(path)?;
    (&dev).read_exact(&mut hdr)?;
    let hdr = toa_blob::snoop_header(hdr).unwrap();
    let blk = match hdr.block_size {
        512 => toa_blob::BlockShift::N9,
        4096 => toa_blob::BlockShift::N12,
        x => todo!("block size {x}"),
    };
    let dev = FileBlocks::wrap(blk, hdr.zone_blocks, hdr.zone_count, dev);
    let store = Store::load(dev)?;
    Ok(store)
}

fn start() -> Result<()> {
    let mut args = std::env::args();
    let procname = args.next();
    let procname = procname.as_deref().unwrap_or("toa-cli");

    let store = args.next().ok_or_else(|| usage(procname))?;
    let accel = args.next().ok_or_else(|| usage(procname))?;

    let mut sockets = Vec::with_capacity(args.len());
    for x in args {
        let x = x
            .parse::<SocketAddr>()
            .map_err(|e| format!("bad address: {e}"))?;
        eprintln!("opening UDP socket on {x}");
        let x = UdpSocket::bind(x).map_err(|e| format!("failed to listen on {x}: {e}"))?;
        sockets.push(x);
    }

    if sockets.is_empty() {
        return Err(usage(procname));
    }

    eprintln!("opening store");
    let store = load_store(&std::path::PathBuf::from(store), true)?;
    eprintln!("loading store");
    let accel = toa::accel::sled::open(accel)?;
    let toa = Toa::load(store, accel)?.unwrap();

    let mut poll = popol::Sources::with_capacity(sockets.len());
    for (i, x) in sockets.iter().enumerate() {
        poll.register(i, x, popol::interest::READ);
    }

    let mut events = Vec::with_capacity(1);
    let mut server = Server {
        toa,
        buf: [0; 4 + 8192].into(),
    };
    eprintln!("starting event loop");
    loop {
        trace!("poll");
        poll.wait(&mut events)?;

        for e in events.drain(..) {
            match server.handle_socket(&mut sockets[e.key]) {
                Ok(()) => {}
                Err(e) => eprintln!("error: {e:?}"),
            }
        }
    }
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
