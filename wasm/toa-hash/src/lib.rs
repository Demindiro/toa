#![no_std]

use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use toa_hash::{Domain, Hash, TreeHasher};

#[repr(transparent)]
struct UnsafeSyncCell<T>(UnsafeCell<T>);

unsafe impl<T> Sync for UnsafeSyncCell<T> {}

#[unsafe(no_mangle)]
static HASH: UnsafeSyncCell<Hash> = UnsafeSyncCell(UnsafeCell::new(Hash::from_bytes([0; 32])));

static HASHER: UnsafeSyncCell<MaybeUninit<TreeHasher>> =
    UnsafeSyncCell(UnsafeCell::new(MaybeUninit::uninit()));

#[unsafe(no_mangle)]
pub extern "C" fn begin(domain: i32) {
    let domain = match domain {
        1 => Domain::Data,
        2 => Domain::Refs,
        x => panic!("invalid domain {x}"),
    };
    unsafe { (*HASHER.0.get()).write(TreeHasher::new(domain)) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn update(base: *const u8, len: usize) {
    unsafe {
        let data = core::slice::from_raw_parts(base, len);
        with_hasher(|x| x.update(data));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn end() {
    unsafe {
        with_hasher(|x| *HASH.0.get() = x.clone().finalize());
    }
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

unsafe fn with_hasher<F>(f: F)
where
    F: FnOnce(&mut TreeHasher)
{
    unsafe { (f)((*HASHER.0.get()).assume_init_mut()) }
}
