#![no_std]

use core::cell::UnsafeCell;
use core::panic::PanicInfo;

// --- bump allocator ---
const HEAP_SIZE: usize = 65536;

struct BumpAlloc {
    heap: UnsafeCell<[u8; HEAP_SIZE]>,
    offset: UnsafeCell<usize>,
}

unsafe impl Sync for BumpAlloc {}

static ALLOC: BumpAlloc = BumpAlloc {
    heap: UnsafeCell::new([0u8; HEAP_SIZE]),
    offset: UnsafeCell::new(0),
};

#[unsafe(no_mangle)]
pub extern "C" fn alloc(size: i32) -> i32 {
    unsafe {
        let offset = &mut *ALLOC.offset.get();
        let heap = &mut *ALLOC.heap.get();
        let align = 8;
        let start = (*offset + align - 1) & !(align - 1);
        let end = start + size as usize;
        if end > heap.len() {
            return 0; // OOM
        }
        *offset = end;
        heap.as_ptr().add(start) as i32
    }
}

// --- host import ---
#[link(wasm_import_module = "swactor")]
unsafe extern "C" {
    #[link_name = "send"]
    fn host_send(dest_ptr: i32, payload_ptr: i32, payload_len: i32);
}

/// Message format: first 32 bytes = destination address, rest = payload.
/// Echo sends the payload portion back to the specified destination.
#[unsafe(no_mangle)]
pub extern "C" fn handle(ptr: i32, len: i32) {
    if len < 32 {
        return;
    }
    let dest_ptr = ptr;
    let payload_ptr = ptr + 32;
    let payload_len = len - 32;
    unsafe {
        host_send(dest_ptr, payload_ptr, payload_len);
    }
}

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}
