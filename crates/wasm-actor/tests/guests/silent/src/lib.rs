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

#[unsafe(no_mangle)]
pub extern "C" fn handle(_ptr: i32, _len: i32) {
    // Silent: receive bytes, do nothing
}

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}
