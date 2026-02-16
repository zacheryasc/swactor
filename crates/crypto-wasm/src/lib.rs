#![no_std]

use core::ptr::addr_of_mut;
use ed25519_dalek::{SigningKey, Signer};

static mut BUF: [u8; 8192] = [0u8; 8192];

#[no_mangle]
pub extern "C" fn buffer_ptr() -> *const u8 {
    addr_of_mut!(BUF).cast()
}

/// Read seed from BUF[0..32], write public key to BUF[32..64]
#[no_mangle]
pub extern "C" fn get_public_key() {
    unsafe {
        let buf = &mut *addr_of_mut!(BUF);
        let seed: [u8; 32] = buf[0..32].try_into().unwrap_unchecked();
        let sk = SigningKey::from_bytes(&seed);
        buf[32..64].copy_from_slice(sk.verifying_key().as_bytes());
    }
}

/// Read seed from BUF[0..32], message from BUF[128..128+msg_len].
/// Write 64-byte signature to BUF[64..128].
#[no_mangle]
pub extern "C" fn ed25519_sign(msg_len: usize) {
    unsafe {
        let buf = &mut *addr_of_mut!(BUF);
        let seed: [u8; 32] = buf[0..32].try_into().unwrap_unchecked();
        let msg = &buf[128..128 + msg_len];
        let sk = SigningKey::from_bytes(&seed);
        let sig = sk.sign(msg);
        buf[64..128].copy_from_slice(&sig.to_bytes());
    }
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}
