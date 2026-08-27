//! Test/E2E support binary: a minimal guest that claims the inherited
//! bootstrap channel and acknowledges or fails attachment on demand. Built as
//! a package bin so integration tests can locate it via
//! `CARGO_BIN_EXE_context_guest_probe`; not part of the library surface.

use data_plane::bootstrap::channel::GuestBootstrap;

fn main() {
    let guest = match GuestBootstrap::claim() {
        Ok(guest) => guest,
        Err(error) => {
            eprintln!("bootstrap: {error}");
            std::process::exit(2);
        }
    };
    let (_material, _arena_fd, attachment) = match guest.into_parts() {
        Ok(parts) => parts,
        Err(error) => {
            eprintln!("bootstrap parts: {error}");
            std::process::exit(2);
        }
    };
    let notification = if std::env::args().any(|argument| argument == "--fail-attachment") {
        attachment.begin_attachment_failed("probe attachment rejected")
    } else {
        attachment.begin_attachment_succeeded()
    };
    if let Err(error) = notification.and_then(|notification| notification.wait()) {
        eprintln!("attachment result: {error}");
        std::process::exit(4);
    }
    if std::env::args().any(|argument| argument == "--fail-attachment") {
        std::process::exit(3);
    }
    println!("probe context ready");
}
