use std::path::Path;
use std::process::Command;

fn main() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let guests_dir = Path::new(manifest_dir).join("tests/guests");

    for guest in &["echo", "double", "silent"] {
        let guest_dir = guests_dir.join(guest);

        println!(
            "cargo:rerun-if-changed={}",
            guest_dir.join("src/lib.rs").display()
        );
        println!(
            "cargo:rerun-if-changed={}",
            guest_dir.join("Cargo.toml").display()
        );

        let status = Command::new("cargo")
            .args(["build", "--target", "wasm32-unknown-unknown", "--release"])
            .current_dir(&guest_dir)
            .status()
            .unwrap_or_else(|e| panic!("failed to run cargo build for {guest} guest: {e}"));

        assert!(status.success(), "failed to build {guest} guest");
    }
}
