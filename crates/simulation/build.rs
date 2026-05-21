//! Derive the runtime surface descriptor from the trait declarations
//! in `src/runtime/mod.rs`.
//!
//! Parsing the source with `syn` at build time means the descriptor
//! cannot drift: a signature edit that does not also touch
//! `surface.lock` fails `tests/surface_locked.rs`.
//!
//! Output goes to `$OUT_DIR/surface_descriptor.txt` and is consumed by
//! `src/runtime/mod.rs` via `include_str!`.

use std::env;
use std::fs;
use std::path::PathBuf;

use quote::ToTokens;

const RUNTIME_SRC: &str = "src/runtime/mod.rs";

fn main() {
    println!("cargo:rerun-if-changed={RUNTIME_SRC}");
    println!("cargo:rerun-if-changed=build.rs");

    let src = fs::read_to_string(RUNTIME_SRC).expect("read runtime source");
    let file = syn::parse_file(&src).expect("parse runtime source");

    let descriptor = build_descriptor(&file);

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR not set"));
    fs::write(out_dir.join("surface_descriptor.txt"), descriptor)
        .expect("write surface_descriptor.txt");
}

fn build_descriptor(file: &syn::File) -> String {
    let mut out = String::new();
    out.push_str("runtime surface descriptor v1\n");

    let traits_mod = file
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Mod(m) if m.ident == "traits" => Some(m),
            _ => None,
        })
        .expect("`pub mod traits` not found in runtime source");

    let (_, items) = traits_mod
        .content
        .as_ref()
        .expect("`pub mod traits` is declaration-only; expected inline body");

    for item in items {
        if let syn::Item::Trait(t) = item {
            out.push_str("trait ");
            out.push_str(&t.ident.to_string());
            out.push('\n');

            for trait_item in &t.items {
                if let syn::TraitItem::Fn(f) = trait_item {
                    let sig = normalize_whitespace(&f.sig.to_token_stream().to_string());
                    out.push_str("  ");
                    out.push_str(&sig);
                    out.push('\n');
                }
            }
        }
    }

    out
}

fn normalize_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
