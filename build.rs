//! Tell cargo when to rebuild.
//!
//! `rust-embed` bakes `web/dist` into the binary for release builds, but it
//! does that in a proc macro — cargo has no idea the directory is an input.
//! Without this, rebuilding the frontend and then `cargo build --release`
//! silently keeps the *previous* bundle, which is the kind of bug you chase
//! in the browser for twenty minutes before suspecting the build.

fn main() {
    println!("cargo:rerun-if-changed=web/dist");
}
