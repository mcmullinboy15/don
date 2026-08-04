//! Tell cargo when to rebuild.
//!
//! `rust-embed` bakes `web/dist` into the binary for release builds, but it
//! does that in a proc macro — cargo has no idea the directory is an input.
//! Without this, rebuilding the frontend and then `cargo build --release`
//! silently keeps the *previous* bundle, which is the kind of bug you chase
//! in the browser for twenty minutes before suspecting the build.

fn main() {
    println!("cargo:rerun-if-changed=web/dist");

    // `rust-embed` fails to *compile* when its folder is absent, and web/dist
    // is a gitignored build artifact — so a fresh clone hasn't got one and
    // `cargo build` would fail before it could explain why. Create it empty;
    // `web::assets::missing_bundle` then explains the situation at runtime.
    //
    // A committed `.gitkeep` can't do this job: Vite's `emptyOutDir` wipes the
    // directory on every build, taking the placeholder with it.
    //
    // Ignoring the error is deliberate — if the directory already exists (the
    // published crate ships one) this is a no-op, and if it can't be created
    // the compile fails anyway with rust-embed's own message.
    let _ = std::fs::create_dir_all("web/dist");
}
