# don web UI

React + TypeScript, built with Vite, embedded into the `don` binary from
`web/dist` (see `src/web/assets.rs`).

## Why `dist/` is committed

`don` is published to crates.io and built by cargo-dist. Neither will run
`npm`, and neither can be asked to — so the built bundle has to be in the
source tree, not produced at build time.

That means **`web/dist` is checked in, and it must be rebuilt and committed
whenever anything under `web/src` changes.** CI enforces this: it runs the
build and fails if `git diff --exit-code web/dist` shows drift.

Output file names are deterministic (`assets/app.js`, `assets/app.css`) rather
than content-hashed, so the committed bundle only changes when the code does.

## Working on it

```sh
npm ci

# Point at a running don and get hot reload. The dev server proxies /api,
# so you need a daemon (or `don start --with-ui`) running.
don daemon &
don ui --print              # copy the token out of the URL
npm run dev                 # then open the dev server URL with ?token=…

# Override the proxy target when using --with-ui or a non-default port:
DON_UI_TARGET=http://127.0.0.1:3667 npm run dev
```

In debug builds `rust-embed` reads `web/dist` from disk, so
`npm run build` alone is enough to see changes in a `cargo run` binary — no
Rust rebuild needed.

## Before committing

```sh
npm run build               # typechecks, then bundles into dist/
git add web/dist
```
