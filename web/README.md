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

The dev server gives you hot reload against a real stack. It serves the UI
itself and proxies `/api` to a running don, so you need a daemon (or
`don start --with-ui`) up first.

```sh
npm ci

# 1. Something for the UI to talk to.
don daemon &
cd ~/some-project && don start -d

# 2. Hot reload.
npm run dev                          # http://localhost:5173

# Pointing at --with-ui, or a daemon on a non-default port:
DON_UI_TARGET=http://127.0.0.1:3667 npm run dev
```

There's nothing to authenticate — the UI serves anything that can reach the
port, on the grounds that reaching it already means running on this machine.

`DON_UI_TARGET` matters more than it used to: under `don start` the daemon is
behind a Don proxy on a port that may not be 3666, so take the address from
`don ports` rather than assuming.

## Skipping the dev server

In debug builds `rust-embed` reads `web/dist` from disk, so you can also just
rebuild the bundle and reload — no Rust rebuild and no proxy, but no hot
reload either:

```sh
npm run build && don ui
```

## Before committing

```sh
npm run build               # typechecks, then bundles into dist/
git add web/dist
```
