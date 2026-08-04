# don web UI

React + TypeScript, built with Vite, embedded into the `don` binary from
`web/dist` (see `src/web/assets.rs`).

## How the bundle ships

`web/dist` is a build artifact: **gitignored, never committed.** It still has
to reach users, because it's embedded into the binary and `cargo install`
won't run npm. So it's built by CI and travels inside the published artifacts:

| Install path | Bundle? | How |
|---|---|---|
| `cargo install don` | yes | `include` in `Cargo.toml` puts `web/dist/**` in the `.crate` tarball; the publish workflow builds it first |
| Homebrew / install script | yes | the release workflow builds it before `dist build` (see `github-build-setup`) |
| `cargo install --git …` | no | build from a clone — run npm yourself |
| `source.tar.gz` | no | `git archive`, so gitignored files are absent |

For the last two, don serves a page telling you to run the build rather than
a broken UI.

**After cloning, run `npm --prefix web run build` once** or the binary you
build has no UI. `don start` in this repo does it for you (and re-does it on
every frontend save).

Output file names are deterministic (`assets/app.js`, `assets/app.css`) rather
than content-hashed — nothing depends on that any more, but it keeps rebuilds
byte-identical when the source hasn't changed.

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
```

Nothing to add — `dist/` is ignored. The build is worth running anyway,
because it typechecks.
