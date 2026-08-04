# Don - Development Guidelines

## Project Overview

Don is a dev environment orchestrator. See `docs/design.md` for the full design document. The crate is both a library (`don`) and a CLI binary — the library exposes all core functionality so other Rust tools can embed it.

## Build & Test

```sh
cargo build          # build
cargo test           # run all tests
cargo clippy         # lint
```

### Running don on don

There's a `don.toml` in this repo, so `don start` here brings up the dev loop:
a dev daemon rebuilt whenever `src/` changes, the Vite dev server proxying to
it, and a throwaway stack (`dev/demo/`) registered with the dev daemon so the
web UI has something to render. Clippy runs on every Rust save and `web/dist`
is rebuilt on every frontend save, which keeps the CI drift check happy.

```sh
don start                  # everything
don start --profile rust   # no npm: just the daemon and clippy
don ports                  # where things actually landed — open the `web` one
don run test               # the suite is manual — it'd fight the cargo lock
```

Several checkouts can run at once. `fallback_ports = true` gives the first
instance the memorable ports (3777 / 5173) and later ones whatever is free, and
the dev daemon's state lives in each checkout's own `.don/dev-daemon` rather
than the XDG location — a daemon installed with `don daemon install` holds an
flock on `daemon.pid` there, so sharing would make the dev daemon refuse to
start whenever the real one is running.

Both services sit behind Don proxies, so the address you opened survives the
daemon restarting under you on every save. That means the daemon's *own* port
is ephemeral and differs from the one you browse to — `don ports` is the source
of truth, and `$(daemon.addr)` is how the Vite service learns it.

For interactive testing of the TUI under realistic load (shutdown, log spam,
hang detection), see [Testing the TUI](#testing-the-tui) — this is the
preferred way to validate any change that touches `src/tui/`, `src/output/`,
or the runner shutdown path.

## Core Principles

### No Panics in Production Code

**No `unwrap()`, `expect()`, or `panic!()` outside of `#[cfg(test)]` blocks.** Don manages child processes and holds PID file locks. A panic means orphaned processes, stale sockets, and ports held hostage. Every fallible operation must use `Result` or `Option` with proper error propagation.

This includes:
- No `unwrap()` on `Option` or `Result` — use `?`, `ok_or()`, `map_err()`, etc.
- No array/slice indexing that could panic — use `.get()` and handle `None`
- No `unreachable!()` in match arms that could theoretically be reached
- `#[cfg(test)]` code and test helper functions may use `unwrap()` freely

### Developer Experience First

Don exists to make devs' lives easier. Every decision should optimize for the person running `don start` at 9am on Monday. Specifically:

- **Error messages must be actionable.** "service 'api': depends on unknown service 'postgre' — did you mean 'postgres'?" is better than "validation failed."
- **Fail fast, fail clearly.** Validate everything before starting anything. Don't start 5 services and then fail on the 6th because of a config typo.
- **Output must be scannable.** Service prefixes, aligned columns, lifecycle events in brackets. A dev glancing at the terminal should immediately know what's running, what failed, and why.
- **Respect the user's terminal.** Clean up on exit. Don't leave the terminal in a weird state. Handle Ctrl+C gracefully. Don't eat their scrollback with unnecessary output.

### Error Handling

Use `thiserror` for error types. Each module should define its own error enum. Errors should carry enough context to produce a good user-facing message.

```rust
// Good
Err(ConfigError::UnknownDependency { service: name.clone(), dependency: dep.clone() })

// Bad
Err("unknown dependency".into())
```

Avoid `anyhow` in the library — it erases types and makes it hard for consumers to match on errors. The binary can use `anyhow` at the top level if needed for convenience.

### Resource Safety

Don manages external resources (child processes, PID files, sockets, docker containers). All resource cleanup must go through structured ownership:

- Use RAII / `Drop` implementations for resources that need cleanup
- PID files must be cleaned up even if the operation that follows fails
- When spawning a child process, the PGID file lock must be acquired *before* the spawn — never after
- Signal handlers must be async-signal-safe — set a flag, let the main loop handle cleanup

### Shutdown Responsiveness

**The runner must stay interruptible at all times.** A user pressing `Ctrl+C` should never be trapped behind a slow build, download, query, or lock wait.

- Do not await long-running external work inline on the runner task unless it is explicitly raced against shutdown.
- Any subprocess awaited from the runner task must be cancellation-safe: dropping the future must stop the underlying work (`kill_on_drop`, abort handle, or equivalent).
- Any network/download future awaited from the runner task must be raced against shutdown so the runner can abandon it immediately.
- If a piece of work cannot be made cancellation-safe, it does not belong on the runner task. Move it to a detached task and communicate back via channels.
- New startup/rebuild/watch paths must answer this question in code review: "what happens if the user hits Ctrl+C right here?"

### Testing

- **Use table-driven tests.** Define a struct for test cases, put them in a `Vec`, iterate. This is the standard pattern in this codebase.
- **Tests may use `unwrap()`.** Panicking in tests is expected behavior.
- **Test the library, not the binary.** Business logic lives in `lib.rs` and its modules. The binary is a thin CLI wrapper.
- **Use temp directories for filesystem tests.** Clean up after yourself. Don't leave test artifacts in the working directory.
- **Integration tests live in `tests/`.** Use the helpers in `tests/helpers/` (TempDir, ConfigBuilder, free_port, run_with_timeout).
- **Tests must work without a real TTY.** PTY allocation can fail in CI/headless environments. Process spawning code must have a pipe-based fallback, and tests must not assume a PTY is available.
- **Every integration test gets a timeout.** Use `run_with_timeout()` to prevent hangs from blocking CI.

### Testing the TUI

Pipe-mode integration tests cover the runner and `OutputManager`, but they
don't exercise ratatui rendering, the inline-viewport DSR setup, the input
task, or the broadcast/log-channel interplay that the TUI loop sits in the
middle of. Several of the worst regressions in this codebase have been
TUI-only — pipe mode fine, TUI hangs / loses lifecycle events / freezes the
status bar under load. **If you touch `src/tui/`, `src/output/`, or the
runner's shutdown path, you need to validate against a real PTY.**

There are three complementary tools in `tools/`:

- **`tools/tui_drive.py`** — runs `don start` under a real PTY (`pty.fork`),
  intercepts and answers DSR cursor queries on the master side (so the TUI
  starts cleanly the way it would in a normal terminal), waits for "all
  services running", sends SIGINT after a configurable linger, watches for
  "shutdown complete", and force-kills + reports HANG if the process
  doesn't exit promptly. Captures the full ANSI byte stream on stdout and
  writes a structured summary to stderr (lifecycle event counts, captured
  byte total, exit code).
- **`tools/gen_stress_config.py`** — generates a synthetic `don.toml` plus
  per-service shell scripts that mirror the shape of a busy monorepo:
  hidden infra services with one of them spamming on SIGTERM, hidden
  consumers with TERM-trap floods, dozens of lazy `app-NN` services with
  `listenfd` proxies. This is the load shape that exposes TUI render-rate
  bugs.
- **`tools/tui_drive_resize.py`** — drives the *resize* path: waits for
  steady state (a large log burst has filled the `LogStore`), then sends a
  sequence of real `SIGWINCH` resizes (`ioctl(TIOCSWINSZ)`), accounts for
  the bytes don writes per resize (a replay-on-resize regression shows up as
  a multi-MB spike), and replays the captured stream into a `pyte` screen to
  detect "ghost" bars left behind. Override the size sequence with
  `DON_RESIZE_SIZES` (e.g. `"50x140,50x100"` for a width-only resize, which
  must preserve on-screen logs and emit no `\x1b[2J`). Requires `pyte` for
  the ghost check.

The standard workflow:

```sh
# Build (release — debug is too slow to expose perf issues at scale).
cargo build --release

# Generate a stress config in a scratch dir.
python3 tools/gen_stress_config.py /tmp/don-stress

# Drive don in TUI mode, send Ctrl+C 4s after steady-state.
rm -rf /tmp/don-stress/.don
python3 tools/tui_drive.py target/release/don /tmp/don-stress 4 \
    > /tmp/tui-stdout.bin 2> /tmp/tui-stderr.log

# Healthy run: every running (non-lazy) service shows
# `: stopping`, `send SIGTERM`, and `: stopped`; "shutdown complete"
# appears; exit code is 0; total runtime well under 10 s.
tail -15 /tmp/tui-stderr.log
```

Things to look for in the stderr summary:

- `saw shutdown: True` — `shutdown complete` reached the TUI.
- `lifecycle 'stopping' / 'send SIGTERM' / 'stopped' events: N` — should
  equal the number of running (non-lazy) services. A drop here means
  lifecycle events were dropped or the runner skipped its shutdown loop.
- `don exit code: 0` — clean exit. `None` + a `HANG` line means the
  driver had to SIGKILL.
- `captured bytes` — useful regression signal. A jump from tens of KB to
  multiple MB without a config change means the TUI started rendering
  log lines that should have stayed filtered.

To exercise even harder, edit `tools/gen_stress_config.py` and bump the
spam loop counts inside the noisy services' TERM traps. The current shape
is calibrated to surface the render-rate bug we fixed; if you're chasing
something different (lots of state events, deep dep chains, etc.) tweak
the shape rather than reaching for a one-off shell script.

When TUI behavior changes, this is the test of record. A `cargo test`
green run is necessary but not sufficient — also run the driver and
include before/after stderr summaries in the change description.

### Code Organization

```
src/
  lib.rs                    # library root — re-exports public API
  main.rs                   # CLI binary — thin wrapper around the library
  duration.rs               # human-readable duration string parsing ("200ms", "1s", "5m")
  config/
    mod.rs                  # Config struct, parsing, validation, FromStr, Levenshtein typo suggestions
    diff.rs                 # config diffing for live reload (added/removed/changed detection)
    service.rs              # Service, ServiceOverride, ResolvedService, presets
    task.rs                 # Task config
    profile.rs              # Profile config
    platform.rs             # Platform enum, deserialization
    download.rs             # DownloadConfig, PlatformDownload, cache paths
    types.rs                # Shared types: Command, ReadyCheck, ShutdownConfig, LogConfig
  runner/
    mod.rs                  # orchestrator — dependency graph, startup/shutdown, config reload, profiles
    service.rs              # service lifecycle: spawn, restart, stop
    task.rs                 # task execution, timeout, skip-if-unchanged
  process/
    mod.rs                  # process group management, PTY spawning, identity tracking
    pid_file.rs             # PID file locking (flock-based) for single-instance guard
    identity.rs             # (pgid, start_time) capture for crash-recovery identity checks
    cleanup.rs              # stale state detection and cleanup (pid files, sockets, docker)
    env.rs                  # .env file parsing, env merging
    socket.rs               # LISTEN_FDS socket binding and fd passing
  watch/
    mod.rs                  # file watching, debounce, change-during-build state machine, config reload
  output/
    mod.rs                  # line buffering, service name prefixing, color assignment, sink management
    ring_buffer.rs          # bounded per-service output buffer
    sanitize.rs             # ANSI escape sequence filtering (strip cursor/screen, keep colors)
  client/
    mod.rs                  # HTTP-over-unix-socket client for CLI ↔ daemon communication
  server/
    mod.rs                  # unix socket HTTP API (axum over hyper-util)
    routes.rs               # API endpoints: status, start, stop, restart, logs (incl. follow)
  docker/
    mod.rs                  # Docker service lifecycle via bollard API
    build.rs                # Docker image building (tar context, streamed output)
    parse.rs                # Port mapping, env merging for docker
    stream.rs               # DockerLogReader: AsyncRead adapter over bollard log stream
  download.rs               # artifact downloading, SHA-256 verification, archive extraction, caching
  task_state.rs             # task file hash tracking for skip detection
```

Don't be afraid to create directories and nested modules. A flat list of 15 files in `src/` is harder to navigate than a well-organized tree. Group related functionality into directories with a `mod.rs` that exposes only what the rest of the crate needs. Each file should be small enough to read in one sitting.

### Modularity & API Surface

Lean hard towards small, focused modules. Each file should do one thing. If a file is getting long, split it. If a struct has methods that serve two different concerns, those concerns probably belong in separate modules.

**Public API discipline:**

- Default to `pub(crate)`. Only make something `pub` if it's part of the library's external API — something another Rust crate consuming `don` would need.
- Every `pub` item must have a doc comment explaining what it does, when to use it, and any important invariants.
- Re-export the public API from `lib.rs` so consumers get a clean `don::Config`, `don::TaskState`, etc. without reaching into submodules.
- Internal helpers, intermediate types, and implementation details stay `pub(crate)` or private.

**Module boundaries:**

- Modules should communicate through well-defined types, not by reaching into each other's internals.
- If module A needs something from module B, B should expose a clean function or type for it — not have A poke at B's fields directly.
- Prefer passing values and references over shared mutable state. When shared state is unavoidable, contain it in one module that owns the state and exposes an API for it.

### Cross-Module Communication

Modules communicate via **tokio channels**, not shared mutable state:

- **`mpsc`** for commands into the runner (CLI/API -> runner). The runner owns an `mpsc::Receiver<RunnerCommand>` and processes commands sequentially. This gives it a clean command loop with no shared mutable state.
- **`broadcast`** for events out of the runner (runner -> output/API/watch). Service state changes (started, ready, stopped, failed) are broadcast so multiple consumers can observe without coupling.
- **`oneshot`** for request/reply (e.g., status queries). The API sends a command with a `oneshot::Sender` for the reply, the runner fills it.

**No `Arc<Mutex<_>>` for shared state.** The runner owns all service state in a plain `HashMap<String, ServiceState>`. Status queries go through the command channel. This avoids deadlocks and contention.

```rust
// The actual command enum (see runner/mod.rs for the full definition)
enum RunnerCommand {
    Start { name: String, reply: oneshot::Sender<CommandResult> },
    Stop { name: String, reply: oneshot::Sender<CommandResult> },
    Restart { name: String, reply: oneshot::Sender<CommandResult> },
    Rebuild { name: String },           // file watch triggered
    TaskRerun { name: String },         // file watch triggered
    Status { reply: oneshot::Sender<Vec<ItemStatus>> },
    Logs { name: String, last_n: usize, reply: oneshot::Sender<Option<String>> },
    StartPending,                       // deferred retry for unsatisfied deps
    Shutdown,
}
```

### Async

The runtime is tokio. All I/O operations should be async. Avoid `block_on` inside async contexts. CPU-heavy work (like hashing files for task state) should use `tokio::task::spawn_blocking`.

### Dependencies

Be conservative with new dependencies. Before adding a crate, consider:
- Is it well-maintained? (check last publish date, open issues)
- Does it pull in a large dependency tree?
- Could we do this with std or an existing dependency?

Current dependency choices and rationale are documented in `docs/design.md`.

### Linting & Warnings

All code must pass `cargo clippy` with no warnings and compile with no warnings. Treat warnings as errors — don't leave `#[allow(unused)]` or dead code lying around. If something is temporarily unused during development, remove it or gate it behind a feature.

Specifically:
- `cargo clippy -- -D warnings` must pass
- `cargo build 2>&1 | grep warning` must be empty
- No `#[allow(dead_code)]` or `#[allow(unused)]` in committed code
- Prefer explicit imports over glob imports (`use std::path::PathBuf`, not `use std::path::*`)

### Git

- Commit messages should be concise and describe *why*, not *what*
- One logical change per commit
- Keep the main branch clean — no broken builds

## Platform Support

Don targets Unix systems (Linux and macOS). Windows is not supported due to reliance on Unix sockets, process groups, signals, and `LISTEN_FDS`. Platform-specific code should use `cfg(target_os)` guards where needed.

## State Directory

All mutable *project* state goes under `.don/` in the project root. This directory must be in `.gitignore`. See `docs/design.md` for the full layout.

Never store project state outside `.don/` — a don-managed project should be fully self-contained and leave no footprint beyond this directory.

**The system-wide daemon is the one documented exception.** `don daemon` is not scoped to a project, so it has nowhere project-local to put its socket, project registry, and web-UI auth token. Those live in a single directory resolved by `src/daemon/paths.rs`: `$DON_STATE_DIR`, else `$XDG_STATE_HOME/don`, else `~/.local/state/don` (`~/Library/Application Support/don` on macOS). Nothing else may write outside `.don/`, and the daemon writes nothing into any project's directory.
