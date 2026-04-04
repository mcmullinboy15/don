# Don - Implementation Plan

This is the living implementation checklist. Check items off as they're completed.
**Agents: update this file as you finish work. Don't let it go stale.**

After completing any phase, run `cargo clippy -- -D warnings && cargo test` and fix any issues before moving on.

**Testing is a first-class concern.** Every phase ends with a coverage checkpoint. AI agents will be developing in this codebase, and they need fast, reliable test feedback to work effectively. If a bug can be caught by a test, it must be caught by a test. Prefer integration tests that exercise real behavior over unit tests that mock everything away.

**No backwards compatibility during initial build.** There are no external consumers yet. If a better API replaces an old one, delete the old one and update all callers (including tests) in the same change. Don't keep deprecated APIs, shims, or compatibility wrappers around — they become traps that agents reach for instead of the correct path.

---

## Phase 1: Restructure, Foundation & Test Harness

Get the existing code into the right module structure, fix known bugs, and establish the testing patterns that all future phases will use.

### Restructure
- [x] Restructure `config.rs` into `config/` module directory (mod.rs, service.rs, task.rs, profile.rs, platform.rs, download.rs, types.rs)
- [x] Move `task_state.rs` to its final location
- [x] Set up `lib.rs` with clean public re-exports
- [x] Define error types with `thiserror` for each module (ConfigError, DurationError, TaskStateError)

### Fix Known Bugs
- [x] Fix `PlatformDownload::binary_path` — had an `expect()`, now returns `Option`
- [x] Fix task state glob resolution — globs now resolve relative to `base_dir` parameter
- [x] Validate duration strings (`interval`, `timeout`, `debounce`, `shutdown.timeout`) during config validation — rejects invalid values at validation time

### Duration Parsing
- [x] Implement duration string parser (e.g. "200ms", "1s", "5m", "30s") — `src/duration.rs`, shared utility
- [x] Wire into config validation — validates debounce, ready.interval, shutdown.timeout, task.timeout

### CLI Skeleton
- [x] Stub out all clap subcommands (start, stop, restart, status, logs, cleanup, validate)
- [x] Add `--profile` flag on `start`
- [x] `don validate` — fully wired to Config::from_file + Config::validate

### Architectural Decisions
- [x] Decided on tokio channels (mpsc + broadcast + oneshot) — no Arc<Mutex>
- [x] Documented in CLAUDE.md under "Cross-Module Communication"

### Test Infrastructure
- [x] Create `tests/` directory for integration tests
- [x] Build test helpers (`tests/helpers/`):
  - `tempdir.rs` — TempDir with auto-cleanup
  - `config.rs` — ConfigBuilder with ServiceBuilder/TaskBuilder sub-builders
  - `port.rs` — free_port() for ephemeral port allocation
  - `timeout.rs` — run_with_timeout() for async test timeouts
- [x] 11 integration tests proving the harness works (valid/invalid configs, CLI binary tests)
- [x] Added CI test guidance to CLAUDE.md (PTY fallback, test timeouts)

### Test Coverage Checkpoint
- [x] All existing config parsing tests pass after restructure (6 unit tests)
- [x] All existing task_state tests pass + new base_dir test
- [x] Duration parser: 18 table-driven test cases (valid, invalid, edge cases)
- [x] Config validation tests cover invalid duration strings (4 new test cases)
- [x] Integration test harness proven with 11 end-to-end tests
- [x] `cargo clippy -- -D warnings` passes
- [x] No `unwrap()`/`expect()`/`panic!()` outside `#[cfg(test)]`

---

## Phase 2: Process Management & Basic Signal Handling

The core of don — spawning children in process groups with PTYs, PID file locking, and enough signal handling to not orphan processes during development.

### Process Spawning
- [x] `process/mod.rs` — spawn child in its own process group via `setpgid(0, 0)`, allocate PTY via `pty-process`
- [x] PTY fallback for headless/CI environments — if PTY allocation fails, fall back to pipe-based spawning (`force_pipe` flag + auto-fallback)
- [x] ProcessHandle with: async read from PTY/pipe output, PID/PGID, wait/kill/terminate methods

### PID File Locking
- [x] `process/pid_file.rs` — `Flock<File>` based locking with O_CLOEXEC, write PGID, hold for lifetime
- [x] `PidFile::acquire()`, `try_lock_stale()`, `update_pgid()`, `cleanup()`
- [x] ~Don's own PID file~ — PidFile is generic enough for this, wiring deferred to Phase 4 runner

### Basic Signal Handling
- [x] ~Signal handler installation~ — deferred to Phase 4 runner (primitives ready via ProcessHandle::terminate)
- [x] ProcessHandle::terminate() — send signal, wait with timeout, escalate to SIGKILL, Unkillable detection (500ms)
- [x] ~PID file cleanup on exit~ — deferred to Phase 4 runner (PidFile Drop releases flock automatically)

### Basic Environment
- [x] `process/env.rs` — parse_env_file() with KEY=VALUE, comments, quotes, export prefix, malformed line warnings
- [x] merge_env() — correct precedence order: .env.<name> → env_file → config env → injected
- [x] Pass merged env when spawning children (SpawnConfig.env)

### Test Coverage Checkpoint
- [x] Unit test: process group creation (spawn a child, verify it has its own PGID)
- [x] Unit test: PID file locking (acquire, try again fails, drop releases, re-acquire succeeds)
- [x] Unit test: PID file flock (try_lock_stale on held = None, on released = Some(pgid))
- [x] Unit test: env loading — merge order verified with overlapping keys
- [x] Unit test: env file parsing — 15 table-driven cases (valid, malformed, quotes, comments, export, etc.)
- [x] Unit test: spawn + SIGTERM via terminate() — process exits
- [x] Unit test: terminate with SIGKILL escalation — process ignoring SIGTERM gets killed after timeout
- [x] Unit test: spawn with PID file — file exists, second spawn blocked, drop releases
- [x] Unit test: pipe fallback mode (force_pipe=true) — output readable
- [x] Unit test: PTY mode — output readable
- [x] `cargo clippy -- -D warnings` passes
- [x] No orphaned processes after test run

---

## Phase 3: Output Handling

Get service output displaying correctly in the terminal.

### Line Buffering & Prefixing
- [x] `output/mod.rs` — read from PTY async, buffer per-line, apply service name prefix with color coding
- [x] Color assignment — assign distinct terminal colors to services deterministically
- [x] Align prefix columns to the longest service/task name
- [x] Merge stdout/stderr from each child into a single stream (PTY already does this)

### Ring Buffer
- [x] `output/ring_buffer.rs` — bounded per-service ring buffer (configurable, default ~10k lines)
- [x] All output feeds the ring buffer regardless of log routing

### Log Routing
- [x] `output/log_router.rs` — route based on `LogConfig`: stdout (prefixed), file (raw), ignore (discard). All modes feed the ring buffer.

### Lifecycle Events
- [x] `[don]` prefix for don's own messages: starting, ready, exited, skipped, errors
- [x] Consistent formatting across all lifecycle events

### Test Coverage Checkpoint
- [x] Unit test: line buffering — feed partial lines, verify complete lines come out with correct prefix
- [x] Unit test: line buffering — concurrent writes from multiple "services" never interleave mid-line
- [x] Unit test: prefix alignment — verify columns align when service names have different lengths
- [x] Unit test: color assignment — same set of service names always gets same colors (deterministic)
- [x] Unit test: ring buffer — write N lines, read back last N, verify order
- [x] Unit test: ring buffer — fill past capacity, verify oldest lines evicted, total count correct
- [x] Unit test: ring buffer — empty buffer returns empty, single line works
- [x] Unit test: log routing — stdout mode: output appears in captured output with prefix
- [x] Unit test: log routing — file mode: output written to file without prefix, ring buffer still fed
- [x] Unit test: log routing — ignore mode: ring buffer still fed, no output to stdout or file
- [x] Integration test: start a service that prints known output, verify prefixed lines appear correctly
- [x] Integration test: start a service with `log = "ignore"`, verify output is suppressed but ring buffer has it

---

## Phase 4: Dependency Graph & Basic Service Runner

Get services starting in the right order with max parallelism.

### Dependency Graph Engine
- [x] `runner/mod.rs` — topological sort of services and tasks, detect cycles (already done in validation, reuse here for execution ordering)
- [x] Parallel executor: start everything whose dependencies are satisfied concurrently, using tokio tasks
- [x] Track service states: pending, starting, running, ready, stopping, stopped, failed

### Signal Handling & Don PID File (deferred from Phase 2)
- [x] Install SIGINT/SIGTERM handler via tokio::signal — set atomic flag, runner checks it in command loop
- [x] On first signal: SIGTERM all child process groups, wait per-service timeout, SIGKILL stragglers
- [x] On second signal: immediate SIGKILL all process groups
- [x] Acquire don's own PID file at `.don/don.pid` on startup — detect if another don is already running
- [x] Clean up `.don/don.pid` and `.don/don.sock` on exit

### Basic Service Lifecycle
- [x] `runner/service.rs` — start a service (spawn process, begin output capture), stop a service (signal + timeout + SIGKILL), restart (stop then start)
- [x] `runner/task.rs` — execute a task: check task_state for skip, run if needed, record_success on exit 0, handle timeout (kill process group on expiry)

### Ready Checks
- [x] Exec ready check: spawn command, check exit code 0
- [x] TCP ready check: attempt async TCP connect
- [x] HTTP ready check: `reqwest` GET, check for 2xx (this is why we added reqwest)
- [x] Retry loop with configurable interval and retries
- [x] Dependency gating: services/tasks wait for dependencies' ready checks to pass or tasks to complete before starting

### Test Coverage Checkpoint
- [x] Unit test: topological sort — linear chain (a -> b -> c), diamond (a -> b, a -> c, b -> d, c -> d), independent nodes
- [x] Unit test: topological sort — cycle detection returns the cycle path
- [x] Unit test: parallel executor — three independent services start concurrently (verify wall-clock time is ~max, not sum)
- [x] Unit test: service state machine — verify valid transitions, reject invalid ones (e.g. stopped -> ready)
- [x] Unit test: task skip logic — unchanged files skip, changed files run, no watch patterns always run, failed tasks always retry
- [x] Unit test: task timeout — task exceeding timeout is killed, treated as failure
- [x] Integration test: start two services where B depends on A, verify A starts and is ready before B starts
- [x] Integration test: start a service and a task where the task depends on the service, verify service is ready before task runs
- [x] Integration test: TCP ready check — start a service that listens on a port, configure TCP ready check, verify dependent starts after port is open
- [x] Integration test: exec ready check — configure a command that fails N times then succeeds, verify retries work
- [x] Integration test: HTTP ready check — start an HTTP server, configure health endpoint, verify check passes
- [x] Integration test: ready check exhausts retries — verify service is marked failed, dependents don't start
- [x] Integration test: task with `watch` files — run once (succeeds), verify skip on second run, modify file, verify re-run

---

## Phase 5: File Watching & Debounce

Watch for changes and trigger rebuilds/restarts.

- [x] `watch/mod.rs` — set up `notify` watchers for each service's `watch` patterns, creates missing watch directories via `create_dir_all` on the glob base dir
- [x] Debounce: collect events for 200ms (or configured `debounce`), trigger one rebuild cycle — sliding window resets on each new event
- [x] Change-during-build state machine: let build finish, mark stale, trigger another cycle — Idle → Debouncing → Rebuilding → Idle (with stale flag for re-trigger)
- [x] Build failure handling: keep old process running, log error, stay in watching state — build failure broadcasts RebuildComplete(success=false), transitions back to Idle
- [x] Watch-triggered task re-evaluation: if a task's watch files change, re-run it — TaskRerun command + TaskRerunComplete event

### Test Coverage Checkpoint
- [x] Unit test: debounce — fire 10 events in 50ms, verify only one rebuild triggered
- [x] Unit test: debounce — events after the window trigger a new cycle
- [x] Unit test: debounce — custom debounce duration is respected
- [x] Unit test: change-during-build — trigger rebuild, fire event during build, verify second rebuild after first completes
- [x] Unit test: change-during-build — multiple events during build still result in only one follow-up rebuild
- [x] Unit test: state machine transitions — idle -> debouncing -> building -> restarting -> idle, with stale flag handling
- [x] Integration test: start a service with watch patterns, modify a watched file, verify service restarts
- [x] Integration test: start a service with a build step, modify a watched file, verify build runs then service restarts
- [x] Integration test: modify a watched file, make build fail, verify old process stays running and error is logged
- [x] Integration test: rapid-fire 5 file changes, verify only one restart cycle

---

## Phase 6: Socket Passing

Zero-downtime restarts for services with `listen` addresses.

- [ ] Bind `listen` addresses in don, hold the fds
- [ ] Pass fds to child processes via `LISTEN_FDS` / `LISTEN_FDNAMES` environment variables
- [ ] Graceful switchover: spawn new process, wait for ready check, then SIGTERM old process to drain
- [ ] Keep sockets open across restarts

### Test Coverage Checkpoint
- [ ] Unit test: `LISTEN_FDS` and `LISTEN_FDNAMES` values are computed correctly for 1, 2, N sockets
- [ ] Integration test: start a service with `listen`, verify the socket is bound and accepting connections
- [ ] Integration test: restart a service with `listen`, verify the socket never closes (connect during restart succeeds)
- [ ] Integration test: verify child process receives the correct fd numbers and env vars
- [ ] Integration test: graceful switchover — old process gets SIGTERM only after new process passes ready check

---

## Phase 7: Docker Support

Full docker service lifecycle.

- [ ] Build the `docker run` command from `DockerConfig` (image, container, ports, volumes, network, command, env, env_file)
- [ ] Docker build: `docker build` from `DockerBuildConfig` (context, dockerfile, target, args), tag with `image`
- [ ] Container status check: `docker inspect` to see if container is already running
- [ ] Container cleanup: stop and remove containers on shutdown and during stale cleanup
- [ ] Watch-triggered docker rebuild: rebuild image, recreate container

### Test Coverage Checkpoint
- [ ] Unit test: `docker run` command construction — verify all flags (ports, volumes, network, env, env_file, command) are generated correctly
- [ ] Unit test: `docker build` command construction — verify context, dockerfile, target, build-args flags
- [ ] Unit test: command construction with minimal config (just image) vs full config
- [ ] Integration test (requires docker): start a docker service, verify container is running via `docker inspect`, stop it, verify container is removed
- [ ] Integration test (requires docker): docker build + run — build from a Dockerfile, verify image is tagged, container runs

---

## Phase 8: Rust Preset

Cargo build and run integration.

- [ ] Generate `cargo build` command from `RustConfig` (binary, features, release, extra_args, target_dir)
- [ ] Resolve binary path from cargo target directory
- [ ] Watch defaults: if no `watch` patterns set, default to `src/**/*.rs`, `Cargo.toml`, `Cargo.lock`

### Test Coverage Checkpoint
- [ ] Unit test: `cargo build` command construction — minimal (just binary), full (features, release, extra_args, target_dir)
- [ ] Unit test: binary path resolution — debug vs release, custom target_dir
- [ ] Unit test: default watch patterns are applied when `watch` is empty
- [ ] Unit test: default watch patterns are NOT applied when `watch` is explicitly set
- [ ] Integration test: create a minimal Rust project in a temp dir, configure as a rust service, verify it builds and runs
- [ ] Integration test: rust service with `release = true`, verify `--release` flag is passed

---

## Phase 9: Downloads

Artifact downloading, verification, and caching.

- [ ] Download artifacts to `.don/cache/<sha256>/`
- [ ] SHA-256 verification of downloaded files
- [ ] Archive extraction (tar.gz, zip)
- [ ] Setup command execution (with marker file to only run once)
- [ ] Binary path resolution wired into the runner (via `resolved_run_cmd`)
- [ ] Skip download if cache hit (sha256 dir already exists)

### Test Coverage Checkpoint
- [ ] Unit test: SHA-256 verification — correct hash passes, wrong hash fails, empty file
- [ ] Unit test: cache path construction — verify `.don/cache/<sha256>/` layout
- [ ] Unit test: binary path resolution — with `path` (archive), without `path` (bare binary), no download for platform (fallback to cmd)
- [ ] Integration test: download a small test file from a local HTTP server, verify it's cached at the correct path
- [ ] Integration test: download with wrong sha256 — verify failure and no partial cache left behind
- [ ] Integration test: tar.gz extraction — verify files extracted to correct paths
- [ ] Integration test: setup command runs once (marker file written), second run skips
- [ ] Integration test: cache hit — second download skips network, uses cached artifact

---

## Phase 10: Unix Socket API

HTTP API for CLI-to-daemon communication. Build the server first so the CLI can talk to it.

- [ ] `server/mod.rs` — axum app listening on `.don/don.sock`
- [ ] `server/routes.rs` — endpoints:
  - `GET /status` — status of all services/tasks
  - `POST /restart/:name` — restart a service
  - `POST /stop/:name` — stop a service
  - `POST /start/:name` — start a stopped service
  - `GET /logs/:name?last=N` — read from ring buffer

### Test Coverage Checkpoint
- [ ] Integration test: start the server, connect to unix socket, hit `GET /status`, verify JSON response with service states
- [ ] Integration test: start a service, `POST /stop/:name`, verify it stops, `GET /status` shows stopped
- [ ] Integration test: `POST /restart/:name` on a running service — verify it restarts
- [ ] Integration test: `POST /stop/:name` with unknown name — verify 404 response
- [ ] Integration test: `GET /logs/:name?last=5` — verify last 5 lines from ring buffer
- [ ] Integration test: `GET /logs/:name` for a service with `log = "ignore"` — verify ring buffer still has output

---

## Phase 11: CLI Commands

Wire all subcommands to the unix socket API.

- [ ] `don start` — start the daemon, run everything (or `--profile` subset)
- [ ] `don start --profile <name>` — resolve profile with transitive deps
- [ ] `don stop <name>` — connect to socket, POST stop
- [ ] `don restart <name>` — connect to socket, POST restart
- [ ] `don status` — connect to socket, GET status, display table
- [ ] `don logs <name>` — connect to socket, GET logs, stream to terminal
- [ ] `don logs <name> --last N` — show last N lines from ring buffer
- [ ] `don cleanup` — run stale state cleanup without starting anything
- [ ] CLI detects if don is running (try connect to socket) — commands that need it fail with a clear error if not

### Test Coverage Checkpoint
- [ ] Integration test: `don validate` with a valid config — exit 0, no output on stderr
- [ ] Integration test: `don validate` with an invalid config — exit non-zero, error message on stderr
- [ ] Integration test: `don validate` with a missing config file — exit non-zero, helpful error
- [ ] Integration test: start don, run `don status` from another process, verify output lists services
- [ ] Integration test: run `don stop api` when don is not running — verify clear error message
- [ ] Integration test: `don start --profile frontend` — verify only profiled services start
- [ ] Integration test: `don logs api --last 10` — verify correct output

---

## Phase 12: Stale State Cleanup

Robust cleanup for crash recovery.

- [ ] `process/cleanup.rs` — full cleanup implementation:
  - Try-lock each file in `.don/pids/` — if lock succeeds, process is dead: read PGID, `killpg`, delete file
  - Check `.don/don.sock` — try connect, remove if stale
  - Check `.don/don.pid` — try lock, clean up if stale
  - Docker container cleanup: for services with `docker.container`, check if container exists and is orphaned
- [ ] Cleanup runs automatically on startup before starting services
- [ ] `don cleanup` invokes the same logic standalone

### Test Coverage Checkpoint
- [ ] Integration test: spawn a process, write its PGID to a pid file, kill the process externally (not via don), run cleanup, verify pid file is removed and process group is killed
- [ ] Integration test: create a stale `.don/don.sock` (no listener), run startup, verify it's cleaned up
- [ ] Integration test: create a stale pid file with a reused PID (process exists but isn't ours), verify flock correctly identifies it as not-stale (lock fails)
- [ ] Integration test: create multiple stale pid files, run cleanup, verify all are cleaned up
- [ ] Integration test: run `don cleanup` with no stale state — verify it exits cleanly with no errors
- [ ] Integration test (requires docker): leave an orphaned container, run cleanup, verify it's stopped and removed

---

## Phase 13: Config Auto-Reload

Watch `don.toml` and apply changes live.

- [ ] Watch `don.toml` for changes (reuse watch/debounce infrastructure)
- [ ] Parse and validate new config on change
- [ ] Diff against running config: identify added, removed, and changed services/tasks
- [ ] Stop removed services
- [ ] Restart changed services (respecting dependency order)
- [ ] Start newly added services (respecting dependency order)
- [ ] If new config is invalid, log errors and keep running with current config

### Test Coverage Checkpoint
- [ ] Unit test: config diff — added service detected, removed service detected, changed service detected, unchanged service not flagged
- [ ] Unit test: config diff — changed env var, changed watch patterns, changed preset all detected as changes
- [ ] Unit test: config diff — added task, removed task, changed task detected
- [ ] Integration test: start don, modify don.toml to add a new service, verify it starts
- [ ] Integration test: start don, modify don.toml to remove a service, verify it stops
- [ ] Integration test: start don, modify don.toml to change a service's env, verify it restarts
- [ ] Integration test: start don, write an invalid don.toml, verify don keeps running with old config and logs the error
- [ ] Integration test: start don, rapidly modify don.toml twice, verify debounce prevents thrashing

---

## Phase 14: Shutdown Refinement

Upgrade the basic signal handling from Phase 2 to full graceful shutdown.

- [ ] Reverse dependency order shutdown: services with no dependents stop first
- [ ] Per-service shutdown config: respect `shutdown.signal` and `shutdown.timeout`
- [ ] Kill running tasks on shutdown: SIGTERM to process group, brief wait, SIGKILL
- [ ] First signal prints `[don] shutting down gracefully... (Ctrl+C again to force)`
- [ ] Second signal prints `[don] forcing immediate shutdown` and SIGKILLs everything
- [ ] Clean up all PID files, remove `.don/don.sock`, remove `.don/don.pid`

### Test Coverage Checkpoint
- [ ] Integration test: start services A -> B -> C (C depends on B depends on A), send SIGTERM, verify C stops first, then B, then A
- [ ] Integration test: start a service with `shutdown.signal = "SIGINT"`, verify it receives SIGINT not SIGTERM
- [ ] Integration test: start a service with `shutdown.timeout = "1s"` that ignores SIGTERM, verify it gets SIGKILL after ~1s
- [ ] Integration test: first Ctrl+C starts graceful shutdown, second Ctrl+C immediately SIGKILLs everything
- [ ] Integration test: verify all PID files and socket are cleaned up after shutdown
- [ ] Integration test: task running during shutdown is killed cleanly

---

## Phase 15: Profiles

Subset selection with transitive dependency resolution.

- [ ] Resolve profile: collect listed services/tasks, walk `depends_on` to include transitive deps
- [ ] Only start/run the resolved subset
- [ ] `don start --profile <name>` wired up end-to-end

### Test Coverage Checkpoint
- [ ] Unit test: transitive dep resolution — profile lists `api`, api depends on `migrate`, migrate depends on `postgres` — all three included
- [ ] Unit test: transitive dep resolution — profile lists two services with overlapping deps, no duplicates in result
- [ ] Unit test: profile with only tasks — verify services pulled in via task deps
- [ ] Unit test: profile references nonexistent service — caught by validation
- [ ] Integration test: start with `--profile frontend`, verify only profiled services and their transitive deps run
- [ ] Integration test: start with `--profile frontend`, verify services NOT in the profile are not started

---

## Phase 16: Polish

Final touches for a great dev experience.

- [ ] Gitignore check: warn at startup if `.don/` is not in `.gitignore`
- [ ] Port conflict pre-check: test declared ports before starting services
- [ ] Actionable error messages: typo suggestions for dependency names (Levenshtein distance), clear context on every error
- [ ] Color-coded service prefixes with aligned columns (verify this works well with 10+ services)
- [ ] Verify all `[don]` lifecycle messages are consistent and helpful
- [ ] Write README.md with quickstart, example config, and feature overview
- [ ] Detect terminal control sequences (alternate screen, cursor movement) in service output — suppress from prefixed output and show `[don] api: interactive output suppressed — use 'don attach api' to view`

### Final Test Coverage Checkpoint
- [ ] Verify no `unwrap()`/`expect()`/`panic!()` outside test code (final audit)
- [ ] Full `cargo clippy -- -D warnings` pass
- [ ] Doc comments on all `pub` items — check with `cargo doc --no-deps`
- [ ] Run full test suite — all tests pass, no flaky tests
- [ ] Review test coverage: every user-facing feature has at least one integration test
- [ ] Review test coverage: every error path has at least one test (invalid config, missing files, failed builds, crashed services, stale state, etc.)
- [ ] Create a sample `don.toml` in `examples/` with all features demonstrated and commented

---

## Phase 17: Interactive Attach

Connect your terminal directly to a running service's PTY for interactive stdin/stdout access. Depends on Phases 2 (PTY), 3 (output), and 10 (unix socket API).

### WebSocket Endpoint
- [ ] `GET /attach/:name` endpoint on the unix socket API that upgrades to a WebSocket
- [ ] Replay recent output from ring buffer on connect (so the user has context)
- [ ] Bidirectional bridge: WebSocket frames ↔ PTY stdin/stdout
- [ ] Terminal resize: CLI sends resize control messages, daemon calls `pty.resize()`

### Attach Lock
- [ ] Daemon tracks which PID holds the attach lock for each service
- [ ] CLI sends its PID in the initial WebSocket handshake
- [ ] Second attach attempt returns error: `"process 82648 is currently attached to 'my-task'"`
- [ ] Lock auto-released on WebSocket disconnect (process dies or detaches)

### CLI
- [ ] `don attach <name>` subcommand
- [ ] Put terminal in raw mode (crossterm) for direct input passthrough
- [ ] Bridge stdin/stdout to WebSocket
- [ ] Detect and forward terminal resize events
- [ ] Escape sequence `~.` to detach without killing the process
- [ ] Restore terminal from raw mode on detach/exit

### Output Integration
- [ ] Pause prefixed output for the attached service in the don terminal
- [ ] Ring buffer continues to be fed during attach
- [ ] Resume prefixed output on detach

### Test Coverage Checkpoint
- [ ] Integration test: attach to a running service, send input, verify it reaches the subprocess
- [ ] Integration test: attempt second attach while first is active — verify rejection with PID
- [ ] Integration test: first attacher disconnects, second attach succeeds
- [ ] Integration test: detach with escape sequence, verify service keeps running
- [ ] Integration test: terminal resize propagates to subprocess PTY
