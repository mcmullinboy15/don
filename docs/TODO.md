# Don - Implementation Plan

This is the living implementation checklist. Check items off as they're completed.
**Agents: update this file as you finish work. Don't let it go stale.**

After completing any phase, run `cargo clippy -- -D warnings && cargo test` and fix any issues before moving on.

**Testing is a first-class concern.** Every phase ends with a coverage checkpoint. AI agents will be developing in this codebase, and they need fast, reliable test feedback to work effectively. If a bug can be caught by a test, it must be caught by a test. Prefer integration tests that exercise real behavior over unit tests that mock everything away.

---

## Phase 1: Restructure, Foundation & Test Harness

Get the existing code into the right module structure, fix known bugs, and establish the testing patterns that all future phases will use.

### Restructure
- [ ] Restructure `config.rs` into `config/` module directory (mod.rs, service.rs, task.rs, profile.rs, platform.rs, download.rs, types.rs)
- [ ] Move `task_state.rs` to its final location
- [ ] Set up `lib.rs` with clean public re-exports
- [ ] Define error types with `thiserror` for each module

### Fix Known Bugs
- [x] Fix `PlatformDownload::binary_path` — had an `expect()`, now returns `Option`
- [ ] Fix task state glob resolution — globs must resolve relative to `task.dir`, not don's cwd
- [ ] Validate duration strings (`interval`, `timeout`, `debounce`, `shutdown.timeout`) during config validation — reject invalid values like `"banana"` at parse time, not runtime

### Duration Parsing
- [ ] Implement duration string parser (e.g. "200ms", "1s", "5m", "30s") — shared utility used by ready checks, shutdown, debounce, and task timeout
- [ ] Wire into config validation

### CLI Skeleton
- [ ] Stub out all clap subcommands (start, stop, restart, status, logs, cleanup, validate) — they can print "not implemented" for now, but the argument structure should be final
- [ ] Add `--profile` flag on `start`
- [ ] `don validate` — wire to existing `Config::from_file` + `Config::validate`, this is already implementable

### Architectural Decisions
- [ ] Decide and document cross-module communication pattern (tokio channels vs Arc<Mutex> vs actor model) — this affects every phase going forward
- [ ] Add the decision to CLAUDE.md so all agents follow it

### Test Infrastructure
- [ ] Create `tests/` directory for integration tests
- [ ] Build a test harness crate or helper module (`tests/helpers/`) with:
  - Temp directory management (project-local `.don/` state)
  - Config builder — programmatic `don.toml` generation for tests
  - Process assertions — helper to verify a process started, check its output, wait for ready
  - Port allocation — find free ports for tests to avoid conflicts
  - Timeout wrapper — every integration test gets a max runtime to prevent hangs
- [ ] Write a basic integration test: parse a config, validate it, verify the result — proves the harness works
- [ ] Add integration test CI guidance to CLAUDE.md (PTY fallback for headless environments — tests must work without a real terminal)

### Test Coverage Checkpoint
- [ ] All existing config parsing tests still pass after restructure
- [ ] All existing task_state tests still pass after restructure
- [ ] Duration parser has table-driven tests covering: valid inputs ("1s", "200ms", "5m", "1h"), invalid inputs ("banana", "", "-1s", "5"), edge cases ("0s", "0ms")
- [ ] Config validation tests cover invalid duration strings
- [ ] Integration test harness is proven working with at least one end-to-end test
- [ ] `cargo clippy -- -D warnings` passes
- [ ] No `unwrap()`/`expect()`/`panic!()` outside `#[cfg(test)]`

---

## Phase 2: Process Management & Basic Signal Handling

The core of don — spawning children in process groups with PTYs, PID file locking, and enough signal handling to not orphan processes during development.

### Process Spawning
- [ ] `process/mod.rs` — spawn child in its own process group via `setpgid(0, 0)`, allocate PTY via `pty-process`
- [ ] PTY fallback for headless/CI environments — if PTY allocation fails, fall back to pipe-based spawning (no colors, but functional)
- [ ] Return a handle that provides: async read from PTY output, child PID/PGID, wait/kill methods

### PID File Locking
- [ ] `process/pid_file.rs` — open `.don/pids/<name>`, `flock(LOCK_EX | LOCK_NB)`, write PGID, hold fd for lifetime
- [ ] Don's own PID file at `.don/don.pid` with flock — detect if another don is already running

### Basic Signal Handling
- [ ] Install SIGINT/SIGTERM handler — set a flag, main loop checks it
- [ ] On first signal: SIGTERM all child process groups, wait briefly, SIGKILL stragglers
- [ ] On second signal: immediate SIGKILL all process groups
- [ ] Clean up PID files and socket on exit

### Basic Environment
- [ ] Load and merge environment variables for child processes: auto-load `.env.<name>`, load `env_file` entries, merge `env` from config, inject don variables
- [ ] Pass merged env when spawning children

### Test Coverage Checkpoint
- [ ] Unit test: process group creation (spawn a child, verify it has its own PGID)
- [ ] Unit test: PID file locking (lock, try lock again — must fail, release, try again — must succeed)
- [ ] Unit test: PID file locking with flock — verify a held lock prevents a second lock, and releasing allows re-lock
- [ ] Unit test: env loading — verify merge order (.env file < env_file < env config < don-injected)
- [ ] Unit test: env file parsing — valid files, missing files (should not error), malformed lines
- [ ] Integration test: spawn a simple process (e.g. `sleep`), verify it starts, send SIGTERM, verify it exits
- [ ] Integration test: spawn a process, simulate don shutdown, verify child is cleaned up (no orphans)
- [ ] Integration test: spawn a process, SIGKILL don, verify the child's process group can be found and killed via the PID file
- [ ] Integration test: PTY fallback — force PTY failure, verify process still spawns with pipe-based I/O
- [ ] Integration test: verify another don instance is detected via PID file lock

---

## Phase 3: Output Handling

Get service output displaying correctly in the terminal.

### Line Buffering & Prefixing
- [ ] `output/mod.rs` — read from PTY async, buffer per-line, apply service name prefix with color coding
- [ ] Color assignment — assign distinct terminal colors to services deterministically
- [ ] Align prefix columns to the longest service/task name
- [ ] Merge stdout/stderr from each child into a single stream (PTY already does this)

### Ring Buffer
- [ ] `output/ring_buffer.rs` — bounded per-service ring buffer (configurable, default ~10k lines)
- [ ] All output feeds the ring buffer regardless of log routing

### Log Routing
- [ ] `output/log_router.rs` — route based on `LogConfig`: stdout (prefixed), file (raw), ignore (discard). All modes feed the ring buffer.

### Lifecycle Events
- [ ] `[don]` prefix for don's own messages: starting, ready, exited, skipped, errors
- [ ] Consistent formatting across all lifecycle events

### Test Coverage Checkpoint
- [ ] Unit test: line buffering — feed partial lines, verify complete lines come out with correct prefix
- [ ] Unit test: line buffering — concurrent writes from multiple "services" never interleave mid-line
- [ ] Unit test: prefix alignment — verify columns align when service names have different lengths
- [ ] Unit test: color assignment — same set of service names always gets same colors (deterministic)
- [ ] Unit test: ring buffer — write N lines, read back last N, verify order
- [ ] Unit test: ring buffer — fill past capacity, verify oldest lines evicted, total count correct
- [ ] Unit test: ring buffer — empty buffer returns empty, single line works
- [ ] Unit test: log routing — stdout mode: output appears in captured output with prefix
- [ ] Unit test: log routing — file mode: output written to file without prefix, ring buffer still fed
- [ ] Unit test: log routing — ignore mode: ring buffer still fed, no output to stdout or file
- [ ] Integration test: start a service that prints known output, verify prefixed lines appear correctly
- [ ] Integration test: start a service with `log = "ignore"`, verify output is suppressed but ring buffer has it

---

## Phase 4: Dependency Graph & Basic Service Runner

Get services starting in the right order with max parallelism.

### Dependency Graph Engine
- [ ] `runner/mod.rs` — topological sort of services and tasks, detect cycles (already done in validation, reuse here for execution ordering)
- [ ] Parallel executor: start everything whose dependencies are satisfied concurrently, using tokio tasks
- [ ] Track service states: pending, starting, running, ready, stopping, stopped, failed

### Basic Service Lifecycle
- [ ] `runner/service.rs` — start a service (spawn process, begin output capture), stop a service (signal + timeout + SIGKILL), restart (stop then start)
- [ ] `runner/task.rs` — execute a task: check task_state for skip, run if needed, record_success on exit 0, handle timeout (kill process group on expiry)

### Ready Checks
- [ ] Exec ready check: spawn command, check exit code 0
- [ ] TCP ready check: attempt async TCP connect
- [ ] HTTP ready check: `reqwest` GET, check for 2xx (this is why we added reqwest)
- [ ] Retry loop with configurable interval and retries
- [ ] Dependency gating: services/tasks wait for dependencies' ready checks to pass or tasks to complete before starting

### Test Coverage Checkpoint
- [ ] Unit test: topological sort — linear chain (a -> b -> c), diamond (a -> b, a -> c, b -> d, c -> d), independent nodes
- [ ] Unit test: topological sort — cycle detection returns the cycle path
- [ ] Unit test: parallel executor — three independent services start concurrently (verify wall-clock time is ~max, not sum)
- [ ] Unit test: service state machine — verify valid transitions, reject invalid ones (e.g. stopped -> ready)
- [ ] Unit test: task skip logic — unchanged files skip, changed files run, no watch patterns always run, failed tasks always retry
- [ ] Unit test: task timeout — task exceeding timeout is killed, treated as failure
- [ ] Integration test: start two services where B depends on A, verify A starts and is ready before B starts
- [ ] Integration test: start a service and a task where the task depends on the service, verify service is ready before task runs
- [ ] Integration test: TCP ready check — start a service that listens on a port, configure TCP ready check, verify dependent starts after port is open
- [ ] Integration test: exec ready check — configure a command that fails N times then succeeds, verify retries work
- [ ] Integration test: HTTP ready check — start an HTTP server, configure health endpoint, verify check passes
- [ ] Integration test: ready check exhausts retries — verify service is marked failed, dependents don't start
- [ ] Integration test: task with `watch` files — run once (succeeds), verify skip on second run, modify file, verify re-run

---

## Phase 5: File Watching & Debounce

Watch for changes and trigger rebuilds/restarts.

- [ ] `watch/mod.rs` — set up `notify` watchers for each service's `watch` patterns
- [ ] Debounce: collect events for 200ms (or configured `debounce`), trigger one rebuild cycle
- [ ] Change-during-build state machine: let build finish, mark stale, trigger another cycle
- [ ] Build failure handling: keep old process running, log error, stay in watching state
- [ ] Watch-triggered task re-evaluation: if a task's watch files change, re-run it

### Test Coverage Checkpoint
- [ ] Unit test: debounce — fire 10 events in 50ms, verify only one rebuild triggered
- [ ] Unit test: debounce — events after the window trigger a new cycle
- [ ] Unit test: debounce — custom debounce duration is respected
- [ ] Unit test: change-during-build — trigger rebuild, fire event during build, verify second rebuild after first completes
- [ ] Unit test: change-during-build — multiple events during build still result in only one follow-up rebuild
- [ ] Unit test: state machine transitions — idle -> debouncing -> building -> restarting -> idle, with stale flag handling
- [ ] Integration test: start a service with watch patterns, modify a watched file, verify service restarts
- [ ] Integration test: start a service with a build step, modify a watched file, verify build runs then service restarts
- [ ] Integration test: modify a watched file, make build fail, verify old process stays running and error is logged
- [ ] Integration test: rapid-fire 5 file changes, verify only one restart cycle

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

### Final Test Coverage Checkpoint
- [ ] Verify no `unwrap()`/`expect()`/`panic!()` outside test code (final audit)
- [ ] Full `cargo clippy -- -D warnings` pass
- [ ] Doc comments on all `pub` items — check with `cargo doc --no-deps`
- [ ] Run full test suite — all tests pass, no flaky tests
- [ ] Review test coverage: every user-facing feature has at least one integration test
- [ ] Review test coverage: every error path has at least one test (invalid config, missing files, failed builds, crashed services, stale state, etc.)
- [ ] Create a sample `don.toml` in `examples/` with all features demonstrated and commented
