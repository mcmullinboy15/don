# don

Boss of your dev environment. Start every service, task, and dependency with one command.

```sh
don start
```

Don reads a `don.toml` in your project root and orchestrates your entire dev stack: databases, API servers, background workers, migration tasks, file watchers — all with dependency ordering, ready checks, and color-coded output in a single terminal.

## Install

```sh
# From source
cargo install --path .

# Or via Homebrew (once published)
brew install pjtatlow/tap/don
```

## Quick Start

Create a `don.toml`:

```toml
[services.postgres]
docker.image = "postgres:16"
docker.ports = ["5432:5432"]
docker.env_file = [".env"]
ready.tcp = "127.0.0.1:5432"

[tasks.migrate]
cmd = "dbmate"
args = ["up"]
depends_on = ["postgres"]

[services.api]
run.cmd = "cargo"
run.args = ["run", "--bin", "api"]
depends_on = ["migrate"]
watch = ["src/**/*.rs", "Cargo.toml"]
env = { DATABASE_URL = "postgres://localhost:5432/myapp" }
ready.http = "http://localhost:3000/health"

[services.worker]
run.cmd = "cargo"
run.args = ["run", "--bin", "worker"]
depends_on = ["migrate"]
watch = ["src/**/*.rs", "Cargo.toml"]
```

Run it:

```sh
don start
```

Don will:
1. Start postgres (docker)
2. Wait for it to accept connections (TCP ready check)
3. Run migrations
4. Start api and worker in parallel (both depend on migrate)
5. Watch for file changes and rebuild/restart automatically

## Features

### Services

Long-running processes (servers, databases, workers). Don keeps them alive and restarts them on file changes.

```toml
[services.api]
run.cmd = "node"
run.args = ["server.js"]
env = { PORT = "3000" }
watch = ["src/**/*.js"]
ready.http = "http://localhost:3000/health"
shutdown.signal = "SIGTERM"
shutdown.timeout = "5s"
```

### Tasks

One-shot commands that run to completion (migrations, codegen, seeding). Only re-run when watched files change.

```toml
[tasks.migrate]
cmd = "dbmate"
args = ["up"]
depends_on = ["postgres"]
watch = ["db/migrations/**/*.sql"]
```

### Dependency Graph

Services and tasks declare `depends_on`. Don topologically sorts them and starts everything in parallel, gating on ready checks:

```toml
[services.db]
# ...
ready.tcp = "127.0.0.1:5432"

[tasks.migrate]
depends_on = ["db"]

[services.api]
depends_on = ["migrate"]
```

### Ready Checks

Don waits for services to be ready before starting dependents:

- **TCP**: `ready.tcp = "127.0.0.1:5432"` — connects to a port
- **HTTP**: `ready.http = "http://localhost:3000/health"` — expects 2xx
- **Exec**: `ready.exec = { cmd = "pg_isready" }` — expects exit code 0

```toml
ready.interval = "500ms"   # how often to check (default: 1s)
ready.retries = 30         # max attempts (default: 30)
```

### File Watching

Services with `watch` patterns automatically rebuild and restart on changes:

```toml
[services.api]
watch = ["src/**/*.rs", "Cargo.toml"]
ignore = ["src/generated/**"]
debounce = "500ms"   # default: 200ms
build.cmd = "cargo"
build.args = ["build", "--bin", "api"]
```

### Docker Services

Run containers alongside native processes:

```toml
[services.postgres]
docker.image = "postgres:16"
docker.ports = ["5432:5432"]
docker.volumes = ["pgdata:/var/lib/postgresql/data"]
docker.env_file = [".env"]
```

Build from a Dockerfile:

```toml
[services.api]
docker.image = "my-api:dev"
docker.build.context = "."
docker.build.dockerfile = "Dockerfile.dev"
docker.ports = ["3000:3000"]
```

### Presets

Built-in support for Rust and Go with automatic build commands and default watch patterns:

```toml
# Rust — runs `cargo build --bin api`, watches src/**/*.rs
[services.api]
rust.binary = "api"
rust.features = ["dev"]

# Go — runs `go build -o .don/bin/api ./cmd/api`, watches **/*.go
[services.api]
go.package = "./cmd/api"
go.ldflags = "-X main.version=dev"
```

### Downloads

Fetch, verify, and cache binary artifacts per-platform:

```toml
[services.crdb]
run.cmd = "cockroach"
run.args = ["start-single-node", "--insecure"]

[services.crdb.download.platform.linux-x86_64]
url = "https://binaries.cockroachdb.com/cockroach-v25.4.0.linux-amd64.tgz"
sha256 = "c07247f245426f6d94e2f901f848946fa50d179cd8409422608805475bc95c51"
path = "cockroach-v25.4.0.linux-amd64/cockroach"
```

Cached in `.don/cache/`, symlinked to `.don/bin/`, and added to child PATH.

### Bazel Integration

Point a service at a Bazel target and Don handles everything — build, run, watch, and rebuild:

```toml
[services.api]
bazel.target = "//services/api:api"
proxy = { listen = "127.0.0.1:8080", env = "PORT" }
```

Don will:
1. Query `bazel query` to discover source packages → auto-set watch patterns
2. Run `bazel build` at startup (batched across all targets)
3. Resolve the output binary via `bazel cquery` and run it directly
4. Watch for source changes and rebuild/restart automatically
5. Watch BUILD files and re-query the build graph when they change

Multiple services sharing the same source files are batched into one `bazel build` invocation.

### Turborepo Integration

For monorepos using Turborepo, Don auto-resolves the task graph:

```toml
[services.web]
turbo.task = "dev"
turbo.filter = "@myorg/web"
proxy = { listen = "127.0.0.1:3000", env = "PORT" }
```

Don queries `turbo run --dry-run=json` to discover workspace dependencies and input files, then watches them for changes. At startup, a batch `turbo run build` runs for all configured packages.

### TCP Proxy

Don listens on a port and forwards connections to the service on an ephemeral port. The proxy stays open across restarts — no dropped connections:

```toml
[services.api]
run.cmd = "./api-server"
proxy = { listen = "127.0.0.1:3000", env = "PORT" }
```

Don injects `PORT=<ephemeral>` into the service's environment. On restart, the proxy queues new connections while the service restarts. Supports multiple proxy entries and lazy start (delay service startup until first connection):

```toml
[services.api]
proxy = { listen = "127.0.0.1:3000", env = "PORT" }
lazy = true
```

### Socket Passing

Zero-downtime restarts via the systemd `LISTEN_FDS` protocol. Don binds the port and passes the socket fd to the child:

```toml
[services.api]
run.cmd = "./api-server"
listen = ["127.0.0.1:3000"]
watch = ["src/**/*.rs"]
```

During a file-watch restart, the port stays bound (connections queue in the kernel backlog).

### Profiles

Run a subset of services for focused work:

```toml
[profiles.frontend]
services = ["api"]
tasks = ["migrate"]

[profiles.backend]
services = ["api", "worker"]
tasks = ["migrate"]
```

```sh
don start --profile frontend
```

Transitive dependencies are included automatically — if `api` depends on `postgres`, it starts too.

### Config Auto-Reload

Edit `don.toml` while don is running. Don detects the change, diffs it, and applies it live:
- Added services start
- Removed services stop
- Changed services restart with the new config
- Invalid configs are rejected (old config continues)

### CLI Commands

```sh
don start                    # start the daemon (or don with no args)
don start --profile <name>   # start a subset
don start <name>             # start a stopped service
don stop <name>              # stop a running service
don restart <name>           # restart a service
don status                   # show all services and their states
don status -v                # verbose: watch paths, ports, commands, build targets
don logs <name>              # view recent output
don logs <name> --follow     # stream output
don logs <name> --last 50    # last N lines
don validate                 # check config without starting
don cleanup                  # remove stale state from a crashed run
don cleanup --force          # kill a running daemon and clean up
```

### Daemon API

Don exposes a unix socket API at `.don/don.sock` for programmatic control:

```
GET  /status              → service/task states
POST /start/:name         → start a stopped service
POST /stop/:name          → stop a service
POST /restart/:name       → restart a service
GET  /logs/:name?last=N   → ring buffer output
GET  /logs/:name?follow=true → streaming NDJSON
```

### Terminal Safety

Service output is sanitized before display — colors and text styles pass through, but cursor movement, screen clearing, and alternate screen mode are stripped. Rogue ncurses apps can't corrupt don's terminal.

### Graceful Shutdown

- First Ctrl+C: graceful shutdown in reverse dependency order (dependents stop first), respecting per-service `shutdown.signal` and `shutdown.timeout`
- Second Ctrl+C: immediate SIGKILL on all processes
- Running tasks are killed
- PID files, sockets, and docker containers are cleaned up

### Crash Recovery

If don crashes, the next `don start` automatically:
- Detects orphaned service processes via `(pgid, start_time)` identity
- Kills confirmed orphans (safe against PID recycling)
- Removes stale PID files, sockets, and docker containers

## Configuration Reference

See [`examples/`](examples/) for complete working configs.

| Field | Type | Description |
|-------|------|-------------|
| `run.cmd` | string | Command to execute |
| `run.args` | [string] | Arguments |
| `dir` | string | Working directory |
| `env` | {key: value} | Environment variables |
| `env_file` | [string] | Env files to load |
| `depends_on` | [string] | Services/tasks to wait for |
| `watch` | [string] | Glob patterns to watch for changes |
| `ignore` | [string] | Glob patterns to exclude from watch |
| `debounce` | string | Debounce duration ("200ms", "1s") |
| `listen` | [string] | TCP addresses for socket passing |
| `ready.tcp` | string | TCP ready check address |
| `ready.http` | string | HTTP ready check URL |
| `ready.exec` | {cmd, args} | Exec ready check command |
| `ready.interval` | string | Check interval (default: "1s") |
| `ready.retries` | u32 | Max attempts (default: 30) |
| `shutdown.signal` | string | Shutdown signal (default: "SIGTERM") |
| `shutdown.timeout` | string | Grace period (default: "10s") |
| `log` | string | Output routing: "stdout", "ignore", or a file path |
| `docker.image` | string | Docker image |
| `docker.ports` | [string] | Port mappings |
| `docker.volumes` | [string] | Volume mounts |
| `docker.build` | table | Dockerfile build config |
| `rust.binary` | string | Rust binary target name |
| `go.package` | string | Go package path |
| `proxy` | string or table | TCP proxy: `"addr"` or `{ listen, env }` |
| `lazy` | bool | Delay start until first proxy connection |
| `bazel.target` | string | Bazel target label (auto watch/build/run) |
| `bazel.query_timeout` | u64 | Query timeout in seconds |
| `turbo.task` | string | Turborepo task name |
| `turbo.filter` | string | Turborepo package filter |
| `turbo.build_task` | string | Task to run during batch build (default: "build") |
| `download.platform.<platform>` | table | Per-platform download config |

## Platform Support

Linux and macOS. Windows is not supported (relies on Unix sockets, process groups, signals, and `LISTEN_FDS`).

## License

MIT
