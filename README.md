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

# Or via Homebrew
brew install pjtatlow/tap/don
```

## Quick Start

Run `don init` to drop a starter `don.toml` in the current directory, or create one by hand:

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

### Interactive TUI

When stdout is a TTY, `don start` runs a ratatui-driven interface: logs stream into native scrollback while a bordered status bar pinned to the bottom shows ready counts, running tasks, a spinner during transitions, and contextual key hints.

| Key | Action |
|-----|--------|
| `l` | Filter services/tasks — space toggles, enter commits, esc clears |
| `a` | Action palette — Start / Stop / Restart / Rebuild / run pending tasks |
| `s` | Full-screen service/task status overlay |
| `Enter` | Insert a blank line separator into scrollback |
| `q` or Ctrl+C | Graceful shutdown (second press force-kills) |

Pipe mode (non-TTY) writes prefixed lines directly to stdout unchanged.
Configs with active foreground terminal tasks also use plain prefixed output
instead of the TUI so those tasks can own stdin without competing with Don's
keyboard handler.

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

Set `reload = false` to opt out of file-watch restarts — useful for services that handle their own hot-reloading (vite, webpack dev server, etc.):

```toml
[services.web]
run.cmd = "npm"
run.args = ["run", "dev"]
watch = ["src/**/*.tsx"]
reload = false   # don tracks changes for status, but doesn't restart
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

Set `auto_run = false` to defer execution — when the task actually needs to run (because watched inputs changed, or another item depends on it), it moves to `pending_run` until you explicitly trigger it:

```toml
[tasks.seed]
cmd = "./scripts/seed-db"
auto_run = false
depends_on = ["migrate"]
```

Run deferred tasks with `don run --all-pending` or from the TUI action palette.

Set `auto_run = "once"` to run a task automatically on startup until it has
one successful run, then require manual triggers forever after:

```toml
[tasks.bootstrap]
cmd = "./scripts/bootstrap-db"
auto_run = "once"
depends_on = ["postgres"]
```

Some tasks need the real terminal: REPLs, editors, interactive migrations, test
watchers, or anything that expects stdin and unprefixed output. Mark those as
foreground terminal tasks:

```toml
[tasks.console]
cmd = "rails"
args = ["console"]
depends_on = ["postgres"]
terminal = "foreground"
```

Foreground tasks run as part of normal startup and file-watch re-runs. When one
becomes ready, Don pauses visible Don output, gives the task stdin/stdout/stderr,
and waits for it to exit before starting other newly-ready services or tasks.
Already-running dependencies keep running, and their output is still captured in
ring buffers and log files.

`terminal = "foreground"` uses the alternate screen by default, giving the task a
clean full-screen workspace that disappears when it exits. Use the main screen
when you want the task's output to remain in normal scrollback:

```toml
terminal = { mode = "foreground", screen = "main" }
```

Tasks can also declare parameters. Parametrized tasks are interactive: values are supplied at run time via `don run <task> --<name>=<value>` or the TUI form, then substituted into `cmd`, `args`, `env`, and `dir` via `{{name}}` placeholders.

```toml
[tasks.sync]
cmd = "sh"
args = ["-c", "echo index={{index}} batch={{batch_size}} dry_run=$DON_PARAM_DRY_RUN"]
auto_run = false

[[tasks.sync.params]]
name = "index"
required = true

[[tasks.sync.params]]
name = "batch_size"
kind = "int"
default = "100"
validate = { min = 1, max = 10000 }

[[tasks.sync.params]]
name = "dry_run"
kind = "bool"
default = "false"
```

Run it with:

```sh
don run sync --index=users --batch_size=500 --dry_run
```

Accepted CLI forms are `--name=value`, `--name value`, and bare `--flag` for bool params. Task params are also exported to the child process as `DON_PARAM_<NAME>` environment variables.

For fixed or dynamic candidate values, use `choices` or `completions`:

```toml
[[tasks.deploy.params]]
name = "environment"
choices = ["dev", "staging", "prod"]

[[tasks.deploy.params]]
name = "service"

[tasks.deploy.params.completions]
cmd = "./scripts/list-services"
args = ["--json"]
parse = "json"
cache = "5m"
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

### Health Monitoring & Auto-Restart

Set `ready.monitor = true` to keep polling the ready check after startup. Consecutive failures mark the service `unhealthy`; `on_failure` decides what happens next:

```toml
[services.api]
run.cmd = "./api-server"
ready.http = "http://localhost:3000/health"
ready.monitor = true              # keep checking after Ready
ready.monitor_interval = "2s"     # interval while monitoring (default: ready.interval)
ready.unhealthy_after = 3         # consecutive failures → Unhealthy (default: 3)
on_failure = "restart"            # "notify" (default) or "restart"
```

`on_failure` also fires when the process exits with a non-zero status (or terminating signal) — clean exits still transition to `stopped`. Restarts use escalating backoff (1, 2, 4, 8, 16, 32, capped at 60s) and reset when the service recovers to Ready.

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
default_profile = "frontend"   # used by bare `don start` (optional)

[profiles.frontend]
services = ["api"]
tasks = ["migrate"]

[profiles.backend]
services = ["api", "worker"]
tasks = ["migrate"]
```

```sh
don start                       # uses default_profile, or everything if unset
don start --profile backend     # override
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
don init                     # scaffold a starter don.toml
don start                    # start the daemon (bare `don` prints help)
don start --profile <name>   # start a subset
don start <name>             # start a stopped service in the running daemon
don stop <name>              # stop a running service
don restart <name>           # restart a service
don status                   # show all services and their states
don status -v                # verbose: watch paths, ports, commands, build targets
don logs <name>              # view recent output
don logs <name> --follow     # stream output
don logs <name> --last 50    # last N lines
don attach <name>            # attach stdin/stdout to a running service
don run <name>               # run a specific task (bypasses auto_run)
don run --all-pending        # run all tasks sitting in pending_run
don exec <cmd> [args...]     # run a command with .don/bin on PATH
don validate                 # check config without starting
don cleanup                  # remove stale state from a crashed run
don cleanup --force          # kill a running daemon and clean up
don completions <shell>      # print a completion script for bash/zsh/fish/...
```

Completions are dynamic: service, task, and profile names from your `don.toml` tab-complete on subcommands that take them (`stop`, `restart`, `run`, `logs`, `attach`). Install with e.g. `don completions bash > ~/.local/share/bash-completion/completions/don` or `don completions zsh > "${fpath[1]}/_don"`.


### Daemon API

Don exposes a unix socket API at `.don/don.sock` for programmatic control:

```
GET  /status                 → service/task states
POST /start/:name            → start a stopped service
POST /stop/:name             → stop a service
POST /restart/:name          → restart a service
POST /run/:name              → run a specific task (bypasses auto_run)
POST /run-pending            → run all tasks in pending_run state
GET  /logs/:name?last=N      → ring buffer output
GET  /logs/:name?follow=true → streaming NDJSON
GET  /attach/:name           → raw-stream attach (stdin/stdout)
```

### Terminal Safety

Service output is sanitized before display — colors and text styles pass through, but cursor movement, screen clearing, and alternate screen mode are stripped. Rogue ncurses apps can't corrupt don's terminal.

### Graceful Shutdown

- First Ctrl+C: graceful shutdown in reverse dependency order (dependents stop first), respecting per-service `shutdown.signal` and `shutdown.timeout`
- Second Ctrl+C: immediate SIGKILL on all processes
- Running tasks are killed
- PID files, sockets, and docker containers are cleaned up

### Crash Recovery

Managed service crashes (non-zero exits or terminating signals) route through `on_failure`: `"notify"` marks the service failed and emits a lifecycle event; `"restart"` reuses the same backoff machinery as monitor failures.

If don itself crashes, the next `don start` automatically:
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
| `ready.monitor` | bool | Keep polling after Ready to detect unhealthy (default: false) |
| `ready.monitor_interval` | string | Poll interval while monitoring (default: "10s") |
| `ready.unhealthy_after` | u32 | Consecutive monitor failures → Unhealthy (default: 3) |
| `on_failure` | string | `"notify"` or `"restart"` on crash/unhealthy (default: "notify") |
| `reload` | bool | Whether file-watch events restart the service (default: true) |
| `auto_run` | bool or string | (tasks) `true`/`"always"`, `false`/`"never"`, or `"once"` for startup-only until first success (default: true) |
| `terminal` | string or table | (tasks) `"muxed"` default, or `"foreground"` for exclusive stdin/stdout/stderr ownership |
| `terminal.mode` | string | (tasks) `"muxed"` or `"foreground"` |
| `terminal.screen` | string | (tasks) `"alternate"` default for foreground, or `"main"` to keep output in scrollback |
| `params` | [[table]] | (tasks) Declare run-time parameters for interactive tasks |
| `params.name` | string | Parameter name, referenced as `{{name}}` and passed as `--name=value` |
| `params.prompt` | string | Optional prompt shown in the TUI form |
| `params.required` | bool | Require an explicit value unless `default` is set |
| `params.default` | string | Default value when the user omits the param |
| `params.kind` | string | `"string"`, `"int"`, `"bool"`, or `"choice"` |
| `params.choices` | [string] | Fixed candidate values; constrains the accepted set |
| `params.validate.min` | i64 | Minimum allowed value for `kind = "int"` |
| `params.validate.max` | i64 | Maximum allowed value for `kind = "int"` |
| `params.completions.cmd` | string | Command to resolve dynamic candidate values |
| `params.completions.args` | [string] | Arguments for the completions command |
| `params.completions.parse` | string | Parse mode: `"lines"`, `"null_separated"`, or `"json"` |
| `params.completions.cache` | string | Cache TTL for completion results |
| `params.completions.timeout` | string | Completion command timeout (default: `"10s"`) |
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
| `turbo.task` | string | Turborepo task name |
| `turbo.filter` | string | Turborepo package filter |
| `turbo.build_task` | string | Task to run during batch build (default: "build") |
| `download.platform.<platform>` | table | Per-platform download config |
| `default_profile` | string | Top-level: profile used by bare `don start` |

## Platform Support

Linux and macOS. Windows is not supported (relies on Unix sockets, process groups, signals, and `LISTEN_FDS`).

## License

MIT
