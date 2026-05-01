# Don Module Map

Use this map to route changes to the right part of the codebase.

## Entry Points

- `src/lib.rs`: public library API and re-exports.
- `src/main.rs`: thin CLI wrapper. Keep business logic out of this file.
- `src/init.rs`: `don init` behavior.
- `src/completions.rs`: shell completion support.

## Config

- `src/config/mod.rs`: config loading, parsing, validation, suggestions.
- `src/config/service.rs`: service presets, resolved services, overrides.
- `src/config/task.rs`: task configuration.
- `src/config/profile.rs`: profile configuration.
- `src/config/param.rs`: task parameter definitions and validation.
- `src/config/platform.rs`: platform-specific config.
- `src/config/download.rs`: download config and cache paths.
- `src/config/types.rs`: shared config types such as commands, ready checks,
  shutdown, and logging.
- `src/config/template.rs`: template substitution.

## Runtime Orchestration

- `src/runner/mod.rs`: command loop, dependency graph, startup and shutdown flow.
- `src/runner/service.rs`: service lifecycle, spawn, restart, stop.
- `src/runner/task.rs`: one-shot task execution, timeout, skip-if-unchanged.
- `src/runner/state.rs`: runtime state types.
- `src/runner/params.rs`: task parameter runtime handling.
- `src/runner/completions.rs`: runtime completion plumbing.

## External Resources

- `src/process/mod.rs`: process group management and PTY/pipe spawning.
- `src/process/pid_file.rs`: PID file locking and single-instance guard.
- `src/process/identity.rs`: process identity checks for crash recovery.
- `src/process/cleanup.rs`: stale state cleanup.
- `src/process/env.rs`: env-file parsing and environment merging.
- `src/process/socket.rs`: socket binding and `LISTEN_FDS` fd passing.
- `src/docker/mod.rs`: Docker lifecycle.
- `src/docker/build.rs`: Docker image build streaming.
- `src/docker/parse.rs`: Docker config parsing helpers.
- `src/docker/stream.rs`: Docker log stream adapter.
- `src/download.rs`: artifact download, SHA-256 verification, extraction, caching.

## User Experience

- `src/output/mod.rs`: line buffering, service prefixes, color assignment, output sinks.
- `src/output/ring_buffer.rs`: bounded per-service output history.
- `src/output/sanitize.rs`: ANSI escape filtering.
- `src/output/osc.rs`: OSC sequence handling.
- `src/tui/`: terminal UI state, rendering, input, palette, log store, and events.

## API And Client

- `src/server/mod.rs`: Unix-socket HTTP API server.
- `src/server/routes.rs`: status, start, stop, restart, logs, and follow routes.
- `src/server/attach.rs`: attach support.
- `src/client/mod.rs`: HTTP-over-Unix-socket client.
- `src/client/attach.rs`: client attach behavior.

## Watch And Build Tools

- `src/watch/mod.rs`: file watching, debounce, rebuild/restart state machine,
  config reload.
- `src/build_tool/mod.rs`: build-tool abstractions.
- `src/build_tool/bazel/`: Bazel graph/query integration.
- `src/build_tool/turbo.rs`: Turbo integration.
- `src/task_state.rs`: task input hashing for skip detection.

## Tests

- `tests/helpers/`: temp dirs, config builders, free ports, and timeout helpers.
- `tests/*_test.rs`: integration tests by feature area.
- Unit tests should live beside the code when they only exercise module-local logic.
