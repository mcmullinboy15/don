---
name: don-development
description: Build, debug, review, and test Don, a Rust dev-environment orchestrator, while preserving process safety and shutdown behavior.
---

# Don Development

Use this skill when working on Don's Rust library, CLI, runner, process management,
watching, Docker, download, TUI, config, or tests.

## Start Here

1. Read the repo instructions first: `AGENTS.md` or `CLAUDE.md`.
2. For architectural questions or larger changes, read `docs/design.md`.
3. For implementation work, inspect the relevant module before proposing changes.
4. Keep changes small, typed, and testable. Prefer extending existing module patterns.

## Non-Negotiables

- Do not add `unwrap()`, `expect()`, `panic!()`, unchecked indexing, or `unreachable!()`
  in production code.
- Do not use `anyhow` in library modules. Use module-specific `thiserror` errors.
- Keep the runner interruptible. Any long-running process, build, download, query, or
  lock wait awaited from the runner must be cancellation-safe or raced against shutdown.
- Do not introduce shared mutable runner state via `Arc<Mutex<_>>`; communicate with
  `mpsc`, `broadcast`, and `oneshot` channels.
- Never store Don state outside `.don/`.
- Do not make internal implementation details `pub`; default to private or `pub(crate)`.

## Common Workflows

### Implementing a Feature

1. Identify the owning module from `references/module-map.md`.
2. Validate config and user input before starting resources.
3. Preserve library-first design: business logic belongs in `src/lib.rs` modules, not
   only in `src/main.rs`.
4. Add or update table-driven tests. Integration tests should use helpers in
   `tests/helpers/` and timeouts.
5. Run the narrowest useful test first, then run broader validation if time allows.

### Reviewing Changes

Use `references/review-checklist.md` for code review. Prioritize process leaks,
stale PID/socket state, shutdown deadlocks, missing validation, and unhelpful errors.

### Working on Shutdown or Process Code

Read `references/shutdown-safety.md` before editing runner, process, watch, download,
Docker, or build-tool paths. In the final answer or review, explicitly state what
happens if the user presses Ctrl+C at the risky await points.

## Validation Commands

Prefer targeted commands while iterating:

```sh
cargo test <test-name>
cargo test --test <integration-test-name>
cargo clippy -- -D warnings
```

Before considering a broad Rust change complete, the expected full validation is:

```sh
cargo test
cargo clippy -- -D warnings
```

If validation cannot be run, state that and explain the remaining risk.
