# Don Review Checklist

Use this checklist for code review or before declaring an implementation complete.

## Process And Resource Safety

- Does any new fallible production path use typed errors instead of `unwrap()`,
  `expect()`, or `panic!()`?
- Could any slice or vector indexing panic? If yes, replace it with `.get()` and a
  handled error path.
- Are PID files, sockets, children, Docker resources, and temporary state owned by
  RAII guards or explicit cleanup paths?
- Is the PID file lock acquired before spawning a child process?
- Can stale `.don/` state be detected and cleaned up after crashes?

## Shutdown Responsiveness

- What happens if the user presses Ctrl+C at each new await point?
- Is long-running external work raced against shutdown?
- If a subprocess future is dropped, does the subprocess stop or have an abort path?
- If the runner delegates work to a task, does the runner remain able to process
  shutdown commands?
- Are lock waits bounded, cancellable, or moved off the runner task?

## Config And Developer Experience

- Is all config validated before starting services or tasks?
- Are errors specific and actionable, with service/task names and useful suggestions?
- Does the output remain scannable and avoid unnecessary terminal noise?
- Does the change respect `.don/` as the only mutable state directory?

## API And Module Boundaries

- Are new public items really part of the library API?
- Does every `pub` item have documentation?
- Are internals `pub(crate)` or private?
- Are modules communicating through clear types or tokio channels rather than shared
  mutable state?

## Tests

- Are tests table-driven when multiple cases exercise the same behavior?
- Do filesystem tests use temp directories?
- Do integration tests use `run_with_timeout()` or another bounded timeout?
- Do process tests work without requiring a real TTY?
- Are both success and failure paths covered for resource-management changes?
