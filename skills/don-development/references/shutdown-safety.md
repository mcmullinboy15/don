# Shutdown Safety Guide

Don's runner must remain interruptible. A user pressing Ctrl+C should not be trapped
behind a slow build, download, subprocess, query, or lock wait.

## Required Reasoning

For every new startup, rebuild, watch, download, Docker, build-tool, or process path,
answer:

1. What external work can block here?
2. Which task owns that work?
3. What happens if shutdown is requested while it is running?
4. Is dropping the future enough to stop the external work?
5. If not, where is the explicit abort or kill path?

## Acceptable Patterns

- Race long-running work with a shutdown signal using `tokio::select!`.
- Use subprocess APIs with `kill_on_drop` or equivalent cleanup.
- Spawn blocking or non-cancellable work off the runner and report back over a channel.
- Keep process-exit authority outside the runner so `main` can force exit after a
  bounded grace period if the runner wedges.
- Prefer request/reply channels for status or control queries instead of shared locks.

## Risky Patterns

- Awaiting a child process directly from the runner without cancellation cleanup.
- Awaiting network downloads from the runner without racing shutdown.
- Waiting on file locks, Docker calls, build-tool queries, or archive extraction from
  the runner with no timeout or abort path.
- Adding `Arc<Mutex<_>>` around runner-owned state to make status queries easier.
- Deferring cleanup to code that only runs after an unbounded await completes.

## Review Language

When reviewing shutdown-sensitive code, state the concrete behavior:

- Good: "If Ctrl+C arrives while the build is running, the select branch drops the
  build future and the child has kill-on-drop enabled, so the runner proceeds to
  service cleanup."
- Bad: "This should be fine because the build usually finishes quickly."
