#!/usr/bin/env python3
"""Drive `don start` in TUI mode under a real PTY and check what it renders.

Why this exists
---------------
Pipe-mode integration tests cover the runner and `OutputManager`, but not
ratatui rendering, the input task, or the interplay between them and the
merged log stream. Several of the worst regressions in this codebase have been
TUI-only: pipe mode fine, TUI hangs or freezes or loses lifecycle events under
load.

This drives the real thing under a PTY, renders its output into a screen (see
`tui_emulator.py`), and asserts on what the user would actually see:

  - the stack reaches N/N services ready
  - Ctrl+C reaches the TUI and shutdown narrates itself
  - don exits promptly rather than wedging
  - the alternate screen is handed back on the way out

That last check matters and is easy to lose: leaving the alternate screen up
after exit dumps the user back into a terminal whose scrollback appears to
have vanished.

Note for anyone updating this: assertions are on the *screen*, never on the
raw byte stream. A full-screen TUI writes text split across escape sequences
and repaints regions repeatedly, so a byte-stream grep both misses lines that
are on screen and finds lines that scrolled off long ago.

Usage
-----
    python3 tools/tui_drive.py <path-to-don-binary> <config-dir> [linger]

Example, against the synthetic stress config in this repo:

    python3 tools/gen_stress_config.py /tmp/don-stress
    cargo build --release
    rm -rf /tmp/don-stress/.don
    python3 tools/tui_drive.py target/release/don /tmp/don-stress 4 \\
        > /tmp/tui-stdout.bin 2> /tmp/tui-stderr.log
    tail -20 /tmp/tui-stderr.log

`captured bytes` is a useful regression signal: a jump from tens of KB to
several MB without a config change means the TUI started repainting far more
than it should.
"""

import re
import sys
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from tui_emulator import HAVE_PYTE, Session  # noqa: E402

# The status bar counts ready services — "11/11 services ready". The
# backreference is what makes this an assertion rather than a formality: it
# only matches once every service the bar counts has come up, and requiring a
# non-zero count keeps an empty "0/0" from matching before the config loads.
#
# Deliberately not the runner's "all services running" lifecycle line, which
# this used to look for. That line is still emitted, but it lands in the log
# pane and scrolls away under any real load — the check passed or failed
# depending on how chatty the config was. The bar stays put.
READY = re.compile(r"\b([1-9]\d*)/\1 services ready\b")
SHUTTING_DOWN = "shutting down"
STARTUP_TIMEOUT = 90.0
SHUTDOWN_TIMEOUT = 30.0


def tlog(start, message):
    print("[%6.0fms] %s" % ((time.time() - start) * 1000, message), file=sys.stderr)


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    binary, project = sys.argv[1], sys.argv[2]
    linger = float(sys.argv[3]) if len(sys.argv) > 3 else 4.0

    start = time.time()
    session = Session(binary, project)
    tlog(start, "spawned don pid=%d (pyte: %s)" % (session.pid, HAVE_PYTE))

    reached_ready = session.wait_for_screen(READY, STARTUP_TIMEOUT)
    tlog(start, "reached %r on screen: %s" % (READY.pattern, reached_ready))
    if not reached_ready:
        tlog(start, "FAIL: startup never settled; last screen follows")
        print(session.text(), file=sys.stderr)

    entered_alt = session.screen.alt
    tlog(start, "alternate screen entered: %s" % entered_alt)

    session.pump(linger)
    session.settle()
    bytes_at_steady = len(session.raw)
    tlog(start, "captured %d bytes by steady state" % bytes_at_steady)

    # A quiet period should cost almost nothing: the loop marks state dirty and
    # draws at most once a frame, and an unchanged screen diffs to nothing.
    session.pump(2.0)
    idle_bytes = len(session.raw) - bytes_at_steady
    tlog(start, "bytes written during 2s idle: %d" % idle_bytes)

    session.interrupt()
    tlog(start, "sent Ctrl+C")
    saw_shutdown = session.wait_for_screen(SHUTTING_DOWN, 10.0)
    tlog(start, "saw %r on screen: %s" % (SHUTTING_DOWN, saw_shutdown))

    code = session.wait_exit(SHUTDOWN_TIMEOUT)
    if code is None:
        tlog(start, "HANG: don still alive %.0fs after Ctrl+C" % SHUTDOWN_TIMEOUT)
        print(session.text(), file=sys.stderr)
        session.kill()
    else:
        tlog(start, "don exit code: %d" % code)

    left_alt = not session.screen.alt
    tlog(start, "alternate screen handed back: %s" % left_alt)
    tlog(start, "captured bytes total: %d" % len(session.raw))

    sys.stdout.buffer.write(bytes(session.raw))

    ok = reached_ready and entered_alt and saw_shutdown and left_alt and code == 0
    tlog(start, "RESULT: %s" % ("ok" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
