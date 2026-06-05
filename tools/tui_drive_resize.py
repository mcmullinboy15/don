#!/usr/bin/env python3
"""Drive `don start` under a real PTY and exercise terminal RESIZE.

Companion to `tui_drive.py`, which covers the shutdown path. This one
targets the resize path specifically: it starts don, waits for steady
state (a large log burst has filled the in-memory `LogStore`), then
sends a sequence of real window-size changes (`ioctl(TIOCSWINSZ)` +
`SIGWINCH`) and measures:

  - resize_bytes: how many bytes don writes to the terminal in response
    to the resizes. A full clear+replay of the LogStore shows up here as
    a multi-MB spike; a bar-only redraw is a few KB.
  - ghost bars: the captured stream is replayed into a `pyte` screen
    (resized at the same offsets the real PTY was) and the final on-screen
    state is inspected. The status bar carries the marker "[s] services";
    if it appears on more than one row, or the box border (┌──) appears
    more than once, a stale "ghost" copy of the bar lingered after resize.

Usage:
    python3 tools/tui_drive_resize.py <don binary> <cwd>

Exit code 0 = clean (don exited 0, no ghost). Non-zero otherwise.
Requires `pyte` for the ghost check; without it, only byte accounting
is reported.
"""

import os
import pty
import re
import select
import signal
import struct
import sys
import time
import fcntl
import termios

if len(sys.argv) < 3:
    print(f"usage: {sys.argv[0]} <don binary> <cwd>", file=sys.stderr)
    sys.exit(2)

DON = os.path.realpath(sys.argv[1])
CWD = sys.argv[2]
if not os.access(DON, os.X_OK):
    print(f"binary not found or not executable: {DON}", file=sys.stderr)
    sys.exit(2)

try:
    import pyte
    HAVE_PYTE = True
except ImportError:
    HAVE_PYTE = False

ANSI = re.compile(rb"\x1b\[[0-9;]*[a-zA-Z]|\x1b\][^\x07]*\x07")
DSR = re.compile(rb"\x1b\[6n")
READY_NEEDLE = b"all services running"
SHUTDOWN_NEEDLE = b"shutdown complete"

# (rows, cols) sequence. Index 0 is the initial size; the rest are the
# resizes applied after steady state, in order. Override with the env var
# DON_RESIZE_SIZES, e.g. "50x140,50x100" for a single width-only resize.
SIZES = [(50, 120), (40, 100), (55, 150), (44, 90)]
if os.environ.get("DON_RESIZE_SIZES"):
    SIZES = [tuple(int(x) for x in pair.split("x"))
             for pair in os.environ["DON_RESIZE_SIZES"].split(",")]

start = time.monotonic()
def tlog(msg):
    sys.stderr.write(f"[{int((time.monotonic() - start) * 1000)}ms] {msg}\n")
    sys.stderr.flush()

def set_winsize(fd, rows, cols):
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))

pid, fd = pty.fork()
if pid == 0:
    os.chdir(CWD)
    os.environ["TERM"] = "xterm-256color"
    os.environ["COLUMNS"] = str(SIZES[0][1])
    os.environ["LINES"] = str(SIZES[0][0])
    os.execvp(DON, [DON, "start"])
    os._exit(127)

tlog(f"spawned don pid={pid}")

flags = fcntl.fcntl(fd, fcntl.F_GETFL)
fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
set_winsize(fd, *SIZES[0])

captured = bytearray()
# (byte_offset_in_captured, (rows, cols)) for replaying into pyte.
resize_marks = []
saw_ready = False
ready_at = None
saw_shutdown = False
shutdown_seen_at = None
sigint_sent = False
sigint_at = None
pre_sigint_offset = None
exit_code = None

# Resize state machine, driven once steady-state is reached.
resize_phase = 0           # index into SIZES[1:]
pre_resize_offset = None
post_resize_offset = None
last_resize_at = None
RESIZE_GAP = 0.6           # seconds between resizes
STEADY_LINGER = 3.0        # seconds after ready before first resize

deadline = time.monotonic() + 90

def drain_reads():
    """Read whatever is available; answer DSR. Returns False on EOF."""
    try:
        data = os.read(fd, 65536)
    except OSError as e:
        if getattr(e, "errno", None) == 5:
            return False
        return False
    if not data:
        return False
    captured.extend(data)
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()
    for _ in DSR.finditer(data):
        try:
            os.write(fd, b"\x1b[1;1R")
        except OSError:
            pass
    return True

while True:
    try:
        rlist, _, _ = select.select([fd], [], [], 0.05)
    except (OSError, select.error):
        break

    if rlist:
        if not drain_reads():
            tlog("EOF on master")
            break
        plain = ANSI.sub(b"", bytes(captured))
        if not saw_ready and READY_NEEDLE in plain:
            saw_ready = True
            ready_at = time.monotonic()
            tlog(f"saw '{READY_NEEDLE.decode()}'")
        if not saw_shutdown and SHUTDOWN_NEEDLE in plain:
            saw_shutdown = True
            shutdown_seen_at = time.monotonic()
            tlog(f"saw '{SHUTDOWN_NEEDLE.decode()}'")

    now = time.monotonic()

    # Once steady (ready + linger so the burst is fully flushed), walk the
    # resize sequence.
    if saw_ready and not sigint_sent and now - ready_at >= STEADY_LINGER:
        if pre_resize_offset is None:
            pre_resize_offset = len(captured)
            last_resize_at = 0  # fire first resize immediately
            tlog(f"steady; pre_resize_offset={pre_resize_offset}")
        if resize_phase < len(SIZES) - 1 and now - last_resize_at >= RESIZE_GAP:
            rows, cols = SIZES[1 + resize_phase]
            try:
                set_winsize(fd, rows, cols)
                os.kill(pid, signal.SIGWINCH)
                resize_marks.append((len(captured), (rows, cols)))
                tlog(f"resize -> {rows}x{cols}")
            except Exception as e:
                tlog(f"resize failed: {e}")
            resize_phase += 1
            last_resize_at = now
        elif resize_phase >= len(SIZES) - 1 and now - last_resize_at >= 1.0:
            # All resizes sent + settled. Snapshot, then SIGINT.
            post_resize_offset = len(captured)
            pre_sigint_offset = len(captured)
            tlog(f"post_resize_offset={post_resize_offset} "
                 f"resize_bytes={post_resize_offset - pre_resize_offset}")
            tlog(f"sending SIGINT to {pid}")
            os.kill(pid, signal.SIGINT)
            sigint_sent = True
            sigint_at = now

    if shutdown_seen_at is not None and now - shutdown_seen_at > 8:
        tlog("HANG: 'shutdown complete' fired but don alive 8s later")
        os.kill(pid, signal.SIGKILL)

    try:
        wpid, status = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        wpid, status = pid, 0
    if wpid:
        if os.WIFEXITED(status):
            exit_code = os.WEXITSTATUS(status)
            tlog(f"don exited normally code={exit_code}")
        elif os.WIFSIGNALED(status):
            exit_code = -os.WTERMSIG(status)
            tlog(f"don killed by signal {os.WTERMSIG(status)}")
        break

    if now > deadline:
        tlog("driver deadline; killing don")
        os.kill(pid, signal.SIGKILL)
    if sigint_at is not None and now - sigint_at > 30:
        tlog("30s after SIGINT — killing")
        os.kill(pid, signal.SIGKILL)

# Drain trailing bytes.
end_drain = time.monotonic() + 0.5
while time.monotonic() < end_drain:
    try:
        rlist, _, _ = select.select([fd], [], [], 0.05)
    except (OSError, select.error):
        break
    if not rlist or not drain_reads():
        break

if exit_code is None:
    grace = time.monotonic() + 2.0
    while time.monotonic() < grace:
        try:
            wpid, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            break
        if wpid:
            if os.WIFEXITED(status):
                exit_code = os.WEXITSTATUS(status)
            elif os.WIFSIGNALED(status):
                exit_code = -os.WTERMSIG(status)
            break
        time.sleep(0.05)

# ---- Ghost analysis via pyte ----
ghost = None
bar_rows = None
if HAVE_PYTE and pre_sigint_offset:
    # Replay the stream up to just before SIGINT, resizing the emulated
    # screen at the same byte offsets the real PTY was resized. The final
    # screen state should show the bar exactly once, at the bottom.
    final_rows, final_cols = SIZES[-1]
    screen = pyte.Screen(SIZES[0][1], SIZES[0][0])
    stream = pyte.ByteStream(screen)
    cursor = 0
    for off, (rows, cols) in resize_marks:
        off = min(off, pre_sigint_offset)
        stream.feed(bytes(captured[cursor:off]))
        screen.resize(rows, cols)
        cursor = off
    stream.feed(bytes(captured[cursor:pre_sigint_offset]))

    lines = screen.display
    bar_rows = [i for i, ln in enumerate(lines) if "[s] services" in ln]
    border_rows = [i for i, ln in enumerate(lines) if "┌" in ln or "┐" in ln]
    ghost = len(bar_rows) > 1 or len(border_rows) > 1
    tlog(f"pyte final {final_rows}x{final_cols}: bar marker rows={bar_rows} "
         f"top-border rows={border_rows}")

plain = ANSI.sub(b"", bytes(captured))
text = plain.decode("utf-8", errors="replace")

tlog("=== resize driver summary ===")
tlog(f"don exit code:   {exit_code}")
tlog(f"saw ready:       {saw_ready}")
tlog(f"saw shutdown:    {saw_shutdown}")
if pre_resize_offset is not None and post_resize_offset is not None:
    tlog(f"resize_bytes:    {post_resize_offset - pre_resize_offset} "
         f"(over {len(SIZES) - 1} resizes)")
tlog(f"captured bytes:  {len(captured)}")
if HAVE_PYTE:
    tlog(f"ghost bar:       {ghost}  (bar marker rows={bar_rows})")
else:
    tlog("ghost bar:       (pyte not installed — skipped)")

ok = exit_code == 0 and (ghost is False or ghost is None)
sys.exit(0 if ok else 1)
