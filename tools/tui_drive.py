#!/usr/bin/env python3
"""Drive `don start` in TUI mode under a real PTY.

Why this exists
---------------
Don's TUI uses ratatui + crossterm and relies on a working terminal —
in particular, an initial DSR (`\\x1b[6n`) query whose response anchors
the inline viewport. Plain pipes / `script(1)` / `expect(1)` don't
answer DSR, so the TUI bails on startup with "The cursor position could
not be read within a normal duration". This driver:

  - allocates a real PTY via `pty.fork()`
  - intercepts DSR queries on the master side and answers `\\x1b[1;1R`
  - waits for "all services running" on stdout
  - sends SIGINT after a configurable linger
  - watches for "shutdown complete" then a clean exit
  - declares HANG and force-kills if the process hasn't exited within
    8 seconds of "shutdown complete" (or 30 s after SIGINT)
  - prints a structured summary on stderr (lifecycle event counts,
    captured byte total, exit code) and the full ANSI stream on stdout

Usage
-----
    python3 tools/tui_drive.py <path-to-don-binary> <config-dir> [linger]

Example (against the synthetic stress config in this repo):

    python3 tools/gen_stress_config.py /tmp/don-stress
    cargo build --release
    python3 tools/tui_drive.py target/release/don /tmp/don-stress 4 \\
        > /tmp/tui-stdout.bin 2> /tmp/tui-stderr.log
    tail -15 /tmp/tui-stderr.log

The "lifecycle 'stopping' events" / "'send SIGTERM'" / "'stopped'"
counts on stderr should equal the number of running (non-lazy) services
when shutdown is healthy. A "HANG" line in the summary or a non-zero
exit is the signal that something regressed.
"""

import os
import pty
import re
import select
import signal
import sys
import time

if len(sys.argv) < 3:
    print(f"usage: {sys.argv[0]} <don binary> <cwd> [linger-seconds]", file=sys.stderr)
    sys.exit(2)

# Resolve the binary path before fork — `os.chdir(CWD)` happens in the
# child, so a relative `target/release/don` would otherwise resolve
# inside the test config dir and fail.
DON = os.path.realpath(sys.argv[1])
CWD = sys.argv[2]
LINGER = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0
if not os.access(DON, os.X_OK):
    print(f"binary not found or not executable: {DON}", file=sys.stderr)
    sys.exit(2)

ANSI = re.compile(rb"\x1b\[[0-9;]*[a-zA-Z]|\x1b\][^\x07]*\x07")
DSR = re.compile(rb"\x1b\[6n")
READY_NEEDLE = b"all services running"
SHUTDOWN_NEEDLE = b"shutdown complete"

start = time.monotonic()
def tlog(msg):
    sys.stderr.write(f"[{int((time.monotonic() - start) * 1000)}ms] {msg}\n")
    sys.stderr.flush()

# Set up the PTY ourselves so we can read AND write to the master.
pid, fd = pty.fork()
if pid == 0:
    # Child: exec don.
    os.chdir(CWD)
    os.environ["TERM"] = "xterm-256color"
    os.environ.setdefault("COLUMNS", "200")
    os.environ.setdefault("LINES", "60")
    os.execvp(DON, [DON, "start"])
    os._exit(127)

tlog(f"spawned don pid={pid}")

# Set master pty non-blocking.
import fcntl
flags = fcntl.fcntl(fd, fcntl.F_GETFL)
fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

# Try to set window size.
try:
    import termios, struct
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 60, 200, 0, 0))
except Exception as e:
    tlog(f"ioctl TIOCSWINSZ failed: {e}")

captured = bytearray()
saw_ready = False
saw_shutdown = False
shutdown_seen_at = None
sigint_sent = False
sigint_at = None
exit_code = None
deadline = time.monotonic() + 90  # absolute cap

while True:
    try:
        rlist, _, _ = select.select([fd], [], [], 0.05)
    except (OSError, select.error):
        break

    if rlist:
        try:
            data = os.read(fd, 8192)
        except OSError as e:
            # Errno 5 (EIO) on Linux means the slave side has closed —
            # this is the normal way a pty signals child exit on this
            # platform. Don't treat it as a failure; let the next
            # waitpid() pick up the actual exit status.
            if getattr(e, 'errno', None) == 5:
                tlog("EOF on master (slave closed)")
            else:
                tlog(f"read error: {e}")
            break
        if not data:
            tlog("EOF on master")
            break
        captured.extend(data)
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()

        # Answer DSR queries.
        for _ in DSR.finditer(data):
            try:
                os.write(fd, b"\x1b[1;1R")
            except OSError:
                pass

        plain = ANSI.sub(b"", bytes(captured))
        if not saw_ready and READY_NEEDLE in plain:
            saw_ready = True
            tlog(f"saw '{READY_NEEDLE.decode()}'")
        if not saw_shutdown and SHUTDOWN_NEEDLE in plain:
            saw_shutdown = True
            shutdown_seen_at = time.monotonic()
            tlog(f"saw '{SHUTDOWN_NEEDLE.decode()}'")

    # After ready + linger, send SIGINT.
    if saw_ready and not sigint_sent:
        if time.monotonic() - start >= LINGER:
            tlog(f"sending SIGINT to {pid}")
            os.kill(pid, signal.SIGINT)
            sigint_sent = True
            sigint_at = time.monotonic()

    # Hang detection: if we saw 'shutdown complete' but the process is still
    # alive 8s later, force-kill and report.
    if shutdown_seen_at is not None and time.monotonic() - shutdown_seen_at > 8:
        tlog("HANG: 'shutdown complete' fired but don is still alive 8s later")
        os.kill(pid, signal.SIGKILL)

    # Reap.
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

    if time.monotonic() > deadline:
        tlog("driver deadline; killing don")
        os.kill(pid, signal.SIGKILL)

    # If we sent SIGINT and the process is still running 30s later, kill.
    if sigint_at is not None and time.monotonic() - sigint_at > 30:
        tlog("30s after SIGINT — process still alive, sending SIGKILL")
        os.kill(pid, signal.SIGKILL)

# Drain any remaining output (process is dead but pty may have buffered bytes).
end_drain = time.monotonic() + 0.5
while time.monotonic() < end_drain:
    try:
        rlist, _, _ = select.select([fd], [], [], 0.05)
    except (OSError, select.error):
        break
    if not rlist:
        break
    try:
        data = os.read(fd, 8192)
    except OSError:
        break
    if not data:
        break
    captured.extend(data)
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

# If we broke out of the read loop on EIO without yet reaping the child,
# do a final waitpid (with a short bounded grace) so the summary reports
# a real exit code instead of None.
if exit_code is None:
    grace_deadline = time.monotonic() + 2.0
    while time.monotonic() < grace_deadline:
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

plain = ANSI.sub(b"", bytes(captured))
text = plain.decode("utf-8", errors="replace")

tlog("=== driver summary ===")
tlog(f"don exit code: {exit_code}")
tlog(f"saw ready:     {saw_ready}")
tlog(f"saw shutdown:  {saw_shutdown}")
tlog(f"lifecycle 'stopping' events: {text.count(': stopping')}")
tlog(f"lifecycle 'send SIGTERM'   : {text.count('send SIGTERM to pgid')}")
tlog(f"lifecycle 'stopped'        : {text.count(': stopped')}")
tlog(f"captured bytes: {len(captured)}")

# Persist plain output.
with open(os.path.join(CWD, ".don-tui-driver.plain.log"), "wb") as f:
    f.write(plain)
sys.exit(0 if exit_code == 0 else (1 if exit_code is not None else 2))
