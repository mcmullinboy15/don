"""A terminal emulator and PTY harness, shared by the TUI drivers.

Why this exists
---------------
don's TUI owns the alternate screen. Its output is a stream of cursor moves
and styled cells, not lines of text — a message arrives split across escape
sequences, a repainted region arrives twice, and a message that scrolled off
is still in the byte stream forever. Grepping the raw bytes therefore answers
a different question from "what does the user see", and the two diverge in
both directions: false negatives from a split line, false positives from a
line that is no longer on screen.

So the drivers render the stream and assert on the screen.

`pyte` does this properly and is used when installed. The fallback covers the
subset ratatui emits — CUP, ED, EL, SGR, alternate screen — which is enough
for text assertions.
"""

import fcntl
import os
import pty
import re
import select
import signal
import struct
import termios
import time

try:  # pragma: no cover - exercised by whichever is installed
    import pyte

    HAVE_PYTE = True
except ImportError:
    HAVE_PYTE = False


class FallbackScreen:
    """Enough of a terminal to answer "what is on screen"."""

    def __init__(self, cols, rows):
        self.cols, self.rows = cols, rows
        self.grid = [[" "] * cols for _ in range(rows)]
        self.cy = self.cx = 0
        self.alt = False

    def resize(self, cols, rows):
        self.cols, self.rows = cols, rows
        self.grid = [[" "] * cols for _ in range(rows)]
        self.cy = self.cx = 0

    def feed(self, data):
        i = 0
        while i < len(data):
            b = data[i : i + 1]
            if b == b"\x1b":
                i = self._escape(data, i)
                continue
            if b == b"\n":
                self.cy = min(self.cy + 1, self.rows - 1)
                i += 1
                continue
            if b == b"\r":
                self.cx = 0
                i += 1
                continue
            n = 1
            while i + n < len(data) and (data[i + n] & 0xC0) == 0x80:
                n += 1
            try:
                ch = data[i : i + n].decode("utf-8")
            except UnicodeDecodeError:
                ch = "?"
            if 0 <= self.cy < self.rows and 0 <= self.cx < self.cols:
                self.grid[self.cy][self.cx] = ch
            self.cx += 1
            if self.cx >= self.cols:
                self.cx = 0
                self.cy = min(self.cy + 1, self.rows - 1)
            i += n

    def _escape(self, data, i):
        m = re.match(rb"\x1b\[([0-9;?]*)([A-Za-z])", data[i : i + 32])
        if not m:
            return i + 1
        params, final = m.group(1), m.group(2)
        nums = [int(p) for p in params.split(b";") if p.isdigit()]
        if final == b"H":
            self.cy = (nums[0] - 1) if nums else 0
            self.cx = (nums[1] - 1) if len(nums) > 1 else 0
        elif final == b"J":
            if (nums[0] if nums else 0) == 2:
                self.grid = [[" "] * self.cols for _ in range(self.rows)]
        elif final == b"K":
            if 0 <= self.cy < self.rows:
                for x in range(self.cx, self.cols):
                    self.grid[self.cy][x] = " "
        elif final == b"h" and b"1049" in params:
            self.alt = True
            self.grid = [[" "] * self.cols for _ in range(self.rows)]
        elif final == b"l" and b"1049" in params:
            self.alt = False
        return i + m.end()

    def display(self):
        return ["".join(row).rstrip() for row in self.grid]


class PyteScreen:
    """pyte-backed screen, with the same surface as the fallback."""

    def __init__(self, cols, rows):
        self.screen = pyte.Screen(cols, rows)
        self.stream = pyte.ByteStream(self.screen)
        self.alt = False

    def resize(self, cols, rows):
        self.screen.resize(rows, cols)

    def feed(self, data):
        if b"\x1b[?1049h" in data:
            self.alt = True
        if b"\x1b[?1049l" in data:
            self.alt = False
        self.stream.feed(data)

    def display(self):
        return [line.rstrip() for line in self.screen.display]


def new_screen(cols, rows):
    return PyteScreen(cols, rows) if HAVE_PYTE else FallbackScreen(cols, rows)


class Session:
    """`don start` running under a PTY, with its screen kept up to date."""

    def __init__(self, binary, project, cols=200, rows=60, args=("start",)):
        self.cols, self.rows = cols, rows
        self.raw = bytearray()
        self.screen = new_screen(cols, rows)
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.chdir(project)
            os.environ["TERM"] = "xterm-256color"
            os.environ["COLUMNS"], os.environ["LINES"] = str(cols), str(rows)
            os.execv(binary, [binary, *args])
            os._exit(127)
        self.set_size(cols, rows)
        self.exit_code = None

    def set_size(self, cols, rows):
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
        self.cols, self.rows = cols, rows
        self.screen.resize(cols, rows)

    def pump(self, seconds):
        """Read for `seconds`. Returns False once the child's side is closed."""
        end = time.time() + seconds
        alive = True
        while time.time() < end:
            r, _, _ = select.select([self.fd], [], [], 0.05)
            if self.fd in r:
                try:
                    chunk = os.read(self.fd, 65536)
                except OSError:
                    alive = False
                    break
                if not chunk:
                    alive = False
                    break
                self.raw.extend(chunk)
                self.screen.feed(chunk)
        return alive

    def wait_for_screen(self, needle, timeout):
        """Pump until `needle` appears *on screen*, or time runs out.

        `needle` is a substring, or a compiled regex to search each line with.
        A regex is the better choice for anything the TUI renders from live
        numbers — matching the literal wording of a status line means the
        driver silently stops checking anything the day that wording changes.
        """
        if hasattr(needle, "search"):
            hit = lambda line: needle.search(line) is not None  # noqa: E731
        else:
            hit = lambda line: needle in line  # noqa: E731
        end = time.time() + timeout
        while time.time() < end:
            if any(hit(line) for line in self.screen.display()):
                return True
            if not self.pump(0.1):
                return any(hit(line) for line in self.screen.display())
        return False

    def send(self, data):
        os.write(self.fd, data)

    def interrupt(self):
        self.send(b"\x03")

    def wait_exit(self, timeout):
        end = time.time() + timeout
        while time.time() < end:
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.exit_code = os.waitstatus_to_exitcode(status)
                return self.exit_code
            self.pump(0.1)
        return None

    def kill(self):
        try:
            os.kill(self.pid, signal.SIGKILL)
            os.waitpid(self.pid, 0)
        except (ProcessLookupError, ChildProcessError):
            pass

    def text(self):
        return "\n".join(self.screen.display())
