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
for text assertions. Two things it has to get right that are easy to miss: a
read is a chunk of a byte stream, so an escape sequence or a multi-byte
character can straddle two of them, and a fragment printed instead of held
shows up as corruption in the thing under test rather than in here.

The other half of "what does the user see" is *when* you look. A frame is one
burst of writes; sampling in the middle of one shows half the new screen over
half the old, which reads as duplicated and truncated rows. `Session.settle`
waits for the gap between frames, and any assertion on the screen should go
after it.
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
        # An escape sequence the last read stopped in the middle of. A read is
        # a chunk of a byte stream, not a message: a cursor move can and does
        # straddle two of them. Printing the fragment instead of holding it is
        # how "[7;1H" ends up as text in the middle of a log line, which reads
        # as a rendering bug in the thing under test.
        self.pending = b""

    def resize(self, cols, rows):
        self.cols, self.rows = cols, rows
        self.grid = [[" "] * cols for _ in range(rows)]
        self.cy = self.cx = 0
        self.pending = b""

    def feed(self, data):
        data = self.pending + data
        self.pending = b""
        i = 0
        while i < len(data):
            b = data[i : i + 1]
            if b == b"\x1b":
                nxt = self._escape(data, i)
                if nxt is None:
                    # Incomplete: keep it for the next read.
                    self.pending = data[i:]
                    return
                i = nxt
                continue
            if b == b"\n":
                self.cy = min(self.cy + 1, self.rows - 1)
                i += 1
                continue
            if b == b"\r":
                self.cx = 0
                i += 1
                continue
            # Width from the lead byte, not from how many continuation bytes
            # happen to have arrived: the box-drawing characters in don's
            # prefixes are three bytes each, and one split across a read
            # boundary used to decode as garbage and then leave the cursor a
            # column out for the rest of the line.
            lead = data[i]
            n = 4 if lead >= 0xF0 else 3 if lead >= 0xE0 else 2 if lead >= 0xC0 else 1
            if i + n > len(data):
                self.pending = data[i:]
                return
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
        """Consume one escape sequence. `None` means "need more bytes"."""
        rest = data[i:]
        # OSC — don uses it for the clipboard. Ends at BEL or ST, and carries
        # arbitrary base64 in between, so it must never be printed.
        if rest[:2] == b"\x1b]":
            end = rest.find(b"\x07")
            st = rest.find(b"\x1b\\")
            if end == -1 or (st != -1 and st < end):
                return None if st == -1 else i + st + 2
            return i + end + 1
        m = re.match(rb"\x1b\[([0-9;?]*)([A-Za-z])", rest[:64])
        if not m:
            # A CSI still being introduced, or a lone ESC at the very end.
            if re.fullmatch(rb"\x1b(\[[0-9;?]*)?", rest[:64]):
                return None
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

    def settle(self, quiet=0.05, timeout=2.0):
        """Pump until the TUI stops writing, so the screen is a whole frame.

        A frame is one burst of cursor moves and cells. Reading the screen
        while one is in flight shows half of it over half of the last, which
        looks exactly like the rendering bugs these drivers exist to catch —
        rows duplicated, rows cut short. Waiting for a gap in the output is
        the difference between sampling a frame and sampling a seam.

        The gap has to be shorter than the interval between frames: the TUI
        redraws about ten times a second even when nothing arrives, to move
        the spinner and the relative timestamps, so it is never quiet for
        long. Returns False if it never paused at all.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            r, _, _ = select.select([self.fd], [], [], quiet)
            if self.fd not in r:
                return True
            try:
                chunk = os.read(self.fd, 65536)
            except OSError:
                return True
            if not chunk:
                return True
            self.raw.extend(chunk)
            self.screen.feed(chunk)
        return False

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
