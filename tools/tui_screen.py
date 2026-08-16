#!/usr/bin/env python3
"""Drive don's TUI under a real PTY and print what is on the screen.

The TUI owns the alternate screen now, so its output is a stream of cursor
moves and styled cells rather than lines of text — grepping the raw bytes for a
message tells you almost nothing, because a styled line arrives split across
escape sequences and a repainted line arrives twice.

This renders the stream into a screen buffer and prints the result, which is
what the user actually sees. Without `pyte` installed it falls back to a naive
cell grid that handles the subset of sequences ratatui emits (CUP, SGR, ED,
EL), which is enough for assertions about text.

    tui_screen.py <don-binary> <project-dir> [linger-seconds] [keys]

`keys` is an optional string sent after the linger, one byte at a time, before
a final screen dump — so a single run can check "start up, press s, look at the
services table".
"""

import os
import pty
import re
import select
import signal
import sys
import time

COLS, ROWS = 200, 60


class Screen:
    """Minimal terminal emulator: enough of the sequences ratatui emits."""

    def __init__(self, cols, rows):
        self.cols, self.rows = cols, rows
        self.grid = [[" "] * cols for _ in range(rows)]
        self.cy = self.cx = 0
        self.alt = False

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
            # Decode one UTF-8 character.
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
        return

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
            mode = nums[0] if nums else 0
            if mode == 2:
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

    def text(self):
        return "\n".join("".join(row).rstrip() for row in self.grid)


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    binary, project = sys.argv[1], sys.argv[2]
    linger = float(sys.argv[3]) if len(sys.argv) > 3 else 6.0
    keys = sys.argv[4].encode() if len(sys.argv) > 4 else b""

    pid, fd = pty.fork()
    if pid == 0:
        os.chdir(project)
        os.environ["TERM"] = "xterm-256color"
        os.environ["COLUMNS"], os.environ["LINES"] = str(COLS), str(ROWS)
        os.execv(binary, [binary, "start"])
        os._exit(127)

    import fcntl
    import struct
    import termios

    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))

    screen = Screen(COLS, ROWS)
    raw = bytearray()

    def pump(seconds):
        end = time.time() + seconds
        while time.time() < end:
            r, _, _ = select.select([fd], [], [], 0.1)
            if fd in r:
                try:
                    chunk = os.read(fd, 65536)
                except OSError:
                    return False
                if not chunk:
                    return False
                raw.extend(chunk)
                screen.feed(chunk)
        return True

    pump(linger)
    print("=== screen after %.1fs ===" % linger)
    print(screen.text())

    if keys:
        for k in keys:
            os.write(fd, bytes([k]))
            pump(0.4)
        print("=== screen after keys %r ===" % keys.decode())
        print(screen.text())

    os.write(fd, b"\x03")  # Ctrl+C
    pump(6.0)
    print("=== screen after Ctrl+C ===")
    print(screen.text())

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    os.waitpid(pid, 0)

    print("=== captured %d bytes; alt screen active at end: %s ===" % (len(raw), screen.alt),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
