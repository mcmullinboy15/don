#!/usr/bin/env python3
"""Check that the log pane always reads in order, whatever is done to it.

Why this exists
---------------
The pane is a view over a store: the filter selects lines, the width decides
how many rows each one wraps to, a running index counts those rows, and a scroll
anchor picks the top one. Five things move underneath that — lines arriving,
lines evicting, the reader marking blank rows with Enter, the filter changing,
the terminal resizing — and the failure they produce is always the same shape.
The index counts rows one way, the pane paints them another, and the view lands
somewhere other than where it says: lines duplicated, lines skipped, the pane
apparently empty.

That shape has a cheap invariant. If a service numbers its output, the numbers
on screen must ascend from the top of the pane to the bottom, with no repeats.
A view that drew a line twice, or drew a stale row, or positioned itself with
one set of row counts and painted with another, breaks it. This drives the real
binary through all five and checks that invariant after every step.

Unit tests cover the same arithmetic against a store built in memory
(`what_is_drawn_matches_what_the_index_counted`); this is the half they cannot
reach — the render loop, the input task, and a terminal.

The project needs two services with numbered output, one of them wrapping:

    cargo build --release
    python3 tools/tui_drive_logs.py target/release/don /tmp/don-logview

Writes its own don.toml into the project directory if there isn't one.
"""

import re
import sys
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from tui_emulator import HAVE_PYTE, Session  # noqa: E402

# One service counts, the other counts *and* wraps — a wrapped line occupies
# several rows, which is where the index and the pane can disagree.
CONFIG = """\
[services.api]
run.cmd = "sh"
run.args = ["-c", "i=0; while true; do i=$((i+1)); echo \\"api line $i\\"; sleep 0.35; done"]

[services.web]
run.cmd = "sh"
run.args = ["-c", "i=0; while true; do i=$((i+1)); echo \\"web says something rather longer than the pane is wide so that it has to wrap across several rows, number $i\\"; sleep 0.5; done"]
"""

COUNTED = re.compile(r"api line (\d+)")


def tlog(start, message):
    print("[%6dms] %s" % ((time.time() - start) * 1000, message), file=sys.stderr)


def ordering(session):
    """The counted lines on screen, top to bottom."""
    found = []
    for line in session.screen.display():
        match = COUNTED.search(line)
        if match:
            found.append(int(match.group(1)))
    return found


def main():
    if len(sys.argv) < 3:
        print("usage: tui_drive_logs.py <don-binary> <project-dir>", file=sys.stderr)
        return 2
    binary, project = sys.argv[1], sys.argv[2]

    import os

    os.makedirs(project, exist_ok=True)
    config = os.path.join(project, "don.toml")
    if not os.path.exists(config):
        with open(config, "w") as handle:
            handle.write(CONFIG)

    start = time.time()
    session = Session(binary, project, cols=100, rows=24)
    tlog(start, "spawned don pid=%s (pyte: %s)" % (session.pid, HAVE_PYTE))

    failures = []

    def check(tag):
        # Always after settling: a frame read halfway through shows part of the
        # new screen over part of the old, which fails this for a reason that
        # has nothing to do with don.
        session.settle()
        seen = ordering(session)
        ordered = seen == sorted(seen) and len(seen) == len(set(seen))
        if not ordered:
            failures.append((tag, seen))
            tlog(start, "FAIL %s: %s" % (tag, seen))
        return ordered

    if not session.wait_for_screen(re.compile(r"api line \d+"), 20):
        tlog(start, "FAIL: no output on screen")
        session.kill()
        return 1
    session.pump(3.0)
    check("settled")

    # Enter marks a blank row after the last line on screen. Repeatedly, which
    # is what a reader does to put a gap before whatever comes next.
    for press in range(6):
        session.send(b"\r")
        session.pump(0.3)
        check("enter x%d" % (press + 1))
    session.pump(1.5)
    check("output after the gaps")

    # Ctrl+V swaps to don's own diagnostics and back. Two records, two stores,
    # two indexes, two sets of blank marks — swapped, not rebuilt, which is
    # where they can end up describing each other's screen.
    for round in range(3):
        session.send(b"\x16")
        session.pump(0.4)
        check("diagnostics %d" % round)
        session.send(b"\x16")
        session.pump(0.4)
        check("back to output %d" % round)
        session.send(b"\r")
        session.pump(0.3)
        check("enter after the swap %d" % round)

    # Out of follow mode and back, with output arriving throughout.
    session.send(b"\x1b[5~")
    session.pump(0.5)
    check("scrolled up")
    session.pump(2.0)
    check("output while held")
    session.send(b"\r")
    session.pump(0.5)
    check("enter resumes following")

    for cols, rows in ((100, 40), (70, 20), (120, 30), (100, 24)):
        session.set_size(cols, rows)
        session.pump(0.6)
        check("resized to %dx%d" % (cols, rows))

    # A burst big enough to push every line of content off the pane, then let
    # output fill it again.
    for _ in range(15):
        session.send(b"\r")
    session.pump(1.0)
    check("a screenful of blank rows")
    session.pump(3.0)
    check("output after the burst")

    session.interrupt()
    code = session.wait_exit(20)
    if code is None:
        tlog(start, "HANG: don did not exit")
        session.kill()
        failures.append(("exit", None))
    elif code != 0:
        tlog(start, "don exit code: %s" % code)
        failures.append(("exit", code))

    if failures:
        tlog(start, "%d check(s) failed; last screen follows" % len(failures))
        print(session.text(), file=sys.stderr)
    tlog(start, "RESULT: %s" % ("FAIL" if failures else "ok"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
