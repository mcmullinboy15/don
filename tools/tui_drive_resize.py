#!/usr/bin/env python3
"""Check that resizing does not move the log out from under the reader.

Why this exists
---------------
The log pane wraps to its own width, so how many *rows* a line occupies
changes with the terminal. That makes a row-counted scroll offset mean
something different after every resize — the view jumps, sometimes by
screenfuls, and the line someone was reading is gone.

`src/tui/logs.rs` anchors instead to a log id plus an offset within that line,
which is stable across resizes by construction. This drives the real binary to
check the construction actually holds end to end: scroll up, resize, and
confirm the line at the top of the pane is still the same line.

It also watches the byte cost of a resize. The old inline-viewport TUI replayed
its entire retained history on resize, which showed up as multi-megabyte spikes
and seconds of screen churn; a full repaint should cost one screenful.

Requires a project whose services produce plenty of distinguishable output —
the stress config generator is ideal:

    python3 tools/gen_stress_config.py /tmp/don-stress
    cargo build --release
    rm -rf /tmp/don-stress/.don
    python3 tools/tui_drive_resize.py target/release/don /tmp/don-stress

Override the size sequence with DON_RESIZE_SIZES, e.g. "50x140,50x100" for a
width-only resize.
"""

import os
import re
import sys
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from tui_emulator import HAVE_PYTE, Session  # noqa: E402

# See the note in tui_drive.py: matched as a pattern so that a reworded status
# bar fails loudly instead of quietly never matching.
READY = re.compile(r"\b([1-9]\d*)/\1 services ready\b")
DEFAULT_SIZES = "60x200,45x120,45x90,60x200"


def tlog(start, message):
    print("[%6.0fms] %s" % ((time.time() - start) * 1000, message), file=sys.stderr)


def anchor_line(session):
    """The topmost content row of the log pane — what the reader is looking at.

    Compared as text rather than by id because the id is don's internal state;
    what matters to the user is that the same *line* is still there.

    The log pane has a border now, and anchoring on a border row would make
    this check pass trivially — every width's border starts with the same
    corner, rule and title. Border rows are recognisable by their first
    character: content rows start with the pane's left border `│` followed by
    text, while pure border rows start with a corner or tee, so we strip one
    leading `│` and skip anything that is still box-drawing or blank.
    """
    box = set("┌┐└┘├┤┬┴┼─│┄┊")
    for line in session.screen.display():
        stripped = line.strip()
        if stripped.startswith("│"):
            stripped = stripped[1:].strip()
        if stripped and stripped[0] not in box:
            return stripped
    return ""


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    binary, project = sys.argv[1], sys.argv[2]
    sizes = os.environ.get("DON_RESIZE_SIZES", DEFAULT_SIZES)
    sequence = []
    for spec in sizes.split(","):
        rows, cols = spec.strip().lower().split("x")
        sequence.append((int(cols), int(rows)))

    start = time.time()
    session = Session(binary, project, cols=sequence[0][0], rows=sequence[0][1])
    tlog(start, "spawned don pid=%d (pyte: %s)" % (session.pid, HAVE_PYTE))

    if not session.wait_for_screen(READY, 90.0):
        tlog(start, "FAIL: startup never settled")
        session.kill()
        return 1

    # Let a decent backlog build so there is something to scroll through.
    session.pump(4.0)

    # Scroll up out of follow mode. PageUp twice puts the reader well clear of
    # the live tail, so any drift is visible rather than masked by new output.
    session.send(b"\x1b[5~\x1b[5~")
    session.pump(0.6)
    before = anchor_line(session)
    tlog(start, "scrolled up; top line: %r" % before[:70])
    if not before:
        tlog(start, "FAIL: nothing on screen after scrolling")
        session.kill()
        return 1

    failures = []
    for cols, rows in sequence[1:]:
        mark = len(session.raw)
        session.set_size(cols, rows)
        session.pump(1.0)
        cost = len(session.raw) - mark
        after = anchor_line(session)
        screenful = cols * rows * 4  # a generous per-cell allowance with styling
        tlog(
            start,
            "resize -> %dx%d: %d bytes (budget %d), top line: %r"
            % (rows, cols, cost, screenful, after[:70]),
        )
        # The anchor is a whole logical line; a narrower terminal wraps it, so
        # the top row can be a prefix of what it was. Requiring containment
        # either way catches a jump without failing on legitimate rewrapping.
        held = before.startswith(after[: min(len(after), 30)]) or after.startswith(
            before[: min(len(before), 30)]
        )
        if not held:
            failures.append("resize to %dx%d moved the view: %r -> %r" % (rows, cols, before, after))
        if cost > screenful:
            failures.append(
                "resize to %dx%d wrote %d bytes, over a %d budget — replaying history?"
                % (rows, cols, cost, screenful)
            )
        before = after

    session.interrupt()
    code = session.wait_exit(30.0)
    if code is None:
        failures.append("don did not exit after Ctrl+C")
        session.kill()

    for failure in failures:
        tlog(start, "FAIL: %s" % failure)
    tlog(start, "RESULT: %s" % ("ok" if not failures else "FAIL"))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
