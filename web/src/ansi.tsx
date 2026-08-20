/**
 * Render log lines that carry ANSI colour.
 *
 * don's output sanitizer (`src/output/sanitize.rs`) already strips cursor and
 * screen-control sequences and keeps SGR, so by the time a line reaches here
 * the only escapes left are colour and style. That's a small enough subset to
 * handle directly rather than pulling in a terminal emulator.
 */

import type { JSX } from "react";

/** Standard 16-colour palette, tuned for a dark background. */
const COLORS = [
  "#3f3f46", // black (lifted, so it stays legible)
  "#f87171", // red
  "#4ade80", // green
  "#fbbf24", // yellow
  "#60a5fa", // blue
  "#c084fc", // magenta
  "#22d3ee", // cyan
  "#d4d4d8", // white
  "#71717a", // bright black
  "#fca5a5", // bright red
  "#86efac", // bright green
  "#fde047", // bright yellow
  "#93c5fd", // bright blue
  "#d8b4fe", // bright magenta
  "#67e8f9", // bright cyan
  "#fafafa", // bright white
];

interface Style {
  color?: string;
  background?: string;
  bold?: boolean;
  dim?: boolean;
  italic?: boolean;
  underline?: boolean;
}

/** Apply one SGR parameter run to the running style. */
function applySgr(style: Style, params: number[]): Style {
  const next = { ...style };
  for (let i = 0; i < params.length; i++) {
    const code = params[i];
    if (code === undefined) continue;
    if (code === 0) {
      for (const key of Object.keys(next) as (keyof Style)[]) delete next[key];
    } else if (code === 1) next.bold = true;
    else if (code === 2) next.dim = true;
    else if (code === 3) next.italic = true;
    else if (code === 4) next.underline = true;
    else if (code === 22) {
      delete next.bold;
      delete next.dim;
    } else if (code === 23) delete next.italic;
    else if (code === 24) delete next.underline;
    else if (code >= 30 && code <= 37) next.color = COLORS[code - 30];
    else if (code >= 90 && code <= 97) next.color = COLORS[code - 90 + 8];
    else if (code === 39) delete next.color;
    else if (code >= 40 && code <= 47) next.background = COLORS[code - 40];
    else if (code >= 100 && code <= 107)
      next.background = COLORS[code - 100 + 8];
    else if (code === 49) delete next.background;
    else if (code === 38 || code === 48) {
      // Extended colour: `38;5;N` (256) or `38;2;R;G;B` (truecolor).
      const mode = params[i + 1];
      const target = code === 38 ? "color" : "background";
      if (mode === 5) {
        const value = params[i + 2];
        if (value !== undefined) next[target] = xterm256(value);
        i += 2;
      } else if (mode === 2) {
        const [r, g, b] = [params[i + 2], params[i + 3], params[i + 4]];
        if (r !== undefined && g !== undefined && b !== undefined) {
          next[target] = `rgb(${r},${g},${b})`;
        }
        i += 4;
      }
    }
  }
  return next;
}

/** Map an xterm-256 index to a CSS colour. */
function xterm256(index: number): string {
  if (index < 16) return COLORS[index] ?? "inherit";
  if (index < 232) {
    const n = index - 16;
    const level = (v: number) => (v === 0 ? 0 : 55 + v * 40);
    return `rgb(${level(Math.floor(n / 36))},${level(Math.floor(n / 6) % 6)},${level(n % 6)})`;
  }
  const gray = 8 + (index - 232) * 10;
  return `rgb(${gray},${gray},${gray})`;
}

function toCss(style: Style): React.CSSProperties | undefined {
  if (Object.keys(style).length === 0) return undefined;
  return {
    color: style.color,
    background: style.background,
    fontWeight: style.bold ? 600 : undefined,
    opacity: style.dim ? 0.65 : undefined,
    fontStyle: style.italic ? "italic" : undefined,
    textDecoration: style.underline ? "underline" : undefined,
  };
}

// eslint-disable-next-line no-control-regex
const SGR = /\x1b\[([0-9;]*)m/g;

/**
 * Split a line into styled spans.
 *
 * Exported for testing and reuse; unmatched escapes are dropped rather than
 * printed, so a stray sequence never shows up as mojibake in the log pane.
 */
export function renderAnsi(line: string, keyPrefix: string): JSX.Element[] {
  const spans: JSX.Element[] = [];
  let style: Style = {};
  let cursor = 0;
  let key = 0;

  SGR.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = SGR.exec(line)) !== null) {
    if (match.index > cursor) {
      const text = line.slice(cursor, match.index);
      spans.push(
        <span key={`${keyPrefix}-${key++}`} style={toCss(style)}>
          {text}
        </span>,
      );
    }
    const params = (match[1] ?? "")
      .split(";")
      .filter((p) => p !== "")
      .map(Number);
    style = applySgr(style, params.length === 0 ? [0] : params);
    cursor = match.index + match[0].length;
  }

  if (cursor < line.length) {
    spans.push(
      <span key={`${keyPrefix}-${key++}`} style={toCss(style)}>
        {line.slice(cursor)}
      </span>,
    );
  }

  return spans;
}
