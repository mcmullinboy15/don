import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../api";
import { renderAnsi } from "../ansi";
import { useEventSource } from "../hooks";

/** Cap on retained lines, so a chatty service can't grow the tab without bound. */
const MAX_LINES = 2000;

interface Props {
  projectId: string;
  name: string;
}

/**
 * Live log tail for one service or task.
 *
 * Follows the tail by default and stops following the moment the user scrolls
 * up — nothing is more annoying than reading a stack trace that jumps away.
 * Scrolling back to the bottom resumes.
 */
export function LogPane({ projectId, name }: Props) {
  const [lines, setLines] = useState<string[]>([]);
  const [following, setFollowing] = useState(true);
  const viewport = useRef<HTMLDivElement>(null);

  // Reset when switching between items, or the new stream appends to the old.
  useEffect(() => {
    setLines([]);
    setFollowing(true);
  }, [projectId, name]);

  const onLine = useCallback((payload: { line?: string }) => {
    if (typeof payload.line !== "string") return;
    setLines((current) => {
      const next = current.concat(payload.line as string);
      return next.length > MAX_LINES ? next.slice(-MAX_LINES) : next;
    });
  }, []);

  useEventSource<{ line?: string }>(api.logStreamUrl(projectId, name), onLine);

  useEffect(() => {
    if (!following) return;
    const element = viewport.current;
    if (element) element.scrollTop = element.scrollHeight;
  }, [lines, following]);

  function onScroll() {
    const element = viewport.current;
    if (!element) return;
    const distanceFromBottom =
      element.scrollHeight - element.scrollTop - element.clientHeight;
    setFollowing(distanceFromBottom < 40);
  }

  return (
    <section className="logs">
      <header className="logs-header">
        <h3>
          logs <code>{name}</code>
        </h3>
        <div className="logs-actions">
          {!following && (
            <button
              className="ghost"
              onClick={() => {
                setFollowing(true);
                const element = viewport.current;
                if (element) element.scrollTop = element.scrollHeight;
              }}
            >
              jump to latest
            </button>
          )}
          <button className="ghost" onClick={() => setLines([])}>
            clear
          </button>
        </div>
      </header>
      <div className="logs-body" ref={viewport} onScroll={onScroll}>
        {lines.length === 0 ? (
          <p className="muted">waiting for output…</p>
        ) : (
          lines.map((line, index) => (
            <div className="log-line" key={index}>
              {/* Strip the trailing CR that PTY-attached processes emit. */}
              {renderAnsi(line.replace(/\r?\n?$/, ""), `${index}`)}
            </div>
          ))
        )}
      </div>
    </section>
  );
}
