/** Shared hooks: SSE subscriptions and live project status. */

import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "./api";
import type { Item, RunnerEvent } from "./types";

/**
 * Subscribe to an SSE endpoint, calling `onMessage` for each parsed payload.
 *
 * `EventSource` reconnects on its own after a network drop, which is the
 * behaviour we want when a project restarts — but a reconnect can miss
 * events, so callers that track state also need a way to resync.
 */
export function useEventSource<T>(
  url: string | null,
  onMessage: (payload: T) => void,
): void {
  // Keep the callback in a ref so a re-render doesn't tear down and rebuild
  // the connection — that would drop the stream on every state update.
  const handler = useRef(onMessage);
  handler.current = onMessage;

  useEffect(() => {
    if (!url) return;
    const source = new EventSource(url);
    source.onmessage = (event) => {
      try {
        handler.current(JSON.parse(event.data) as T);
      } catch {
        // A malformed frame is not worth tearing the stream down for.
      }
    };
    return () => source.close();
  }, [url]);
}

export interface StatusState {
  items: Item[];
  loading: boolean;
  error: string | null;
  refresh: () => void;
}

/**
 * Fetch a project's status and keep it current from the event stream.
 *
 * State transitions arrive over SSE rather than being polled for, so the grid
 * reacts as fast as the runner does. Events that imply more than a state
 * change — a rebuild finishing, or a dropped subscription — trigger a full
 * refetch instead of being patched in.
 */
export function useStatus(projectId: string | null): StatusState {
  const [items, setItems] = useState<Item[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(() => {
    if (!projectId) return;
    api
      .status(projectId)
      .then((next) => {
        setItems(next);
        setError(null);
      })
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [projectId]);

  useEffect(() => {
    setLoading(true);
    refresh();
  }, [refresh]);

  const onEvent = useCallback(
    (event: RunnerEvent) => {
      switch (event.type) {
        case "service_state_changed":
        case "task_state_changed":
          setItems((current) =>
            current.map((item) => {
              if (item.name !== event.name) return item;
              if (item.kind === "service" && event.type === "service_state_changed") {
                return {
                  ...item,
                  state: event.state,
                  failed_dependencies: event.failed_dependencies,
                };
              }
              if (item.kind === "task" && event.type === "task_state_changed") {
                return {
                  ...item,
                  state: event.state,
                  last_run: event.last_run ?? item.last_run,
                  failed_dependencies: event.failed_dependencies,
                };
              }
              return item;
            }),
          );
          break;
        // A rebuild or rerun can change more than one field, and a lagged
        // subscriber has missed transitions outright — refetch rather than
        // guess.
        case "rebuild_complete":
        case "task_rerun_complete":
        case "lagged":
          refresh();
          break;
        default:
          break;
      }
    },
    [refresh],
  );

  useEventSource<RunnerEvent>(
    projectId ? api.eventsUrl(projectId) : null,
    onEvent,
  );

  return { items, loading, error, refresh };
}

/** The current route, derived from the URL hash. */
export function useRoute(): { projectId: string | null; navigate: (id: string | null) => void } {
  const [hash, setHash] = useState(() => window.location.hash);

  useEffect(() => {
    const onChange = () => setHash(window.location.hash);
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);

  const navigate = useCallback((id: string | null) => {
    window.location.hash = id ? `#/projects/${id}` : "";
  }, []);

  const match = /^#\/projects\/([A-Za-z0-9]+)/.exec(hash);
  return { projectId: match?.[1] ?? null, navigate };
}
