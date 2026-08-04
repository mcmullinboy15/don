import type { Item } from "../types";

/**
 * Group every state into one of four visual buckets.
 *
 * The point of the grid is answering "is anything wrong?" at a glance, so the
 * colour carries meaning and the label carries the detail. `dependency_failed`
 * reads as a warning rather than an error: the item didn't break, something it
 * needed did, and the actual culprit is highlighted separately.
 */
function tone(item: Item): "good" | "busy" | "bad" | "idle" {
  if (item.kind === "service") {
    switch (item.state) {
      case "ready":
      case "lazy":
        return "good";
      case "pending":
      case "building":
      case "starting":
      case "running":
      case "stopping":
        return "busy";
      case "failed":
      case "dependencyfailed":
      case "unhealthy":
        return "bad";
      default:
        return "idle";
    }
  }
  switch (item.state) {
    case "completed":
      return "good";
    case "pending":
    case "building":
    case "running":
      return "busy";
    case "failed":
    case "dependency_failed":
      return "bad";
    default:
      return "idle";
  }
}

/** Human-readable state text. */
function label(item: Item): string {
  if (item.kind === "service" && item.state === "dependencyfailed") {
    return "dep failed";
  }
  return item.state.replace(/_/g, " ");
}

export function StatePill({ item }: { item: Item }) {
  return (
    <span className={`pill pill-${tone(item)}`}>
      {label(item)}
      {/* Tasks parked awaiting a manual trigger, matching the TUI's `*`. */}
      {item.kind === "task" && item.state === "pending_run" && (
        <span className="pill-flag" title="waiting for a manual run">
          *
        </span>
      )}
    </span>
  );
}
