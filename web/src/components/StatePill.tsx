import type { Process } from "../types";

/**
 * Group every state into one of four visual buckets.
 *
 * The point of the grid is answering "is anything wrong?" at a glance, so the
 * colour carries meaning and the label carries the detail. `dependency_failed`
 * reads as a warning rather than an error: the process didn't break, something it
 * needed did, and the actual culprit is highlighted separately.
 */
function tone(process: Process): "good" | "busy" | "bad" | "idle" {
  if (process.kind === "service") {
    switch (process.state) {
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
  switch (process.state) {
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
function label(process: Process): string {
  if (process.kind === "service" && process.state === "dependencyfailed") {
    return "dep failed";
  }
  return process.state.replace(/_/g, " ");
}

export function StatePill({ process }: { process: Process }) {
  return (
    <span className={`pill pill-${tone(process)}`}>
      {label(process)}
      {/* Tasks parked awaiting a manual trigger, matching the TUI's `*`. */}
      {process.kind === "task" && process.state === "pending_run" && (
        <span className="pill-flag" title="waiting for a manual run">
          *
        </span>
      )}
    </span>
  );
}
