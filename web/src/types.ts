/** Wire types, mirroring the Rust structs they're serialized from. */

/** A project registered with the daemon (`daemon::registry::ProjectEntry`). */
export interface Project {
  id: string;
  name: string;
  root: string;
  socket: string;
  pid: number;
  profile?: string | null;
  registered_at_unix_secs: number;
}

/** Service states, from `runner::ServiceState`. */
export type ServiceState =
  | "pending"
  | "building"
  | "lazy"
  | "starting"
  | "running"
  | "ready"
  | "unhealthy"
  | "stopping"
  | "stopped"
  | "failed"
  | "dependencyfailed";

/** Task states, from `runner::TaskItemState`. */
export type TaskState =
  | "pending"
  | "building"
  | "running"
  | "completed"
  | "skipped"
  | "failed"
  | "dependency_failed"
  | "pending_run";

/** One task param the run form should collect (`runner::ParamInfo`). */
export interface ParamInfo {
  name: string;
  prompt?: string;
  required: boolean;
  default?: string;
  kind: "string" | "int" | "bool" | "choice";
  choices?: string[];
  has_completions?: boolean;
  min?: number;
  max?: number;
}

/** Extra detail returned with `?verbose=true` (`runner::VerboseInfo`). */
export interface VerboseInfo {
  depends_on?: unknown[];
  params?: ParamInfo[];
  watch?: string[];
  watch_count?: number;
  proxy?: string[];
  docker_ports?: string[];
  proxy_active_connections?: number;
  bazel_target?: string;
  turbo_task?: string;
  ready?: string;
  cmd?: string;
  watch_state?: string;
  watch_notes?: string[];
}

export interface TaskRunInfo {
  finished_at_unix_secs: number;
  duration_ms?: number;
  success: boolean;
  exit_code?: number;
  message?: string;
}

/** One row of `GET /status` (`runner::ItemStatus`). */
export type Item =
  | {
      kind: "service";
      name: string;
      state: ServiceState;
      failed_dependencies?: string[];
      verbose?: VerboseInfo;
    }
  | {
      kind: "task";
      name: string;
      state: TaskState;
      failed_dependencies?: string[];
      last_run?: TaskRunInfo;
      verbose?: VerboseInfo;
    };

/** A runner event delivered over SSE (`runner::RunnerEvent`). */
export type RunnerEvent =
  | {
      type: "service_state_changed";
      name: string;
      state: ServiceState;
      pid: number | null;
      failed_dependencies: string[];
    }
  | {
      type: "task_state_changed";
      name: string;
      state: TaskState;
      last_run: TaskRunInfo | null;
      failed_dependencies: string[];
    }
  | { type: "rebuild_complete"; name: string; success: boolean }
  | { type: "task_rerun_complete"; name: string; success: boolean }
  | { type: "shutdown_started" }
  | { type: "shutdown_complete" }
  | {
      type: "update_check_complete";
      current_version: string;
      latest_version: string | null;
    }
  /** The subscriber fell behind; refetch status to resync. */
  | { type: "lagged"; skipped: number };

/** `.don/ports.json`, via `GET /api/projects/:id/ports`. */
export interface PortManifest {
  version: number;
  generated_at_unix_secs: number;
  services?: Record<
    string,
    {
      proxy?: { configured_addr: string; bound_addr: string }[];
      docker?: { configured: string; bound_addr: string }[];
    }
  >;
}
