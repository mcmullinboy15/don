/**
 * Thin wrapper over the don web API.
 *
 * There are no credentials to carry: the server binds loopback and serves
 * whatever reaches it, on the grounds that reaching it already means running
 * on the same machine.
 */

import type { Process, PortManifest, Project } from "./types";

/** An API call that came back non-2xx, carrying the server's message. */
export class ApiError extends Error {
  readonly status: number;
  readonly logPath?: string;

  constructor(status: number, message: string, logPath?: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.logPath = logPath;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    let logPath: string | undefined;
    try {
      const body = await response.json();
      if (typeof body?.error === "string") message = body.error;
      if (typeof body?.log_path === "string") logPath = body.log_path;
    } catch {
      // Not JSON — the status line is the best message available.
    }
    throw new ApiError(response.status, message, logPath);
  }

  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export const api = {
  projects: () =>
    request<{ projects: Project[] }>("/projects").then((r) => r.projects),

  status: (projectId: string) =>
    request<{ processes: Process[] }>(`/projects/${projectId}/status?verbose=true`).then(
      (r) => r.processes,
    ),

  ports: (projectId: string) =>
    request<PortManifest>(`/projects/${projectId}/ports`),

  logs: (projectId: string, name: string, last = 500) =>
    request<{ lines: string[] }>(
      `/projects/${projectId}/logs/${encodeURIComponent(name)}?last=${last}`,
    ).then((r) => r.lines),

  start: (projectId: string, name: string) =>
    request<void>(`/projects/${projectId}/start/${encodeURIComponent(name)}`, {
      method: "POST",
    }),

  stop: (projectId: string, name: string) =>
    request<void>(`/projects/${projectId}/stop/${encodeURIComponent(name)}`, {
      method: "POST",
    }),

  restart: (projectId: string, name: string) =>
    request<void>(`/projects/${projectId}/restart/${encodeURIComponent(name)}`, {
      method: "POST",
    }),

  run: (projectId: string, name: string, params: Record<string, string>) =>
    request<void>(`/projects/${projectId}/run/${encodeURIComponent(name)}`, {
      method: "POST",
      body: JSON.stringify({ params }),
    }),

  completions: (
    projectId: string,
    task: string,
    param: string,
    partial: Record<string, string>,
    forceRefresh = false,
  ) =>
    request<{ values: string[] }>(
      `/projects/${projectId}/completions/${encodeURIComponent(task)}/${encodeURIComponent(param)}`,
      {
        method: "POST",
        body: JSON.stringify({ partial, force_refresh: forceRefresh }),
      },
    ).then((r) => r.values),

  /** SSE URL for a project's state changes. */
  eventsUrl: (projectId: string) => `/api/projects/${projectId}/events`,

  /** SSE URL for a live log tail. */
  logStreamUrl: (projectId: string, name: string, last = 200) =>
    `/api/projects/${projectId}/logs/${encodeURIComponent(name)}/stream?last=${last}`,
};
