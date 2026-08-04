/**
 * Thin wrapper over the don web API.
 *
 * Auth rides on a cookie the server set when the tokenized URL was first
 * opened, so nothing here handles credentials — a 401 means the tab was
 * opened without going through `don ui`, and the right response is to say so
 * rather than to retry.
 */

import type { Item, PortManifest, Project } from "./types";

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
    request<{ items: Item[] }>(`/projects/${projectId}/status?verbose=true`).then(
      (r) => r.items,
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

  runPending: (projectId: string) =>
    request<void>(`/projects/${projectId}/run-pending`, { method: "POST" }),

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
