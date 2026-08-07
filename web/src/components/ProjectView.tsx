import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import { useStatus } from "../hooks";
import type { Process, PortManifest, Project } from "../types";
import { LogPane } from "./LogPane";
import { RunTaskDialog } from "./RunTaskDialog";
import { StatePill } from "./StatePill";

interface Props {
  project: Project;
  onBack: () => void;
}

/** Services a control action can be applied to, given the current state. */
function actions(process: Process): { start: boolean; stop: boolean; restart: boolean } {
  if (process.kind !== "service") {
    return { start: false, stop: false, restart: false };
  }
  const stopped = process.state === "stopped" || process.state === "failed";
  return { start: stopped, stop: !stopped, restart: !stopped };
}

export function ProjectView({ project, onBack }: Props) {
  const { processes, loading, error, refresh } = useStatus(project.id);
  const [selected, setSelected] = useState<string | null>(null);
  const [ports, setPorts] = useState<PortManifest | null>(null);
  const [runTask, setRunTask] = useState<Process | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  useEffect(() => {
    api.ports(project.id).then(setPorts).catch(() => setPorts(null));
  }, [project.id]);

  // Default the log pane to the first process so the page is useful immediately.
  useEffect(() => {
    if (!selected && processes.length > 0) setSelected(processes[0]?.name ?? null);
  }, [processes, selected]);

  async function act(action: () => Promise<void>) {
    setActionError(null);
    try {
      await action();
      // The event stream carries the state change; this catches anything
      // that isn't a plain state transition.
      refresh();
    } catch (e) {
      setActionError((e as ApiError).message);
    }
  }

  const pendingRun = processes.filter(
    (process) => process.kind === "task" && process.state === "pending_run",
  );

  return (
    <div className="project-view">
      <header className="page-header">
        <button className="ghost" onClick={onBack}>
          ← projects
        </button>
        <div className="page-title">
          <h1>{project.name}</h1>
          <p className="muted">
            {project.root}
            {project.profile && <> · profile <code>{project.profile}</code></>}
            {" · pid "}
            {project.pid}
          </p>
        </div>
        {pendingRun.length > 0 && (
          <button onClick={() => act(() => api.runPending(project.id))}>
            run {pendingRun.length} pending task
            {pendingRun.length === 1 ? "" : "s"}
          </button>
        )}
      </header>

      {error && <p className="error">{error}</p>}
      {actionError && <p className="error">{actionError}</p>}

      <div className="columns">
        <section className="processes">
          {loading && processes.length === 0 && <p className="muted">loading…</p>}
          <table>
            <tbody>
              {processes.map((process) => {
                const can = actions(process);
                const params = process.verbose?.params ?? [];
                return (
                  <tr
                    key={process.name}
                    className={selected === process.name ? "selected" : undefined}
                    onClick={() => setSelected(process.name)}
                  >
                    <td className="process-name">
                      <span className={`kind kind-${process.kind}`}>
                        {process.kind === "service" ? "svc" : "task"}
                      </span>
                      {process.name}
                      {process.failed_dependencies &&
                        process.failed_dependencies.length > 0 && (
                          <span className="muted">
                            {" "}
                            ← {process.failed_dependencies.join(", ")}
                          </span>
                        )}
                    </td>
                    <td>
                      <StatePill process={process} />
                    </td>
                    <td className="process-actions">
                      {can.start && (
                        <button
                          className="ghost"
                          onClick={(e) => {
                            e.stopPropagation();
                            act(() => api.start(project.id, process.name));
                          }}
                        >
                          start
                        </button>
                      )}
                      {can.restart && (
                        <button
                          className="ghost"
                          onClick={(e) => {
                            e.stopPropagation();
                            act(() => api.restart(project.id, process.name));
                          }}
                        >
                          restart
                        </button>
                      )}
                      {can.stop && (
                        <button
                          className="ghost"
                          onClick={(e) => {
                            e.stopPropagation();
                            act(() => api.stop(project.id, process.name));
                          }}
                        >
                          stop
                        </button>
                      )}
                      {process.kind === "task" && (
                        <button
                          className="ghost"
                          onClick={(e) => {
                            e.stopPropagation();
                            // Params are collected in a dialog only when the
                            // task declares some; otherwise run straight away.
                            if (params.length > 0) setRunTask(process);
                            else act(() => api.run(project.id, process.name, {}));
                          }}
                        >
                          run
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>

          {ports && ports.services && Object.keys(ports.services).length > 0 && (
            <div className="ports">
              <h3>ports</h3>
              <ul>
                {Object.entries(ports.services).flatMap(([name, entry]) =>
                  (entry.proxy ?? []).map((p) => (
                    <li key={`${name}-${p.bound_addr}`}>
                      <code>{name}</code> {p.bound_addr}
                      {p.configured_addr !== p.bound_addr && (
                        <span className="muted"> (asked {p.configured_addr})</span>
                      )}
                    </li>
                  )),
                )}
              </ul>
            </div>
          )}
        </section>

        {selected && <LogPane projectId={project.id} name={selected} />}
      </div>

      {runTask && (
        <RunTaskDialog
          projectId={project.id}
          task={runTask.name}
          params={runTask.verbose?.params ?? []}
          onClose={() => setRunTask(null)}
          onRan={refresh}
        />
      )}
    </div>
  );
}
