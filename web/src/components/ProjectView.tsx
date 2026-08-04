import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import { useStatus } from "../hooks";
import type { Item, PortManifest, Project } from "../types";
import { LogPane } from "./LogPane";
import { RunTaskDialog } from "./RunTaskDialog";
import { StatePill } from "./StatePill";

interface Props {
  project: Project;
  onBack: () => void;
}

/** Services a control action can be applied to, given the current state. */
function actions(item: Item): { start: boolean; stop: boolean; restart: boolean } {
  if (item.kind !== "service") {
    return { start: false, stop: false, restart: false };
  }
  const stopped = item.state === "stopped" || item.state === "failed";
  return { start: stopped, stop: !stopped, restart: !stopped };
}

export function ProjectView({ project, onBack }: Props) {
  const { items, loading, error, refresh } = useStatus(project.id);
  const [selected, setSelected] = useState<string | null>(null);
  const [ports, setPorts] = useState<PortManifest | null>(null);
  const [runTask, setRunTask] = useState<Item | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  useEffect(() => {
    api.ports(project.id).then(setPorts).catch(() => setPorts(null));
  }, [project.id]);

  // Default the log pane to the first item so the page is useful immediately.
  useEffect(() => {
    if (!selected && items.length > 0) setSelected(items[0]?.name ?? null);
  }, [items, selected]);

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

  const pendingRun = items.filter(
    (item) => item.kind === "task" && item.state === "pending_run",
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
        <section className="items">
          {loading && items.length === 0 && <p className="muted">loading…</p>}
          <table>
            <tbody>
              {items.map((item) => {
                const can = actions(item);
                const params = item.verbose?.params ?? [];
                return (
                  <tr
                    key={item.name}
                    className={selected === item.name ? "selected" : undefined}
                    onClick={() => setSelected(item.name)}
                  >
                    <td className="item-name">
                      <span className={`kind kind-${item.kind}`}>
                        {item.kind === "service" ? "svc" : "task"}
                      </span>
                      {item.name}
                      {item.failed_dependencies &&
                        item.failed_dependencies.length > 0 && (
                          <span className="muted">
                            {" "}
                            ← {item.failed_dependencies.join(", ")}
                          </span>
                        )}
                    </td>
                    <td>
                      <StatePill item={item} />
                    </td>
                    <td className="item-actions">
                      {can.start && (
                        <button
                          className="ghost"
                          onClick={(e) => {
                            e.stopPropagation();
                            act(() => api.start(project.id, item.name));
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
                            act(() => api.restart(project.id, item.name));
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
                            act(() => api.stop(project.id, item.name));
                          }}
                        >
                          stop
                        </button>
                      )}
                      {item.kind === "task" && (
                        <button
                          className="ghost"
                          onClick={(e) => {
                            e.stopPropagation();
                            // Params are collected in a dialog only when the
                            // task declares some; otherwise run straight away.
                            if (params.length > 0) setRunTask(item);
                            else act(() => api.run(project.id, item.name, {}));
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
