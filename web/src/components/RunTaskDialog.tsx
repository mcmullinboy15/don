import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import type { ParamInfo } from "../types";

interface Props {
  projectId: string;
  task: string;
  params: ParamInfo[];
  onClose: () => void;
  onRan: () => void;
}

/**
 * Collect a task's params and run it.
 *
 * Params with a `completions` command are resolved by the daemon (which runs
 * the command in the project's environment) rather than guessed at here — the
 * same values the CLI and TUI would offer.
 */
export function RunTaskDialog({ projectId, task, params, onClose, onRan }: Props) {
  const [values, setValues] = useState<Record<string, string>>(() =>
    Object.fromEntries(params.map((p) => [p.name, p.default ?? ""])),
  );
  const [candidates, setCandidates] = useState<Record<string, string[]>>({});
  const [error, setError] = useState<string | null>(null);
  const [logPath, setLogPath] = useState<string | undefined>();
  const [running, setRunning] = useState(false);

  // Resolve dynamic completions once the form opens.
  useEffect(() => {
    let cancelled = false;
    for (const param of params) {
      if (!param.has_completions) continue;
      api
        .completions(projectId, task, param.name, values, false)
        .then((found) => {
          if (!cancelled) {
            setCandidates((c) => ({ ...c, [param.name]: found }));
          }
        })
        .catch((e: ApiError) => {
          if (!cancelled) {
            setError(`completions for '${param.name}': ${e.message}`);
            setLogPath(e.logPath);
          }
        });
    }
    return () => {
      cancelled = true;
    };
    // Deliberately runs once per open: re-resolving on every keystroke would
    // shell out constantly. A refresh button covers the rest.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId, task]);

  const missing = params.filter(
    (p) => p.required && !(values[p.name] ?? "").trim(),
  );

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    if (missing.length > 0) return;
    setRunning(true);
    setError(null);
    setLogPath(undefined);
    try {
      // Drop empty optional values so the daemon applies its own defaults.
      const supplied = Object.fromEntries(
        Object.entries(values).filter(([, v]) => v !== ""),
      );
      await api.run(projectId, task, supplied);
      onRan();
      onClose();
    } catch (e) {
      const err = e as ApiError;
      setError(err.message);
      setLogPath(err.logPath);
      setRunning(false);
    }
  }

  return (
    <div className="overlay" onClick={onClose}>
      <form
        className="dialog"
        onClick={(e) => e.stopPropagation()}
        onSubmit={submit}
      >
        <h2>
          run <code>{task}</code>
        </h2>

        {params.length === 0 && (
          <p className="muted">This task takes no parameters.</p>
        )}

        {params.map((param) => {
          const options = param.choices?.length
            ? param.choices
            : (candidates[param.name] ?? []);
          const value = values[param.name] ?? "";
          const set = (v: string) =>
            setValues((current) => ({ ...current, [param.name]: v }));

          return (
            <label className="field" key={param.name}>
              <span className="field-label">
                {param.prompt ?? param.name}
                {param.required && <span className="required"> *</span>}
              </span>

              {param.kind === "bool" ? (
                <input
                  type="checkbox"
                  checked={value === "true"}
                  onChange={(e) => set(e.target.checked ? "true" : "false")}
                />
              ) : options.length > 0 ? (
                <select value={value} onChange={(e) => set(e.target.value)}>
                  <option value="">—</option>
                  {options.map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  type={param.kind === "int" ? "number" : "text"}
                  value={value}
                  min={param.min}
                  max={param.max}
                  onChange={(e) => set(e.target.value)}
                />
              )}
            </label>
          );
        })}

        {error && (
          <p className="error">
            {error}
            {logPath && (
              <>
                <br />
                <span className="muted">log: {logPath}</span>
              </>
            )}
          </p>
        )}

        <div className="dialog-actions">
          <button type="button" className="ghost" onClick={onClose}>
            cancel
          </button>
          <button type="submit" disabled={running || missing.length > 0}>
            {running ? "running…" : "run"}
          </button>
        </div>
        {missing.length > 0 && (
          <p className="muted">
            required: {missing.map((p) => p.name).join(", ")}
          </p>
        )}
      </form>
    </div>
  );
}
