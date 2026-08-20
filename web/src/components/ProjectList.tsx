import type { Project } from "../types";

interface Props {
  projects: Project[];
  loading: boolean;
  error: string | null;
  onOpen: (id: string) => void;
}

export function ProjectList({ projects, loading, error, onOpen }: Props) {
  return (
    <div className="project-list">
      <header className="page-header">
        <h1>don</h1>
      </header>

      {error && <p className="error">{error}</p>}
      {loading && projects.length === 0 && <p className="muted">loading…</p>}

      {!loading && projects.length === 0 && !error && (
        <div className="empty">
          <p>No don projects are running.</p>
          <p className="muted">
            Run <code>don start</code> in a project and it will show up here.
          </p>
        </div>
      )}

      <ul className="cards">
        {projects.map((project) => (
          <li key={project.id}>
            <button className="card" onClick={() => onOpen(project.id)}>
              <span className="card-title">{project.name}</span>
              <span className="muted card-path">{project.root}</span>
              <span className="muted">
                pid {project.pid}
                {project.profile && (
                  <>
                    {" · profile "}
                    <code>{project.profile}</code>
                  </>
                )}
              </span>
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}
