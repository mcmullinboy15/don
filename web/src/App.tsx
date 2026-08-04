import { useCallback, useEffect, useState } from "react";
import { api, ApiError } from "./api";
import { ProjectList } from "./components/ProjectList";
import { ProjectView } from "./components/ProjectView";
import { useRoute } from "./hooks";
import type { Project } from "./types";

/**
 * How often the project list is refreshed.
 *
 * Projects appear and disappear through the daemon's registry, not through a
 * runner event stream, so this is the one place polling is the right answer.
 * Slow enough to be invisible, fast enough that a `don start` in another
 * terminal shows up before you switch windows.
 */
const PROJECT_POLL_MS = 3000;

export function App() {
  const { projectId, navigate } = useRoute();
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(() => {
    api
      .projects()
      .then((next) => {
        setProjects(next);
        setError(null);
      })
      .catch((e: ApiError) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    refresh();
    const timer = setInterval(refresh, PROJECT_POLL_MS);
    return () => clearInterval(timer);
  }, [refresh]);

  const project = projects.find((p) => p.id === projectId);

  // A project that shut down while it was open shouldn't leave a dead page.
  useEffect(() => {
    if (projectId && !loading && projects.length > 0 && !project) {
      navigate(null);
    }
  }, [projectId, project, projects, loading, navigate]);

  if (project) {
    return <ProjectView project={project} onBack={() => navigate(null)} />;
  }

  return (
    <ProjectList
      projects={projects}
      loading={loading}
      error={error}
      onOpen={navigate}
    />
  );
}
