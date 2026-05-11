//! `don init` — scaffold a starter `don.toml` for a new project.
//!
//! Writes a commented template showing each preset (run, docker, rust, go,
//! bazel, turbo) plus tasks and profiles. The file is meant to be edited;
//! every section is commented out so `don validate` passes on the raw output.

use std::path::Path;

const STARTER_TEMPLATE: &str = r#"# don — dev environment orchestrator.
# Uncomment the sections you want. Each service/task/profile is optional.

# Pick a default profile to limit what `don start` brings up without `--profile`.
# default_profile = "dev"
# Ignore generated files across every watch rule in the workspace.
# watch_ignore = ["target/**", ".don/**"]

# ── Services ────────────────────────────────────────────────────────────────
# Long-running processes. Don keeps them alive and restarts on file changes.

# [services.api]
# run.cmd = "node"
# run.args = ["server.js"]
# env = { PORT = "3000" }
# watch = ["src/**/*.js"]
# ready.http = "http://localhost:3000/health"

# Docker preset — run a container via the local Docker daemon.
# [services.postgres]
# docker.image = "postgres:16"
# docker.ports = ["5432:5432"]
# docker.env_file = [".env"]
# ready.tcp = "127.0.0.1:5432"

# Rust preset — runs `cargo build --bin <name>`, watches src/**/*.rs.
# [services.api]
# rust.binary = "api"

# Go preset — runs `go build -o .don/bin/<name> <package>`, watches **/*.go.
# [services.api]
# go.package = "./cmd/api"

# Bazel preset — auto-resolves watch patterns from `bazel query`.
# [services.api]
# bazel.target = "//services/api:api"
# bazel.watch = false  # disable auto-resolved file watches, keep startup build
# proxy = { listen = "127.0.0.1:8080", env = "PORT" }

# Turborepo preset — auto-resolves watch patterns from the task graph.
# [services.web]
# turbo.task = "dev"
# turbo.filter = "@myorg/web"

# ── Tasks ───────────────────────────────────────────────────────────────────
# One-shot commands. Re-run only when watched files change.

# [tasks.migrate]
# cmd = "dbmate"
# args = ["up"]
# depends_on = ["postgres"]
# watch = ["db/migrations/**/*.sql"]

# Set auto_run = false to defer execution — when the task needs to run,
# trigger it with `don run <name>` or `don run --all-pending`.
# [tasks.seed]
# cmd = "./scripts/seed-db"
# auto_run = false
# depends_on = ["migrate"]

# Set auto_run = "once" to auto-run on startup until the first successful run,
# then require manual triggers forever after.
# [tasks.bootstrap]
# cmd = "./scripts/bootstrap-db"
# auto_run = "once"

# ── Profiles ────────────────────────────────────────────────────────────────
# Named subsets for focused work. Transitive deps are included automatically.

# [profiles.dev]
# services = ["api", "postgres"]
# tasks = ["migrate"]
"#;

/// Write the starter template at `path`. When `force` is false, returns an
/// error if a file already exists there.
pub fn write_starter_config(path: &Path, force: bool) -> Result<(), String> {
    if path.exists() && !force {
        return Err(format!(
            "{} already exists — pass --force to overwrite",
            path.display()
        ));
    }
    std::fs::write(path, STARTER_TEMPLATE)
        .map_err(|e| format!("failed to write {}: {e}", path.display()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    struct InitTestCase {
        name: &'static str,
        pre_exists: bool,
        force: bool,
        expect_err: bool,
    }

    #[test]
    fn write_starter_config_table() {
        let cases = [
            InitTestCase {
                name: "writes new file",
                pre_exists: false,
                force: false,
                expect_err: false,
            },
            InitTestCase {
                name: "refuses existing file without --force",
                pre_exists: true,
                force: false,
                expect_err: true,
            },
            InitTestCase {
                name: "overwrites with --force",
                pre_exists: true,
                force: true,
                expect_err: false,
            },
        ];

        for case in cases {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("don.toml");
            if case.pre_exists {
                std::fs::write(&path, "existing contents").unwrap();
            }
            let result = write_starter_config(&path, case.force);
            assert_eq!(
                result.is_err(),
                case.expect_err,
                "{}: expected err={} got {:?}",
                case.name,
                case.expect_err,
                result
            );
            if !case.expect_err {
                let contents = std::fs::read_to_string(&path).unwrap();
                assert!(
                    contents.contains("# don — dev environment orchestrator"),
                    "{}: template header missing",
                    case.name
                );
            }
        }
    }

    #[test]
    fn starter_template_is_valid_toml() {
        let parsed: toml::Value = toml::from_str(STARTER_TEMPLATE).unwrap();
        assert!(parsed.as_table().unwrap().is_empty());
    }
}
