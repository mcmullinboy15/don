//! The daemon's registry of currently-running don projects.
//!
//! An entry is pure metadata: where the project lives and how to reach its
//! existing unix-socket API. The daemon never owns a project's processes, so
//! an entry going stale is normal — a `kill -9`'d runner leaves one behind.
//! Rather than heartbeat every project, the registry is pruned lazily: before
//! anything reads it, entries whose socket no longer answers are dropped.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How long to wait for a project's socket to answer before calling it dead.
/// Generous enough for a loaded machine, short enough that listing projects
/// stays interactive even with several dead entries.
const PROBE_TIMEOUT: Duration = Duration::from_millis(250);

/// One project currently known to the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEntry {
    /// Stable, URL-safe identifier derived from the canonical project root.
    /// Re-registering the same directory reuses the id, so a restarted stack
    /// replaces its old entry instead of accumulating duplicates.
    pub id: String,
    /// Display name — the project directory's basename.
    pub name: String,
    /// Canonical project root (the directory holding `don.toml`).
    pub root: PathBuf,
    /// The project's API socket, normally `<root>/.don/don.sock`.
    pub socket: PathBuf,
    /// PID of the `don start` process that registered.
    pub pid: u32,
    /// Active profile, when the stack was started with one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// When the project registered.
    pub registered_at_unix_secs: u64,
}

/// The registry row, translated into the web layer's vocabulary. Lives
/// here — on the daemon's side of the arrow — so `web` never imports the
/// registry.
impl From<ProjectEntry> for crate::web::Project {
    fn from(entry: ProjectEntry) -> Self {
        crate::web::Project {
            id: entry.id,
            name: entry.name,
            root: entry.root,
            socket: entry.socket,
            pid: entry.pid,
            profile: entry.profile,
            registered_at_unix_secs: entry.registered_at_unix_secs,
        }
    }
}

impl ProjectEntry {
    /// Build an entry for a project rooted at `root`.
    ///
    /// `root` should already be canonicalized — the runner canonicalizes its
    /// base dir at startup (`runner::setup::canonicalize_base_dir`), so the
    /// id is stable across `don start` invocations from different cwds.
    pub fn new(root: PathBuf, pid: u32, profile: Option<String>) -> Self {
        let name = root
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| root.display().to_string());
        Self {
            id: project_id(&root),
            name,
            socket: root.join(".don").join("don.sock"),
            root,
            pid,
            profile,
            registered_at_unix_secs: now_unix_secs(),
        }
    }
}

/// Derive a project's stable id from its canonical root path.
///
/// A hash rather than the path itself: ids end up in URLs, and a raw path
/// would need escaping, would leak the user's directory layout into browser
/// history, and would make routes awkward to read.
pub fn project_id(root: &Path) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(root.as_os_str().as_encoded_bytes());
    let digest = hasher.finalize();
    hex::encode(&digest[..6])
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The set of projects the daemon knows about, backed by a JSON file.
///
/// The on-disk copy exists so a daemon restart (a machine reboot, a
/// `systemctl --user restart don`) doesn't lose sight of stacks that are
/// still running. It is a cache, not a source of truth: liveness is always
/// re-derived by probing sockets.
#[derive(Debug)]
pub struct ProjectRegistry {
    path: PathBuf,
    entries: HashMap<String, ProjectEntry>,
}

/// What `ProjectRegistry::load` found on disk. A corrupt or unreadable file
/// is not fatal — the daemon starts with an empty registry and projects
/// re-register — but the caller should say so rather than silently dropping
/// entries.
#[derive(Debug, PartialEq, Eq)]
pub enum LoadOutcome {
    /// No registry file existed yet.
    Fresh,
    /// Entries were restored from disk.
    Restored { count: usize },
    /// The file existed but couldn't be used; starting empty.
    Discarded { reason: String },
}

impl ProjectRegistry {
    /// Load the registry from `path`, falling back to empty.
    pub fn load(path: PathBuf) -> (Self, LoadOutcome) {
        let outcome = match std::fs::read(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => LoadOutcome::Fresh,
            Err(e) => LoadOutcome::Discarded {
                reason: e.to_string(),
            },
            Ok(bytes) => match serde_json::from_slice::<Vec<ProjectEntry>>(&bytes) {
                Ok(entries) => {
                    let count = entries.len();
                    let map = entries.into_iter().map(|e| (e.id.clone(), e)).collect();
                    return (Self { path, entries: map }, LoadOutcome::Restored { count });
                }
                Err(e) => LoadOutcome::Discarded {
                    reason: e.to_string(),
                },
            },
        };
        (
            Self {
                path,
                entries: HashMap::new(),
            },
            outcome,
        )
    }

    /// Add or replace an entry. Returns true when this replaced an existing
    /// registration for the same project.
    pub fn register(&mut self, entry: ProjectEntry) -> bool {
        self.entries.insert(entry.id.clone(), entry).is_some()
    }

    /// Remove an entry by id. Returns true when something was removed.
    pub fn deregister(&mut self, id: &str) -> bool {
        self.entries.remove(id).is_some()
    }

    /// Look up a single project.
    pub fn get(&self, id: &str) -> Option<&ProjectEntry> {
        self.entries.get(id)
    }

    /// All entries, sorted by name for a stable UI ordering.
    pub fn list(&self) -> Vec<ProjectEntry> {
        let mut out: Vec<ProjectEntry> = self.entries.values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
        out
    }

    /// Number of registered projects.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry holds no projects.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drop every entry whose id is absent from `alive`, returning what was
    /// removed. Split out from the probing itself so the retention rule is
    /// testable without sockets.
    pub fn retain_ids(&mut self, alive: &HashSet<String>) -> Vec<ProjectEntry> {
        let dead: Vec<String> = self
            .entries
            .keys()
            .filter(|id| !alive.contains(*id))
            .cloned()
            .collect();
        dead.iter()
            .filter_map(|id| self.entries.remove(id))
            .collect()
    }

    /// Probe every entry's socket and drop the ones that don't answer.
    /// Returns the removed entries so the caller can log them.
    ///
    /// Probes run concurrently, so the whole prune costs one [`PROBE_TIMEOUT`]
    /// in the worst case rather than one per dead project. That matters
    /// because pruning happens inline on the daemon's command loop.
    pub async fn prune(&mut self) -> Vec<ProjectEntry> {
        let candidates = self.list();
        let probes = candidates
            .into_iter()
            .map(|entry| async move { is_reachable(&entry.socket).await.then_some(entry.id) });
        let alive: HashSet<String> = futures_util::future::join_all(probes)
            .await
            .into_iter()
            .flatten()
            .collect();
        self.retain_ids(&alive)
    }

    /// Write the registry to disk atomically (temp file + rename) so a
    /// crash mid-write can't leave a half-written file behind.
    pub fn persist(&self) -> Result<(), std::io::Error> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(&self.list())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let tmp = self.path.with_extension("json.tmp");
        write_private(&tmp, &bytes)?;
        std::fs::rename(&tmp, &self.path)
    }
}

/// Write `bytes` to `path` with owner-only permissions. The registry names
/// every project directory the user is working in — not a secret, but not
/// something to hand the rest of the machine either.
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Whether a unix socket accepts a connection right now.
///
/// This is the daemon's entire liveness model. It beats checking the PID:
/// a recycled PID would read as alive, and a runner whose API socket is gone
/// is useless to the UI regardless of whether the process still exists.
pub async fn is_reachable(socket: &Path) -> bool {
    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, tokio::net::UnixStream::connect(socket)).await,
        Ok(Ok(_))
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn entry(root: &str, pid: u32) -> ProjectEntry {
        ProjectEntry::new(PathBuf::from(root), pid, None)
    }

    #[test]
    fn project_id_is_stable_and_path_specific() {
        struct Case {
            name: &'static str,
            left: &'static str,
            right: &'static str,
            same: bool,
        }

        let cases = vec![
            Case {
                name: "identical paths match",
                left: "/home/u/work/api",
                right: "/home/u/work/api",
                same: true,
            },
            Case {
                name: "sibling directories differ",
                left: "/home/u/work/api",
                right: "/home/u/work/web",
                same: false,
            },
            Case {
                name: "trailing component matters",
                left: "/home/u/work",
                right: "/home/u/work/api",
                same: false,
            },
            Case {
                name: "same basename under different parents differ",
                left: "/a/api",
                right: "/b/api",
                same: false,
            },
        ];

        for case in cases {
            let left = project_id(Path::new(case.left));
            let right = project_id(Path::new(case.right));
            assert_eq!(
                left == right,
                case.same,
                "case: {} ({left} vs {right})",
                case.name
            );
            assert_eq!(left.len(), 12, "case: {} — id should be 12 hex", case.name);
        }
    }

    #[test]
    fn entry_derives_name_and_socket_from_root() {
        let e = entry("/home/u/work/api", 42);
        assert_eq!(e.name, "api");
        assert_eq!(e.socket, PathBuf::from("/home/u/work/api/.don/don.sock"));
        assert_eq!(e.pid, 42);
        assert_eq!(e.id, project_id(Path::new("/home/u/work/api")));
    }

    #[test]
    fn register_replaces_same_project_and_reports_it() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut reg, outcome) = ProjectRegistry::load(tmp.path().join("registry.json"));
        assert_eq!(outcome, LoadOutcome::Fresh);

        assert!(!reg.register(entry("/p/one", 1)), "first insert is new");
        assert!(
            !reg.register(entry("/p/two", 2)),
            "different project is new"
        );
        assert_eq!(reg.len(), 2);

        // Same directory, new process — replaces rather than duplicates.
        assert!(reg.register(entry("/p/one", 99)), "re-register replaces");
        assert_eq!(reg.len(), 2);
        let id = project_id(Path::new("/p/one"));
        assert_eq!(reg.get(&id).unwrap().pid, 99);
    }

    #[test]
    fn deregister_removes_only_the_named_project() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut reg, _) = ProjectRegistry::load(tmp.path().join("registry.json"));
        reg.register(entry("/p/one", 1));
        reg.register(entry("/p/two", 2));

        assert!(reg.deregister(&project_id(Path::new("/p/one"))));
        assert!(
            !reg.deregister(&project_id(Path::new("/p/one"))),
            "idempotent"
        );
        assert!(!reg.deregister("nonexistent"));
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.list()[0].name, "two");
    }

    #[test]
    fn retain_ids_drops_everything_not_marked_alive() {
        struct Case {
            name: &'static str,
            registered: Vec<&'static str>,
            alive: Vec<&'static str>,
            expect_remaining: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "all alive keeps everything",
                registered: vec!["/p/a", "/p/b"],
                alive: vec!["/p/a", "/p/b"],
                expect_remaining: vec!["a", "b"],
            },
            Case {
                name: "none alive empties the registry",
                registered: vec!["/p/a", "/p/b"],
                alive: vec![],
                expect_remaining: vec![],
            },
            Case {
                name: "partial liveness keeps only the survivors",
                registered: vec!["/p/a", "/p/b", "/p/c"],
                alive: vec!["/p/b"],
                expect_remaining: vec!["b"],
            },
            Case {
                name: "alive ids that aren't registered are ignored",
                registered: vec!["/p/a"],
                alive: vec!["/p/a", "/p/ghost"],
                expect_remaining: vec!["a"],
            },
        ];

        for case in cases {
            let tmp = tempfile::tempdir().unwrap();
            let (mut reg, _) = ProjectRegistry::load(tmp.path().join("registry.json"));
            for root in &case.registered {
                reg.register(entry(root, 1));
            }
            let alive: HashSet<String> = case
                .alive
                .iter()
                .map(|r| project_id(Path::new(r)))
                .collect();

            let removed = reg.retain_ids(&alive);
            let remaining: Vec<String> = reg.list().into_iter().map(|e| e.name).collect();
            assert_eq!(remaining, case.expect_remaining, "case: {}", case.name);
            assert_eq!(
                removed.len(),
                case.registered.len() - case.expect_remaining.len(),
                "case: {} — removed count",
                case.name
            );
        }
    }

    #[test]
    fn list_is_sorted_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut reg, _) = ProjectRegistry::load(tmp.path().join("registry.json"));
        for root in ["/p/zeta", "/p/alpha", "/p/mid"] {
            reg.register(entry(root, 1));
        }
        let names: Vec<String> = reg.list().into_iter().map(|e| e.name).collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
    }

    #[test]
    fn persist_then_load_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");

        let (mut reg, _) = ProjectRegistry::load(path.clone());
        reg.register(ProjectEntry::new(
            PathBuf::from("/p/one"),
            7,
            Some("dev".into()),
        ));
        reg.register(entry("/p/two", 8));
        reg.persist().unwrap();

        let (restored, outcome) = ProjectRegistry::load(path.clone());
        assert_eq!(outcome, LoadOutcome::Restored { count: 2 });
        assert_eq!(restored.list(), reg.list());
        assert_eq!(
            restored
                .get(&project_id(Path::new("/p/one")))
                .unwrap()
                .profile,
            Some("dev".to_string())
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "registry should be owner-only");
        }
    }

    #[test]
    fn load_survives_a_corrupt_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        std::fs::write(&path, b"{ this is not json").unwrap();

        let (reg, outcome) = ProjectRegistry::load(path);
        assert!(reg.is_empty());
        assert!(
            matches!(outcome, LoadOutcome::Discarded { .. }),
            "got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn prune_drops_projects_whose_socket_is_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let live_root = tmp.path().join("live");
        let dead_root = tmp.path().join("dead");
        std::fs::create_dir_all(live_root.join(".don")).unwrap();

        // A real listener for the live project; nothing at all for the dead one.
        let _listener =
            tokio::net::UnixListener::bind(live_root.join(".don").join("don.sock")).unwrap();

        let (mut reg, _) = ProjectRegistry::load(tmp.path().join("registry.json"));
        reg.register(ProjectEntry::new(live_root, 1, None));
        reg.register(ProjectEntry::new(dead_root, 2, None));
        assert_eq!(reg.len(), 2);

        let removed = reg.prune().await;
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].name, "dead");
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.list()[0].name, "live");
    }
}
