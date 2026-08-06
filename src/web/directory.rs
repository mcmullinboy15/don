//! Where the web layer finds projects.
//!
//! The web API is written against exactly one question — "given this id,
//! which unix socket do I talk to?" — so both hosting modes can share every
//! handler. The daemon answers from its registry; `don start --with-ui`
//! answers with the single project it is already running.
//!
//! A two-variant enum rather than a `dyn` trait: the set of answers is
//! closed, and it keeps the whole thing allocation-free and `async fn`
//! without a trait-object dance.

use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

/// A project as the web layer sees it.
///
/// The web layer's own type, deliberately not the daemon's registry row:
/// `web` is a presentation layer over *some* directory of projects, and
/// importing the daemon's types made `daemon ↔ web` a dependency cycle.
/// Field names and serde attributes mirror the registry row exactly, so
/// the JSON the SPA consumes is unchanged.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Project {
    /// Stable, URL-safe identifier.
    pub id: String,
    /// Display name — the project directory's basename.
    pub name: String,
    /// Canonical project root (the directory holding `don.toml`).
    pub root: PathBuf,
    /// The project's API socket, normally `<root>/.don/don.sock`.
    pub socket: PathBuf,
    /// PID of the runner process.
    pub pid: u32,
    /// Active profile, when the stack was started with one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// When the project registered.
    pub registered_at_unix_secs: u64,
}

/// The two questions the web layer asks of whoever owns the project set.
///
/// The daemon answers from its registry through a small adapter task on
/// *its* side of the dependency arrow; the web layer never learns the
/// registry's vocabulary.
pub(crate) enum DirectoryQuery {
    /// Every project currently visible.
    List {
        reply: oneshot::Sender<Vec<Project>>,
    },
    /// Resolve one project by id.
    Get {
        id: String,
        reply: oneshot::Sender<Option<Project>>,
    },
}

/// Resolves project ids to the projects behind them.
#[derive(Clone)]
pub(crate) enum ProjectDirectory {
    /// Backed by an external owner (the daemon's registry task, via its
    /// adapter). Reads prune dead projects as a side effect there, so the
    /// UI never lists a stack that has gone away.
    Queried {
        query_tx: mpsc::UnboundedSender<DirectoryQuery>,
    },
    /// A single project, for `don start --with-ui`. There is no registry and
    /// nothing to prune — if this process is serving, its project is alive.
    Single(Box<Project>),
}

impl ProjectDirectory {
    /// Every project currently visible, sorted by name.
    pub(crate) async fn list(&self) -> Vec<Project> {
        match self {
            Self::Queried { query_tx } => {
                let (tx, rx) = oneshot::channel();
                if query_tx.send(DirectoryQuery::List { reply: tx }).is_err() {
                    return Vec::new();
                }
                rx.await.unwrap_or_default()
            }
            Self::Single(project) => vec![(**project).clone()],
        }
    }

    /// Resolve one project by id.
    pub(crate) async fn get(&self, id: &str) -> Option<Project> {
        match self {
            Self::Queried { query_tx } => {
                let (tx, rx) = oneshot::channel();
                query_tx
                    .send(DirectoryQuery::Get {
                        id: id.to_string(),
                        reply: tx,
                    })
                    .ok()?;
                rx.await.ok().flatten()
            }
            Self::Single(project) => (project.id == id).then(|| (**project).clone()),
        }
    }
}
