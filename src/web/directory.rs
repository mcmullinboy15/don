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

use crate::daemon::registry::ProjectEntry;
use crate::daemon::routes::DaemonCommand;
use tokio::sync::{mpsc, oneshot};

/// Resolves project ids to the projects behind them.
#[derive(Clone)]
pub(crate) enum ProjectDirectory {
    /// Backed by the daemon's registry task. Reads prune dead projects as a
    /// side effect, so the UI never lists a stack that has gone away.
    Daemon {
        cmd_tx: mpsc::UnboundedSender<DaemonCommand>,
    },
    /// A single project, for `don start --with-ui`. There is no registry and
    /// nothing to prune — if this process is serving, its project is alive.
    Single(Box<ProjectEntry>),
}

impl ProjectDirectory {
    /// Every project currently visible, sorted by name.
    pub(crate) async fn list(&self) -> Vec<ProjectEntry> {
        match self {
            Self::Daemon { cmd_tx } => {
                let (tx, rx) = oneshot::channel();
                if cmd_tx.send(DaemonCommand::List { reply: tx }).is_err() {
                    return Vec::new();
                }
                rx.await.unwrap_or_default()
            }
            Self::Single(entry) => vec![(**entry).clone()],
        }
    }

    /// Resolve one project by id.
    pub(crate) async fn get(&self, id: &str) -> Option<ProjectEntry> {
        match self {
            Self::Daemon { cmd_tx } => {
                let (tx, rx) = oneshot::channel();
                cmd_tx
                    .send(DaemonCommand::Get {
                        id: id.to_string(),
                        reply: tx,
                    })
                    .ok()?;
                rx.await.ok().flatten()
            }
            Self::Single(entry) => (entry.id == id).then(|| (**entry).clone()),
        }
    }
}
