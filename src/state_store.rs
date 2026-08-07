//! A runner state projection that is readable everywhere and writable only by
//! the runner.
//!
//! The runner is the single source of truth for every service's and task's
//! state, but almost everything else in the process needs to *read* that state:
//! `don status`, `GET /status`, `GET /ready`, the web UI's polling, the TUI's
//! status bar. Routing every one of those through the command channel makes
//! reads queue behind whatever the runner is currently doing, which is exactly
//! backwards — a status query is the one thing that should never wait.
//!
//! The obvious shape, `Arc<RwLock<State>>`, hands every holder a `.write()` and
//! reduces single-writer to a rule people have to remember. This module splits
//! the handle by type instead:
//!
//! - [`StateWriter`] is **not** `Clone`. It is moved into the runner at
//!   construction, so no other component can obtain one.
//! - [`StateReader`] is `Clone` and exposes reads only.
//!
//! Single-writer is then enforced by ownership rather than by discipline.
//!
//! # What goes in the snapshot
//!
//! Only the cheap projection: the non-verbose [`ProcessStatus`] values, plus
//! whether the initial startup sweep has finished. Verbose status stays a
//! command — it needs a round trip to the watch manager and a ready-check
//! resolution per service, which must not run on every state transition.
//!
//! # Why reads return `Arc`, not `watch::Ref`
//!
//! [`StateReader::snapshot`] clones an `Arc` rather than handing back a
//! `watch::Ref`. A `Ref` holds a read lock on the channel for as long as it
//! lives, and sooner or later someone holds one across an `.await` and stalls
//! every writer. Returning an `Arc` makes that unrepresentable instead of
//! documenting against it.

use crate::process::{ServiceState, TaskState};
use std::sync::Arc;
use tokio::sync::watch;

/// Status of a single process (service or task) for status queries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ProcessStatus {
    Service {
        name: String,
        state: ServiceState,
        /// Root service/task failures blocking this process.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        failed_dependencies: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        verbose: Option<VerboseInfo>,
    },
    Task {
        name: String,
        state: TaskState,
        /// Root service/task failures blocking this process.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        failed_dependencies: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_run: Option<crate::task_state::TaskRunInfo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        verbose: Option<VerboseInfo>,
    },
}

/// One task param, as a client needs to see it to build a run form.
///
/// Deliberately not [`crate::config::TaskParam`] itself: that carries the
/// `completions` shell command, which is an implementation detail of the
/// daemon and has no business crossing to a browser. Clients learn only
/// *that* a param has dynamic completions, and ask for the values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParamInfo {
    /// Param identifier, as passed to `POST /run/:task`.
    pub name: String,
    /// Human-readable label; falls back to `name` when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    /// Whether the task refuses to run without a value.
    pub required: bool,
    /// Value used when the client doesn't supply one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// One of `string`, `int`, `bool`, `choice`.
    pub kind: String,
    /// Fixed candidate values, when the config lists them inline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub choices: Vec<String>,
    /// Whether candidates come from a completion command — fetch them from
    /// the completions endpoint rather than expecting `choices`.
    #[serde(default)]
    pub has_completions: bool,
    /// Lower bound for `int` params.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    /// Upper bound for `int` params.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
}

impl ParamInfo {
    /// Project a configured param into its client-visible shape.
    pub(crate) fn from_config(param: &crate::config::TaskParam) -> Self {
        use crate::config::ParamKind;
        Self {
            name: param.name.clone(),
            prompt: param.prompt.clone(),
            required: param.required,
            default: param.default.clone(),
            kind: match param.kind {
                ParamKind::String => "string",
                ParamKind::Int => "int",
                ParamKind::Bool => "bool",
                ParamKind::Choice => "choice",
            }
            .to_string(),
            choices: param.choices.clone(),
            has_completions: param.completions.is_some(),
            min: param.validate.as_ref().and_then(|v| v.min),
            max: param.validate.as_ref().and_then(|v| v.max),
        }
    }
}

/// Extended information for verbose status display.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VerboseInfo {
    /// Services/tasks this process depends on. Non-blocking (ordering-only)
    /// edges serialize as `{ name, blocking = false }`, blocking ones as a
    /// string.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<crate::config::Dependency>,
    /// File watch patterns (explicit or resolved from build tool).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub watch: Vec<String>,
    /// Number of file watch patterns resolved for this process.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub watch_count: usize,
    /// Proxy entries, each formatted as `"addr (env=NAME)"` or
    /// `"addr (listenfd)"` for display.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proxy: Vec<String>,
    /// Active Docker mappings, formatted with actual host addresses.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub docker_ports: Vec<String>,
    /// Active Don-managed proxy connections. Present only for env/forward
    /// proxy entries; listenfd connections are owned by the child process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_active_connections: Option<usize>,
    /// Bazel target (if configured).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bazel_target: Option<String>,
    /// Params a task declares, so a client can render a run form without
    /// reading `don.toml` itself. Empty for services.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<ParamInfo>,
    /// Ready check description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<String>,
    /// Run command.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cmd: Option<String>,
    /// Live watch-manager state for this process, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watch_state: Option<String>,
    /// Extra watch diagnostics for this process.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub watch_notes: Vec<String>,
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

/// Whether every service in `processes` has reached an available state — `Ready`
/// (passed its ready check) or `Lazy` (proxy bound, will spawn on first
/// connection). Tasks are ignored: they are one-shot and do not gate stack
/// readiness. An process set with no services is considered ready.
///
/// This is the bool surfaced by `don status --json`, so scripts and agents can
/// poll "is the whole stack up?" without parsing the human-readable table.
/// Status only ever reports the active set (the started profile's subset), so
/// services excluded by a profile do not drag this to `false`.
pub fn all_services_ready(processes: &[ProcessStatus]) -> bool {
    processes.iter().all(|process| match process {
        ProcessStatus::Service { state, .. } => {
            matches!(state, ServiceState::Ready | ServiceState::Lazy)
        }
        ProcessStatus::Task { .. } => true,
    })
}

/// An immutable point-in-time view of runner state.
///
/// Obtained from [`StateReader::snapshot`]. Cheap to clone (it lives behind an
/// `Arc`) and never changes underneath you — a newer snapshot is a new value.
#[derive(Debug, Clone, Default)]
pub struct StateSnapshot {
    /// Every active service and task with its current state, in the same shape
    /// and order `GET /status` returns for a non-verbose query.
    pub processes: Vec<ProcessStatus>,
    /// Whether the runner's initial startup sweep has decided every process.
    ///
    /// `false` until then. Callers that need the runner's answer *about an
    /// process* — rather than its answer about what it is currently doing —
    /// should wait for `true` first.
    pub startup_complete: bool,
}

/// The write half of the state projection. Owned by the runner, and
/// deliberately **not** `Clone`.
///
/// Every method takes `&self` because the underlying channel is internally
/// synchronised; the exclusivity that matters comes from there being exactly
/// one of these in the process.
pub(crate) struct StateWriter {
    tx: watch::Sender<Arc<StateSnapshot>>,
}

impl StateWriter {
    /// Replace the published process list, preserving `startup_complete`.
    pub(crate) fn publish_processes(&self, processes: Vec<ProcessStatus>) {
        // Read and drop the borrow before sending: holding a `Ref` across
        // `send_replace` would deadlock against its own write lock.
        let startup_complete = self.tx.borrow().startup_complete;
        self.tx.send_replace(Arc::new(StateSnapshot {
            processes,
            startup_complete,
        }));
    }

    /// Mark the initial startup sweep as finished (or not), preserving processes.
    pub(crate) fn set_startup_complete(&self, startup_complete: bool) {
        let current = self.tx.borrow();
        if current.startup_complete == startup_complete {
            return;
        }
        let processes = current.processes.clone();
        drop(current);
        self.tx.send_replace(Arc::new(StateSnapshot {
            processes,
            startup_complete,
        }));
    }

    /// Hand out another read-only view. Unlimited readers are fine; they never
    /// block the writer.
    pub(crate) fn reader(&self) -> StateReader {
        StateReader {
            rx: self.tx.subscribe(),
        }
    }
}

/// The read half of the state projection. Clone one per consumer.
///
/// Reads never block on the runner's command loop, so a status query stays
/// answerable while the runner is busy starting services.
#[derive(Clone, Debug)]
pub struct StateReader {
    rx: watch::Receiver<Arc<StateSnapshot>>,
}

impl StateReader {
    /// The latest published snapshot.
    pub fn snapshot(&self) -> Arc<StateSnapshot> {
        self.rx.borrow().clone()
    }

    /// Wait for the next snapshot to be published.
    ///
    /// Returns `None` once the runner is gone and no further updates can
    /// arrive, which callers should treat as "stop waiting", not as an error.
    pub async fn changed(&mut self) -> Option<Arc<StateSnapshot>> {
        self.rx.changed().await.ok()?;
        Some(self.rx.borrow_and_update().clone())
    }

    /// Wait until the runner's initial startup sweep has decided every process.
    ///
    /// Returns `false` if the runner went away first. Await this from your own
    /// task — never from the runner's command loop, which is what would have
    /// to publish the value you are waiting for.
    pub async fn wait_for_startup_complete(&mut self) -> bool {
        if self.snapshot().startup_complete {
            return true;
        }
        while let Some(snapshot) = self.changed().await {
            if snapshot.startup_complete {
                return true;
            }
        }
        false
    }
}

/// Create a linked writer/reader pair seeded with `initial`.
pub(crate) fn channel(initial: StateSnapshot) -> (StateWriter, StateReader) {
    let (tx, rx) = watch::channel(Arc::new(initial));
    (StateWriter { tx }, StateReader { rx })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::process::ServiceState;

    fn service(name: &str, state: ServiceState) -> ProcessStatus {
        ProcessStatus::Service {
            name: name.to_string(),
            state,
            failed_dependencies: Vec::new(),
            verbose: None,
        }
    }

    fn state_of(snapshot: &StateSnapshot, want: &str) -> Option<ServiceState> {
        snapshot.processes.iter().find_map(|process| match process {
            ProcessStatus::Service { name, state, .. } if name == want => Some(*state),
            _ => None,
        })
    }

    #[test]
    fn publish_and_startup_flag_are_independent() {
        struct Case {
            name: &'static str,
            /// Each step either publishes processes or sets the startup flag.
            steps: Vec<Step>,
            want_state: Option<ServiceState>,
            want_startup_complete: bool,
        }
        enum Step {
            Processes(Vec<ProcessStatus>),
            Startup(bool),
        }

        let cases = vec![
            Case {
                name: "initial snapshot is empty and unsettled",
                steps: vec![],
                want_state: None,
                want_startup_complete: false,
            },
            Case {
                name: "publishing processes leaves the startup flag alone",
                steps: vec![Step::Processes(vec![service(
                    "api",
                    ServiceState::Starting,
                )])],
                want_state: Some(ServiceState::Starting),
                want_startup_complete: false,
            },
            Case {
                name: "setting the startup flag preserves processes",
                steps: vec![
                    Step::Processes(vec![service("api", ServiceState::Ready)]),
                    Step::Startup(true),
                ],
                want_state: Some(ServiceState::Ready),
                want_startup_complete: true,
            },
            Case {
                name: "publishing after settling preserves the startup flag",
                steps: vec![
                    Step::Startup(true),
                    Step::Processes(vec![service("api", ServiceState::Stopped)]),
                ],
                want_state: Some(ServiceState::Stopped),
                want_startup_complete: true,
            },
            Case {
                name: "later publishes win",
                steps: vec![
                    Step::Processes(vec![service("api", ServiceState::Starting)]),
                    Step::Processes(vec![service("api", ServiceState::Ready)]),
                ],
                want_state: Some(ServiceState::Ready),
                want_startup_complete: false,
            },
        ];

        for case in cases {
            let (writer, reader) = channel(StateSnapshot::default());
            for step in case.steps {
                match step {
                    Step::Processes(processes) => writer.publish_processes(processes),
                    Step::Startup(value) => writer.set_startup_complete(value),
                }
            }
            let snapshot = reader.snapshot();
            assert_eq!(
                state_of(&snapshot, "api"),
                case.want_state,
                "{}: service state",
                case.name
            );
            assert_eq!(
                snapshot.startup_complete, case.want_startup_complete,
                "{}: startup_complete",
                case.name
            );
        }
    }

    #[tokio::test]
    async fn readers_see_updates_published_before_they_subscribed() {
        let (writer, _reader) = channel(StateSnapshot::default());
        writer.publish_processes(vec![service("api", ServiceState::Ready)]);
        // A reader created after the fact still starts from current state,
        // which is what makes `snapshot()` safe to call at any point in a
        // component's lifetime.
        let late = writer.reader();
        assert_eq!(state_of(&late.snapshot(), "api"), Some(ServiceState::Ready));
    }

    #[tokio::test]
    async fn writes_land_with_no_readers_alive() {
        // `watch::Sender::send` fails when every receiver is gone *and leaves
        // the value stale*. This must use `send_replace` instead, or a runner
        // that transitions while nothing is watching would publish a snapshot
        // that never catches up.
        let (writer, reader) = channel(StateSnapshot::default());
        drop(reader);
        writer.publish_processes(vec![service("api", ServiceState::Failed)]);
        writer.set_startup_complete(true);

        let fresh = writer.reader();
        let snapshot = fresh.snapshot();
        assert_eq!(state_of(&snapshot, "api"), Some(ServiceState::Failed));
        assert!(snapshot.startup_complete);
    }

    #[tokio::test]
    async fn changed_wakes_on_publish_and_ends_when_the_writer_drops() {
        let (writer, mut reader) = channel(StateSnapshot::default());
        writer.publish_processes(vec![service("api", ServiceState::Starting)]);
        let snapshot = reader.changed().await.expect("a published snapshot");
        assert_eq!(state_of(&snapshot, "api"), Some(ServiceState::Starting));

        drop(writer);
        assert!(reader.changed().await.is_none());
    }

    #[tokio::test]
    async fn wait_for_startup_complete_returns_false_when_the_runner_goes_away() {
        let (writer, mut reader) = channel(StateSnapshot::default());
        let waiter = tokio::spawn(async move { reader.wait_for_startup_complete().await });
        drop(writer);
        assert!(!waiter.await.unwrap());
    }

    #[tokio::test]
    async fn wait_for_startup_complete_returns_immediately_once_settled() {
        let (writer, mut reader) = channel(StateSnapshot::default());
        writer.set_startup_complete(true);
        assert!(reader.wait_for_startup_complete().await);
        // And again — it is a level, not an edge.
        assert!(reader.wait_for_startup_complete().await);
    }
}
