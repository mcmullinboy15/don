//! What a client may ask a process to do.
//!
//! A start, a stop, a restart or a run is a request to a *process*. It used
//! to travel through the runner's command loop because that is where the
//! state behind the pre-checks lived — but the supervisors own their
//! processes now, and the state a pre-check reads is published
//! ([`StateReader`](crate::state_store::StateReader)) or fixed at
//! construction ([`ProcessCatalog`]).
//!
//! So this is the control plane: a cloneable handle the API server holds,
//! which addresses supervisors directly and leaves the runner's mailbox to
//! the things that are genuinely scheduling decisions.
//!
//! # What still goes through the runner
//!
//! The *request* path stops using [`RunnerCommand`]. The *reply* path does
//! not: three integration tests use a reply as a happens-after marker for a
//! runner fold — `socket_test` treats a stop reply as "this is no longer a
//! satisfied dependency", which is only true once the runner has folded
//! `Stopped`. So a client's oneshot rides down to the supervisor and comes
//! back up through the report channel, answered after the fold. That is the
//! shape `ServiceStartIntent::Reply` already proved.

use std::collections::{HashMap, HashSet};

use crate::command::{CommandError, CommandResult};
use crate::runner::RunnerCommand;

/// Every configured process name and what kind it is.
///
/// Fixed at construction, so "is this a service?" and "is this a task?" are
/// answered without waking the scheduler — which is what keeps a 404 for a
/// typo'd name off the runner's critical path.
pub(crate) struct ProcessCatalog {
    /// Every *configured* service and task, active in this profile or not.
    /// Control commands (`start`/`stop`/`restart`) resolve against these.
    configured_services: HashSet<String>,
    configured_tasks: HashSet<String>,
    /// The profile-selected subset the supervisor registries were built from.
    /// `run` resolves against these.
    active_services: HashSet<String>,
    active_tasks: HashSet<String>,
}

impl ProcessCatalog {
    pub(crate) fn new(
        config: &crate::config::Config,
        active_services: HashSet<String>,
        active_tasks: HashSet<String>,
    ) -> Self {
        Self {
            configured_services: config.services.keys().cloned().collect(),
            configured_tasks: config.tasks.keys().cloned().collect(),
            active_services,
            active_tasks,
        }
    }

    /// Resolve a name for a control command.
    ///
    /// Checks *configured* services, then configured tasks — so `don stop` on
    /// a task is a 400 "that's a task", not a 404. A configured service the
    /// active profile excluded resolves fine here and fails later as "not
    /// running", which is a 409; that asymmetry is deliberate and pinned by
    /// `server_test`.
    pub(crate) fn require_service(&self, name: &str) -> Result<(), CommandError> {
        if self.configured_services.contains(name) {
            return Ok(());
        }
        if self.configured_tasks.contains(name) {
            return Err(CommandError::NotAService {
                name: name.to_string(),
            });
        }
        Err(CommandError::UnknownService {
            name: name.to_string(),
        })
    }

    /// Resolve a name for `don run`.
    ///
    /// Checks *active* services then active tasks — a different set and a
    /// different order from [`Self::require_service`]. Both are pinned; do
    /// not unify them.
    pub(crate) fn require_task(&self, name: &str) -> Result<(), CommandError> {
        if self.active_services.contains(name) {
            return Err(CommandError::NotATask {
                name: name.to_string(),
            });
        }
        if self.active_tasks.contains(name) {
            return Ok(());
        }
        Err(CommandError::UnknownTask {
            name: name.to_string(),
        })
    }

    /// Whether `name` is a task, for the polymorphic restart dispatch.
    pub(crate) fn is_task(&self, name: &str) -> bool {
        self.configured_tasks.contains(name)
    }
}

/// The runner is gone and nothing can be asked of it. Maps to 503.
#[derive(Debug)]
pub struct Unavailable;

/// A control request's outcome: the command's own result, or the runner
/// having gone away underneath it.
pub type ControlResult = Result<CommandResult, Unavailable>;

/// Cloneable handle for asking processes to do things.
///
/// Held by the API server. `pub` because `server::serve_api` is, but every
/// field is private — callers get the six verbs and nothing else.
#[derive(Clone)]
pub struct ProcessControl {
    catalog: std::sync::Arc<ProcessCatalog>,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<RunnerCommand>,
}

impl ProcessControl {
    pub(crate) fn new(
        catalog: std::sync::Arc<ProcessCatalog>,
        cmd_tx: tokio::sync::mpsc::UnboundedSender<RunnerCommand>,
    ) -> Self {
        Self { catalog, cmd_tx }
    }

    /// Start a stopped service.
    pub async fn start(&self, name: &str) -> ControlResult {
        if let Err(e) = self.catalog.require_service(name) {
            return Ok(Err(e));
        }
        self.request(|reply| RunnerCommand::Start {
            name: name.to_string(),
            reply,
        })
        .await
    }

    /// Stop a running service.
    pub async fn stop(&self, name: &str) -> ControlResult {
        if let Err(e) = self.catalog.require_service(name) {
            return Ok(Err(e));
        }
        self.request(|reply| RunnerCommand::Stop {
            name: name.to_string(),
            reply,
        })
        .await
    }

    /// Restart a service, or re-run a task with its last parameters.
    pub async fn restart(&self, name: &str) -> ControlResult {
        if !self.catalog.is_task(name)
            && let Err(e) = self.catalog.require_service(name)
        {
            return Ok(Err(e));
        }
        self.request(|reply| RunnerCommand::Restart {
            name: name.to_string(),
            reply,
        })
        .await
    }

    /// Force a rebuild, then restart.
    pub async fn hard_restart(&self, name: &str) -> ControlResult {
        if let Err(e) = self.catalog.require_service(name) {
            return Ok(Err(e));
        }
        self.request(|reply| RunnerCommand::HardRestart {
            name: name.to_string(),
            reply,
        })
        .await
    }

    /// Run a task, optionally waiting for it to finish.
    pub async fn run_task(
        &self,
        name: &str,
        params: HashMap<String, String>,
        wait: bool,
        wait_timeout: Option<String>,
    ) -> ControlResult {
        if let Err(e) = self.catalog.require_task(name) {
            return Ok(Err(e));
        }
        self.request(|reply| RunnerCommand::RunTask {
            name: name.to_string(),
            params,
            wait,
            wait_timeout,
            reply,
        })
        .await
    }

    /// Run every task parked in `PendingRun`.
    pub async fn run_pending(&self) -> ControlResult {
        self.request(|reply| RunnerCommand::RunPendingTasks { reply })
            .await
    }

    /// Begin graceful shutdown. Fire-and-forget: teardown narrates itself.
    pub fn shutdown(&self) -> Result<(), Unavailable> {
        self.cmd_tx
            .send(RunnerCommand::Shutdown)
            .map_err(|_| Unavailable)
    }

    /// A control plane whose runner is already gone, for router tests: every
    /// request answers `Unavailable`, so a 200 proves the response came from
    /// a projection rather than the command channel.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(cmd_rx);
        Self::new(
            std::sync::Arc::new(ProcessCatalog {
                configured_services: HashSet::new(),
                configured_tasks: HashSet::new(),
                active_services: HashSet::new(),
                active_tasks: HashSet::new(),
            }),
            cmd_tx,
        )
    }

    async fn request(
        &self,
        build: impl FnOnce(tokio::sync::oneshot::Sender<CommandResult>) -> RunnerCommand,
    ) -> ControlResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx.send(build(tx)).map_err(|_| Unavailable)?;
        rx.await.map_err(|_| Unavailable)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn catalog() -> ProcessCatalog {
        let config: crate::config::Config = concat!(
            "[services.api]\nrun = { cmd = \"true\" }\n",
            "[services.excluded]\nrun = { cmd = \"true\" }\n",
            "[tasks.setup]\ncmd = \"true\"\n",
        )
        .parse()
        .unwrap();
        // `excluded` is configured but not active — a profile left it out.
        ProcessCatalog::new(
            &config,
            HashSet::from(["api".to_string()]),
            HashSet::from(["setup".to_string()]),
        )
    }

    /// The two lookups differ in both the set they read and the order they
    /// read it in, and each difference is a pinned HTTP status.
    #[test]
    fn control_and_run_resolve_names_differently() {
        struct Case {
            name: &'static str,
            process: &'static str,
            control: Result<(), &'static str>,
            run: Result<(), &'static str>,
        }

        let cases = vec![
            Case {
                name: "an active service",
                process: "api",
                control: Ok(()),
                run: Err("is a service"),
            },
            Case {
                name: "an active task",
                process: "setup",
                control: Err("is a task"),
                run: Ok(()),
            },
            Case {
                name: "a configured service the profile excluded still \
                       resolves for control — it fails later as 'not running'",
                process: "excluded",
                control: Ok(()),
                run: Err("unknown task"),
            },
            Case {
                name: "a name nobody declared",
                process: "ghost",
                control: Err("unknown service"),
                run: Err("unknown task"),
            },
        ];

        let catalog = catalog();
        for case in cases {
            match (catalog.require_service(case.process), case.control) {
                (Ok(()), Ok(())) => {}
                (Err(e), Err(want)) => assert!(
                    e.to_string().contains(want),
                    "{}: control error was {e}",
                    case.name
                ),
                (got, want) => panic!("{}: control {got:?} wanted {want:?}", case.name),
            }
            match (catalog.require_task(case.process), case.run) {
                (Ok(()), Ok(())) => {}
                (Err(e), Err(want)) => assert!(
                    e.to_string().contains(want),
                    "{}: run error was {e}",
                    case.name
                ),
                (got, want) => panic!("{}: run {got:?} wanted {want:?}", case.name),
            }
        }
    }

    #[tokio::test]
    async fn a_dead_runner_is_unavailable_not_a_command_failure() {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(cmd_rx);
        let control = ProcessControl::new(std::sync::Arc::new(catalog()), cmd_tx);
        assert!(
            control.start("api").await.is_err(),
            "a real request needs the runner"
        );
        // …but a bad name is answered locally, and as the command's own
        // error rather than as the runner being unavailable — otherwise a
        // typo would report 503 instead of 404.
        let answered = control
            .start("ghost")
            .await
            .expect("a name check must not need the runner");
        assert!(
            answered
                .unwrap_err()
                .to_string()
                .contains("unknown service"),
            "a typo must answer as the command's error, not as 503"
        );
    }
}
