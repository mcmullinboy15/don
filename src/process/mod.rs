//! Everything a service or task does for itself: the supervisors that own
//! processes from prepare to reap, the spawn/stop machinery they drive, the
//! health monitor, ready resolution, and the vocabulary they speak.
//!
//! The edge is deliberate and greppable: **this module imports nothing from
//! `crate::runner`.** Processes produce reports; the runner folds them and
//! decides what starts next. Commands flow down, reports flow up, and the
//! scheduler never reaches into an process's internals.

pub(crate) mod env_refs;
pub(crate) mod health;
pub(crate) mod params;
pub(crate) mod paths;
pub(crate) mod ready;
pub(crate) mod registry;
pub(crate) mod service;
pub(crate) mod service_supervisor;
pub(crate) mod service_worker;
pub mod state;
pub(crate) mod task;
pub(crate) mod task_supervisor;
pub(crate) mod task_worker;

pub use state::{ServiceState, TaskState};

pub(crate) use state::{Demand, ProcessKind, ServiceHandleIdentity};
use tokio::sync::oneshot;

pub(crate) enum ServiceStartIntent {
    /// The dependency sweep asked for this start; the ready outcome drives
    /// the sweep-visible transition.
    Scheduled,
    Reply {
        reply: oneshot::Sender<crate::command::CommandResult>,
    },
    Background,
}

pub(crate) enum TaskRunIntent {
    /// The dependency sweep asked for this run; its exit report drives the
    /// sweep-visible transition.
    Scheduled,
    Background,
}

#[derive(Debug)]
pub(crate) struct TaskExit {
    pub(crate) name: String,
    pub(crate) pgid: i32,
    pub(crate) success: bool,
    pub(crate) message: Option<String>,
    pub(crate) elapsed: Option<std::time::Duration>,
    pub(crate) last_run: Option<crate::task_state::TaskRunInfo>,
    pub(crate) rerun: bool,
}

/// Runner-private messages emitted by detached workers.
/// What an process tells the scheduler, on the lossless report channel.
///
/// This is the up-direction of the supervisor architecture: processes report,
/// the runner folds. It is `mpsc`, not `broadcast`, because the scheduler
/// must never miss one — lossy observation is for peers and edges, which
/// resync from the snapshot. Starts with lazy demand; per-process lifecycle
/// reports (exit, transitions) migrate here as supervisors absorb them.
pub(crate) enum ProcessReport {
    /// A lazy service's proxy saw its first connection. Demand originates
    /// inside the process, but the *reaction* belongs to the scheduler: a lazy
    /// service has dependencies, and starting it is a scheduling decision
    /// like any other.
    Demand { name: String, demand: Demand },
    /// A service's process died and its supervisor reaped it. `status` is
    /// the reaped exit status (`None` when the wait itself failed).
    ServiceExited {
        name: String,
        pgid: i32,
        status: Option<std::process::ExitStatus>,
    },
    /// A service's restart backoff elapsed; attempt `attempt` may begin.
    RestartDue { name: String, attempt: u32 },
    /// The health monitor observed a transition. State-guarded on fold;
    /// the monitor itself dies with custody (its cancel lives in the
    /// supervisor), so it cannot outlive its process by more than a probe.
    HealthChanged { name: String, healthy: bool },
    /// A task process exited after an explicit run/restart.
    TaskExited(TaskExit),
    /// A service's supervisor settled a start request — wired (metadata
    /// only; custody stays with the supervisor) or failed to prepare.
    /// A supervisor spent its start permission and is beginning a start.
    ///
    /// This is the ack that makes a level-triggered permission single-use
    /// across the channel boundary: the runner folds it into `Starting`,
    /// which closes the gate. `epoch` lets the fold ignore an ack for a
    ServiceStarting { name: String },
    ServiceStartPrepared {
        name: String,
        intent: ServiceStartIntent,
        result: Result<Box<service_supervisor::ServiceWired>, String>,
    },
    /// A service's ready check settled — or reported immediately when no
    /// check is configured (`had_check: false`). Forwarded by the
    /// supervisor loop, so it always trails its own prepared report.
    ServiceReady {
        name: String,
        success: bool,
        message: Option<String>,
        had_check: bool,
    },
    /// A service's supervisor finished executing a stop.
    ///
    /// No operation id: a supervisor runs one stop at a time and reports it
    /// before taking the next command, so the Nth completion is the Nth stop
    /// by construction.
    ServiceStopComplete {
        name: String,
        result: Result<(), String>,
        /// The requester's reply, carried down with the stop and back up
        /// here. Answered by the fold rather than the supervisor, because
        /// callers read a stop reply as "the scheduler has applied this" —
        /// `don stop` returning means the service is no longer a satisfied
        /// dependency.
        reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
        /// The supervisor has already begun the start half of a restart; a
        /// [`ProcessReport::ServiceStarting`] from it follows. The fold
        /// applies `Stopped` and leaves the start alone.
        restarting: bool,
    },
    /// A task's supervisor settled a run request.
    TaskRunPrepared {
        name: String,
        task_cfg: Box<crate::config::Task>,
        intent: TaskRunIntent,
        result: Result<task_supervisor::TaskRunReport, String>,
    },
}
