//! Everything a service or task does for itself: the supervisors that own
//! processes from prepare to reap, the spawn/stop machinery they drive, the
//! health monitor, ready resolution, and the vocabulary they speak.
//!
//! The edge is deliberate and greppable: **this module imports nothing from
//! `crate::runner`.** Processes produce reports; the runner folds them and
//! decides what starts next. Commands flow down, reports flow up, and the
//! scheduler never reaches into an process's internals.

pub(crate) mod health;
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

pub(crate) use state::{ProcessKind, ServiceHandleIdentity};
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
    Demand { name: String },
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
    ServiceStartPrepared {
        name: String,
        context: Box<service_worker::ServiceStartContext>,
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
    ServiceStopComplete {
        name: String,
        op_id: u64,
        result: Result<(), String>,
    },
    /// A task's supervisor settled a run request.
    TaskRunPrepared {
        name: String,
        task_cfg: Box<crate::config::Task>,
        intent: TaskRunIntent,
        result: Result<task_supervisor::TaskRunReport, String>,
    },
}
