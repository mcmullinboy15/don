//! Per-service start supervision.
//!
//! The mirror of [`super::task_supervisor`] for the other item kind, and it
//! exists for the same reason: preparing a start is slow (downloads, builds,
//! docker pulls, port allocation) so it has always been detached, and
//! detaching onto a shared completion channel is what forced the runner to
//! ask "is this still the current start?" when the answer landed —
//! `start_generation`, compared on arrival, plus a branch to stop whatever
//! the losing attempt had already brought up.
//!
//! One supervisor per service removes the question. It is the only thing that
//! reports a prepared start for its service, and only for the start it is
//! committed to.
//!
//! Note this owns *preparation only*. Wiring the process up, running the
//! ready check and sequencing shutdown all stay on the runner, because they
//! read cross-item state (dependency order) or are read back from many places
//! — see the task-side measurement in the plan for why moving them is a net
//! loss rather than the obvious next step.

use super::RunnerInternalCommand;
use super::service;
use super::service_worker::{ServiceStartMode, start_service_worker};
use super::{ServiceStartContext, ServiceStartIntent};
use crate::config::ShutdownConfig;
use crate::output::{ItemOutput, LifecycleEmitter};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// One request to start a service, as handed to its supervisor.
pub(in crate::runner) struct StartRequest {
    pub(in crate::runner) context: Box<ServiceStartContext>,
    pub(in crate::runner) mode: ServiceStartMode,
    pub(in crate::runner) intent: ServiceStartIntent,
}

/// Everything a supervisor needs that doesn't vary per request.
#[derive(Clone)]
pub(in crate::runner) struct StartEnv {
    pub(in crate::runner) base_dir: PathBuf,
    pub(in crate::runner) pid_dir: PathBuf,
    pub(in crate::runner) platform: crate::config::Platform,
    pub(in crate::runner) docker_client: Option<bollard::Docker>,
    pub(in crate::runner) emitter: LifecycleEmitter,
    /// Global shutdown defaults, for stopping a start that lost a race.
    pub(in crate::runner) shutdown: ShutdownConfig,
}

/// Send-only handle to one service's start supervisor.
///
/// The addressing half, exactly as [`TaskHandle`] is for tasks: it can ask a
/// service to start and cannot create or destroy the supervisor.
///
/// [`TaskHandle`]: super::task_supervisor::TaskHandle
#[derive(Clone)]
pub(in crate::runner) struct StartHandle {
    tx: mpsc::UnboundedSender<StartRequest>,
    busy: Arc<AtomicBool>,
}

impl StartHandle {
    /// Queue a start. Fails only once the supervisor is gone (shutdown).
    pub(in crate::runner) fn request(&self, request: StartRequest) -> bool {
        self.busy.store(true, Ordering::Relaxed);
        let sent = self.tx.send(request).is_ok();
        if !sent {
            self.busy.store(false, Ordering::Relaxed);
        }
        sent
    }

    /// Whether a start is queued or being prepared.
    pub(in crate::runner) fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Relaxed)
    }
}

/// Every service's start mailbox, addressable by name.
///
/// Lock-free for the same reason [`TaskRegistry`] is: the item set is fixed
/// at construction, so there is no insert and no remove to synchronise.
///
/// [`TaskRegistry`]: super::task_supervisor::TaskRegistry
#[derive(Clone)]
pub(in crate::runner) struct StartRegistry {
    handles: Arc<HashMap<String, StartHandle>>,
}

impl StartRegistry {
    pub(in crate::runner) fn get(&self, name: &str) -> Option<&StartHandle> {
        self.handles.get(name)
    }

    /// Whether `name` has a start queued or being prepared. `false` for an
    /// unknown name.
    pub(in crate::runner) fn is_busy(&self, name: &str) -> bool {
        self.get(name).is_some_and(StartHandle::is_busy)
    }

    /// Names with a start queued or being prepared.
    pub(in crate::runner) fn busy_names(&self) -> impl Iterator<Item = &str> {
        self.handles
            .iter()
            .filter(|(_, handle)| handle.is_busy())
            .map(|(name, _)| name.as_str())
    }
}

/// The owner half: the supervisor tasks themselves.
pub(in crate::runner) struct ServiceStarts {
    registry: StartRegistry,
    joins: Vec<(String, tokio::task::JoinHandle<()>)>,
}

impl ServiceStarts {
    /// Start one supervisor per service. Eager, so the registry is immutable.
    pub(in crate::runner) fn spawn_all<'a>(
        names: impl Iterator<Item = &'a String>,
        env: &StartEnv,
        outputs: &dyn Fn(&str) -> Option<ItemOutput>,
        internal_tx: &mpsc::Sender<RunnerInternalCommand>,
    ) -> Self {
        let mut handles = HashMap::new();
        let mut joins = Vec::new();
        for name in names {
            let (tx, rx) = mpsc::unbounded_channel();
            let busy = Arc::new(AtomicBool::new(false));
            let join = tokio::spawn(supervise(
                name.clone(),
                rx,
                env.clone(),
                outputs(name),
                internal_tx.clone(),
                Arc::clone(&busy),
            ));
            handles.insert(name.clone(), StartHandle { tx, busy });
            joins.push((name.clone(), join));
        }
        Self {
            registry: StartRegistry {
                handles: Arc::new(handles),
            },
            joins,
        }
    }

    pub(in crate::runner) fn registry(&self) -> &StartRegistry {
        &self.registry
    }

    /// Cancel every supervisor, returning the handles to await.
    ///
    /// Returns rather than awaits so shutdown can fire all the aborts before
    /// waiting on any — see `TaskSupervisors::abort_all`.
    pub(in crate::runner) fn abort_all(&mut self) -> Vec<(String, tokio::task::JoinHandle<()>)> {
        let joins = std::mem::take(&mut self.joins);
        for (_, join) in &joins {
            join.abort();
        }
        joins
    }
}

/// Stop a service brought up by a start that has since been superseded.
///
/// The losing attempt may already have a live process; nothing else knows
/// about it, so it has to be stopped here. Detached, because the caller is
/// the supervisor loop and the next start shouldn't wait on the old one's
/// shutdown grace period.
fn stop_superseded_start(
    env: &StartEnv,
    name: &str,
    context: &ServiceStartContext,
    start_result: service::StartResult,
) {
    let shutdown_config = context
        .resolved
        .shutdown
        .clone()
        .map(|shutdown| shutdown.merged_over(&env.shutdown))
        .unwrap_or_else(|| env.shutdown.clone());
    let debug = service::StopDebug::new(name.to_string(), env.emitter.clone());
    tokio::spawn(async move {
        let service::StartResult {
            handle,
            child_output,
        } = start_result;
        // Nothing will consume this, and holding it open keeps the child's
        // pipe alive.
        drop(child_output);
        let _ =
            service::stop_service(handle, Some(&shutdown_config), true, false, Some(debug)).await;
    });
}

/// Drive one service's starts, strictly in order.
///
/// Same rule as the task supervisor: a superseded start is **finished, not
/// aborted**. `start_service_worker` may already have a process up by the
/// time a newer request arrives, and dropping that future would strand it.
async fn supervise(
    name: String,
    mut rx: mpsc::UnboundedReceiver<StartRequest>,
    env: StartEnv,
    output: Option<ItemOutput>,
    internal_tx: mpsc::Sender<RunnerInternalCommand>,
    busy: Arc<AtomicBool>,
) {
    let service_writer = output.map(|output| output.writer());
    let mut pending: Option<StartRequest> = None;
    let mut mailbox_closed = false;

    loop {
        let request = match pending.take() {
            Some(request) => request,
            None => {
                busy.store(false, Ordering::Relaxed);
                match rx.recv().await {
                    Some(request) => {
                        busy.store(true, Ordering::Relaxed);
                        request
                    }
                    None => return,
                }
            }
        };
        let StartRequest {
            context,
            mode,
            intent,
        } = request;

        // Clone the context the worker borrows so the original can move into
        // the completion message afterwards.
        let context_for_worker = context.clone();
        let worker = start_service_worker(
            &env.base_dir,
            &env.pid_dir,
            env.platform,
            env.docker_client.as_ref(),
            &env.emitter,
            &name,
            context_for_worker.as_ref(),
            mode,
            service_writer.as_ref(),
        );
        tokio::pin!(worker);

        let mut superseded: Option<StartRequest> = None;
        let result = loop {
            tokio::select! {
                result = &mut worker => break result,
                next = rx.recv(), if !mailbox_closed => match next {
                    Some(next) => superseded = Some(next),
                    // Guarded so a closed mailbox doesn't spin this select.
                    None => mailbox_closed = true,
                },
            }
        };

        match superseded {
            Some(next) => {
                if let Ok(start_result) = result {
                    stop_superseded_start(&env, &name, context.as_ref(), start_result);
                }
                pending = Some(next);
            }
            None => {
                let sent = internal_tx
                    .send(RunnerInternalCommand::ServiceStartPrepared {
                        name: name.clone(),
                        context,
                        intent,
                        result: result.map(Box::new),
                    })
                    .await;
                if sent.is_err() {
                    return;
                }
            }
        }
    }
}
