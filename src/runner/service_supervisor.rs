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

/// Everything a service's supervisor can be asked to do.
///
/// Grows as the supervisor absorbs the lifecycle: `Stop` and the
/// process-EOF notice land with handle custody, per the slice-1 protocol
/// in the plan.
pub(in crate::runner) enum ServiceCommand {
    /// Begin a start — or supersede the one being prepared.
    Start(StartRequest),
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

/// Owner half for services.
pub(in crate::runner) type ServiceStarts = super::supervisor::Supervisors<ServiceCommand>;

/// Start one start-supervisor per service.
pub(in crate::runner) fn spawn_supervisors<'a>(
    names: impl Iterator<Item = &'a String>,
    env: &StartEnv,
    outputs: &dyn Fn(&str) -> Option<ItemOutput>,
    internal_tx: &mpsc::Sender<RunnerInternalCommand>,
) -> ServiceStarts {
    ServiceStarts::spawn_all(names, |name, rx, busy| {
        let output = outputs(&name);
        supervise(name, rx, env.clone(), output, internal_tx.clone(), busy)
    })
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
    mut rx: mpsc::UnboundedReceiver<ServiceCommand>,
    env: StartEnv,
    output: Option<ItemOutput>,
    internal_tx: mpsc::Sender<RunnerInternalCommand>,
    busy: Arc<AtomicBool>,
) {
    let service_writer = output.map(|output| output.writer());
    let mut pending: Option<ServiceCommand> = None;
    let mut mailbox_closed = false;

    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => {
                busy.store(false, Ordering::Relaxed);
                match rx.recv().await {
                    Some(command) => {
                        busy.store(true, Ordering::Relaxed);
                        command
                    }
                    None => return,
                }
            }
        };
        let ServiceCommand::Start(StartRequest {
            context,
            mode,
            intent,
        }) = command;

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

        let mut superseded: Option<ServiceCommand> = None;
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
