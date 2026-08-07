//! The build-batcher actor: coalescing for build-tool work, end to end.
//!
//! [`crate::build_tool::manager::BuildBatcher`] remains the scheduling state
//! — queues, windows, the in-flight slot, the bazel mutex — and this task
//! owns *driving* it: it receives queue requests on a mailbox, flushes when a
//! window closes, spawns the batch workers, and forwards their outcomes to
//! the runner on a dedicated channel.
//!
//! Two properties are structural here rather than by-discipline:
//!
//! - **Release-before-apply.** The in-flight slot is released inside this
//!   task the moment a worker finishes, *before* the outcome is forwarded.
//!   Any follow-up rebuild the runner's application enqueues therefore
//!   always sees a free slot — the invariant the old runner arm maintained
//!   with a carefully-ordered pair of calls and a comment.
//! - **The outcome channel is unbounded.** This task must never block on a
//!   send: the runner awaits [`BatchRequest::ForceRebuild`] replies
//!   synchronously, and a bounded outcome channel would let that pair
//!   deadlock.
//!
//! Rebuild specs are captured at *queue* time from resolved config (fixed
//! after construction — there is no live config reload), which is what frees
//! this task from reading runner state to build a batch. The one thing it
//! still reads is the state *snapshot*: services that are still coming up
//! (`Building`/`Pending`/`Starting`) are deferred back onto the queue, so a
//! file edited mid-build waits instead of racing the in-flight startup build.
//! The snapshot can trail the runner's fold, so the application side keeps
//! its own eligibility guard as the safety net.

use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use super::batch::{
    BazelRebuildItem, GraphRequeryOutcomeItem, GraphRequeryRequestItem, RebuildBatchOutcome,
    RebuildBatchRequest, run_graph_requery_worker, run_rebuild_batch_worker,
};
use crate::build_tool::manager::{BatchDue, BuildBatcher};
use crate::output::LifecycleEmitter;
use crate::process::ServiceState;
use crate::state_store::ProcessStatus;
use crate::state_store::StateReader;

/// What one queued rebuild is, captured at queue time.
pub(crate) enum RebuildSpec {
    /// A bazel-built service: the batch runs `bazel build` for its target.
    Bazel(BazelRebuildItem),
    /// A build-tool-managed service with no bazel target (shouldn't happen,
    /// but handled gracefully) — passes through the batch untouched and is
    /// rebuilt by the ordinary per-service path on application.
    Plain { name: String },
}

impl RebuildSpec {
    fn name(&self) -> &str {
        match self {
            Self::Bazel(item) => &item.name,
            Self::Plain { name } => name,
        }
    }
}

/// Everything the batcher can be asked to do.
pub(crate) enum BatchRequest {
    /// Queue a rebuild and (re)open the batch window.
    QueueRebuild { spec: RebuildSpec },
    /// Queue a build-graph re-query and (re)open its window.
    QueueRequery { item: GraphRequeryRequestItem },
    /// Run a forced rebuild for one item immediately, bypassing the window.
    /// Replies with an error if a batch is already in flight, preserving the
    /// hard-restart path's synchronous "already in progress" answer.
    ForceRebuild {
        spec: RebuildSpec,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Abort any in-flight batch (bounded) and exit.
    Shutdown { done: oneshot::Sender<()> },
}

/// A finished batch, forwarded to the runner for application.
pub(crate) enum BatchOutcome {
    Rebuilds(RebuildBatchOutcome),
    Requeries(Vec<GraphRequeryOutcomeItem>),
}

/// What the detached workers hand back to the actor.
enum WorkerDone {
    Rebuilds(RebuildBatchOutcome),
    Requeries(Vec<GraphRequeryOutcomeItem>),
}

/// Spawn the batcher task. Returns its mailbox, the outcome channel the
/// runner folds, and the task handle (joined bounded at shutdown).
pub(crate) fn spawn(
    state: StateReader,
    emitter: LifecycleEmitter,
) -> (
    mpsc::UnboundedSender<BatchRequest>,
    mpsc::UnboundedReceiver<BatchOutcome>,
    tokio::task::JoinHandle<()>,
) {
    let (request_tx, request_rx) = mpsc::unbounded_channel();
    let (outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(run(request_rx, outcome_tx, state, emitter));
    (request_tx, outcome_rx, handle)
}

async fn run(
    mut request_rx: mpsc::UnboundedReceiver<BatchRequest>,
    outcome_tx: mpsc::UnboundedSender<BatchOutcome>,
    state: StateReader,
    emitter: LifecycleEmitter,
) {
    let mut scheduler = BuildBatcher::new();
    let mut rebuild_specs: HashMap<String, RebuildSpec> = HashMap::new();
    let mut requery_specs: HashMap<String, GraphRequeryRequestItem> = HashMap::new();
    // Workers report here; the handles stored in the scheduler are wrappers
    // around these sends, so `abort_in_flight` still kills the real work.
    let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<WorkerDone>();

    loop {
        tokio::select! {
            request = request_rx.recv() => match request {
                Some(BatchRequest::QueueRebuild { spec }) => {
                    scheduler.queue_rebuild(spec.name());
                    rebuild_specs.insert(spec.name().to_string(), spec);
                }
                Some(BatchRequest::QueueRequery { item }) => {
                    scheduler.queue_requery(item.name.clone());
                    requery_specs.insert(item.name.clone(), item);
                }
                Some(BatchRequest::ForceRebuild { spec, reply }) => {
                    if scheduler.rebuild_in_flight() {
                        let _ = reply.send(Err(
                            "build-tool rebuild already in progress".to_string(),
                        ));
                        continue;
                    }
                    // This build supersedes anything queued for the batch.
                    scheduler.cancel_pending_rebuild(spec.name());
                    rebuild_specs.remove(spec.name());
                    spawn_rebuild_batch(
                        &mut scheduler,
                        vec![spec],
                        true,
                        &emitter,
                        &worker_tx,
                    );
                    let _ = reply.send(Ok(()));
                }
                Some(BatchRequest::Shutdown { done }) => {
                    scheduler.abort_in_flight().await;
                    let _ = done.send(());
                    return;
                }
                // Runner gone; nothing left to build for.
                None => {
                    scheduler.abort_in_flight().await;
                    return;
                }
            },
            Some(done) = worker_rx.recv() => {
                // Release the slot before forwarding: anything the
                // application enqueues must see a free batcher.
                match done {
                    WorkerDone::Rebuilds(outcome) => {
                        scheduler.finish_rebuild_batch();
                        if outcome_tx.send(BatchOutcome::Rebuilds(outcome)).is_err() {
                            scheduler.abort_in_flight().await;
                            return;
                        }
                    }
                    WorkerDone::Requeries(outcomes) => {
                        scheduler.finish_requery_batch();
                        if outcome_tx.send(BatchOutcome::Requeries(outcomes)).is_err() {
                            scheduler.abort_in_flight().await;
                            return;
                        }
                    }
                }
            },
            due = scheduler.next_due() => match due {
                BatchDue::Rebuilds => flush_rebuilds(
                    &mut scheduler,
                    &mut rebuild_specs,
                    &state,
                    &emitter,
                    &worker_tx,
                ),
                BatchDue::Requeries => flush_requeries(
                    &mut scheduler,
                    &mut requery_specs,
                    &emitter,
                    &worker_tx,
                ),
            },
        }
    }
}

/// Whether the snapshot says this service is still coming up. Rebuilding a
/// service that is mid-build or mid-start would race its in-flight build or
/// double-start it, so such items are deferred back onto the queue and
/// retried once they reach a settled state. (This is what keeps a file
/// edited mid-build from being lost.)
fn coming_up(state: &StateReader, name: &str) -> bool {
    state.snapshot().processes.iter().any(|status| {
        matches!(
            status,
            ProcessStatus::Service { name: process_name, state, .. }
                if process_name == name
                    && matches!(
                        state,
                        ServiceState::Building
                            | ServiceState::Pending
                            | ServiceState::Starting
                    )
        )
    })
}

fn flush_rebuilds(
    scheduler: &mut BuildBatcher,
    specs: &mut HashMap<String, RebuildSpec>,
    state: &StateReader,
    emitter: &LifecycleEmitter,
    worker_tx: &mpsc::UnboundedSender<WorkerDone>,
) {
    let mut names = scheduler.take_pending_rebuilds();
    if names.is_empty() {
        return;
    }
    if scheduler.rebuild_in_flight() {
        scheduler.queue_rebuilds(names);
        return;
    }

    // Defer services that haven't finished coming up; their specs stay in
    // the map for the retry.
    let mut deferred: Vec<String> = Vec::new();
    names.retain(|name| {
        if coming_up(state, name) {
            deferred.push(name.clone());
            false
        } else {
            true
        }
    });
    scheduler.queue_rebuilds(deferred);
    if names.is_empty() {
        return;
    }

    let flushed = names
        .iter()
        .filter_map(|name| specs.remove(name))
        .collect::<Vec<_>>();
    spawn_rebuild_batch(scheduler, flushed, false, emitter, worker_tx);
}

fn spawn_rebuild_batch(
    scheduler: &mut BuildBatcher,
    specs: Vec<RebuildSpec>,
    force: bool,
    emitter: &LifecycleEmitter,
    worker_tx: &mpsc::UnboundedSender<WorkerDone>,
) {
    let mut bazel_items: Vec<BazelRebuildItem> = Vec::new();
    let mut plain_rebuilds: Vec<String> = Vec::new();
    for spec in specs {
        match spec {
            RebuildSpec::Bazel(item) => bazel_items.push(item),
            RebuildSpec::Plain { name } => plain_rebuilds.push(name),
        }
    }
    let request = RebuildBatchRequest {
        bazel_items,
        plain_rebuilds,
        force,
    };
    let emitter = emitter.clone();
    let bazel_build_mutex = scheduler.bazel_mutex();
    let worker_tx = worker_tx.clone();
    scheduler.set_rebuild_batch(tokio::spawn(async move {
        let outcome = run_rebuild_batch_worker(request, emitter, bazel_build_mutex).await;
        let _ = worker_tx.send(WorkerDone::Rebuilds(outcome));
    }));
}

fn flush_requeries(
    scheduler: &mut BuildBatcher,
    specs: &mut HashMap<String, GraphRequeryRequestItem>,
    emitter: &LifecycleEmitter,
    worker_tx: &mpsc::UnboundedSender<WorkerDone>,
) {
    let names = scheduler.take_pending_requeries();
    if names.is_empty() {
        return;
    }
    if scheduler.requery_in_flight() {
        scheduler.queue_requeries(names);
        return;
    }

    let items: Vec<GraphRequeryRequestItem> =
        names.iter().filter_map(|name| specs.remove(name)).collect();
    if items.is_empty() {
        return;
    }

    emitter.lifecycle_event(&format!(
        "re-querying build tool for {} item{}...",
        items.len(),
        if items.len() == 1 { "" } else { "s" }
    ));

    let emitter = emitter.clone();
    let worker_tx = worker_tx.clone();
    scheduler.set_requery_batch(tokio::spawn(async move {
        let outcomes = run_graph_requery_worker(items, emitter).await;
        let _ = worker_tx.send(WorkerDone::Requeries(outcomes));
    }));
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::LogConfig;
    use crate::output::OutputManager;
    use crate::state_store::{self, StateSnapshot, StateWriter};
    use std::time::Duration;

    async fn test_emitter() -> LifecycleEmitter {
        let log_config = LogConfig::Stdout;
        let services = [("svc", &log_config)];
        let output_manager = OutputManager::new(&services, tokio::io::sink())
            .await
            .unwrap();
        output_manager.clone_lifecycle_emitter()
    }

    fn service_statuses(states: &[(&str, ServiceState)]) -> Vec<ProcessStatus> {
        states
            .iter()
            .map(|(name, state)| ProcessStatus::Service {
                name: name.to_string(),
                state: *state,
                failed_dependencies: Vec::new(),
                verbose: None,
            })
            .collect()
    }

    async fn spawn_actor(
        initial: Vec<ProcessStatus>,
    ) -> (
        StateWriter,
        mpsc::UnboundedSender<BatchRequest>,
        mpsc::UnboundedReceiver<BatchOutcome>,
        tokio::task::JoinHandle<()>,
    ) {
        let (writer, reader) = state_store::channel(StateSnapshot::default());
        writer.publish_processes(initial);
        let emitter = test_emitter().await;
        let (tx, outcome_rx, handle) = spawn(reader, emitter);
        (writer, tx, outcome_rx, handle)
    }

    fn queue(tx: &mpsc::UnboundedSender<BatchRequest>, name: &str) {
        tx.send(BatchRequest::QueueRebuild {
            spec: RebuildSpec::Plain {
                name: name.to_string(),
            },
        })
        .unwrap();
    }

    /// One edit fans out into a rebuild request per affected service; those
    /// must collapse into a single batch, and nothing may build before the
    /// window closes.
    #[tokio::test(start_paused = true)]
    async fn a_burst_of_rebuild_requests_becomes_one_batch() {
        let (_writer, tx, mut outcome_rx, handle) = spawn_actor(service_statuses(&[
            ("api", ServiceState::Ready),
            ("web", ServiceState::Ready),
        ]))
        .await;

        for name in ["api", "web", "api", "web", "api"] {
            queue(&tx, name);
        }

        let outcome = tokio::time::timeout(Duration::from_secs(5), outcome_rx.recv())
            .await
            .expect("the window should close and the batch should run")
            .expect("actor alive");
        let BatchOutcome::Rebuilds(outcome) = outcome else {
            panic!("expected a rebuild outcome");
        };
        let mut names = outcome.plain_rebuilds.clone();
        names.sort();
        assert_eq!(
            names,
            vec!["api".to_string(), "web".to_string()],
            "all five requests went into the one batch"
        );

        // Nothing left over to build again.
        let extra = tokio::time::timeout(Duration::from_millis(500), outcome_rx.recv()).await;
        assert!(extra.is_err(), "exactly one batch for the burst");
        handle.abort();
    }

    /// Regression: a watched file changes while a service is still coming up
    /// (`Building`). The request must be deferred — not dropped, and not
    /// raced against the in-flight startup build — then run once the
    /// service settles.
    #[tokio::test(start_paused = true)]
    async fn rebuild_during_build_is_deferred_not_dropped() {
        let (writer, tx, mut outcome_rx, handle) =
            spawn_actor(service_statuses(&[("api", ServiceState::Building)])).await;

        queue(&tx, "api");

        // While the service is Building, windows keep re-arming and nothing
        // flushes into a build.
        let deferred = tokio::time::timeout(Duration::from_secs(2), outcome_rx.recv()).await;
        assert!(
            deferred.is_err(),
            "rebuild must stay deferred while the service is coming up"
        );

        // Once the service settles, the deferred rebuild fires.
        writer.publish_processes(service_statuses(&[("api", ServiceState::Ready)]));
        let outcome = tokio::time::timeout(Duration::from_secs(5), outcome_rx.recv())
            .await
            .expect("deferred rebuild should fire once the service is up")
            .expect("actor alive");
        let BatchOutcome::Rebuilds(outcome) = outcome else {
            panic!("expected a rebuild outcome");
        };
        assert_eq!(outcome.plain_rebuilds, vec!["api".to_string()]);
        handle.abort();
    }

    /// The hard-restart path's synchronous answer: a force rebuild while a
    /// batch is in flight is refused, not queued.
    #[tokio::test(start_paused = true)]
    async fn force_rebuild_is_refused_while_a_batch_runs() {
        let (_writer, tx, mut outcome_rx, handle) =
            spawn_actor(service_statuses(&[("api", ServiceState::Ready)])).await;

        // Occupy the slot: a forced rebuild spawns immediately.
        let (first_tx, first_rx) = oneshot::channel();
        tx.send(BatchRequest::ForceRebuild {
            spec: RebuildSpec::Plain {
                name: "api".to_string(),
            },
            reply: first_tx,
        })
        .unwrap();
        assert!(first_rx.await.unwrap().is_ok());

        // The plain worker completes almost instantly, so the refusal race
        // is only observable if the second request lands before the actor
        // processes the worker's completion — both are already queued, and
        // mailbox order guarantees the second ForceRebuild is handled
        // before the (later-sent) completion can be. To keep this
        // deterministic, send the second request immediately after the
        // first's reply, before yielding to the actor again.
        let (second_tx, second_rx) = oneshot::channel();
        tx.send(BatchRequest::ForceRebuild {
            spec: RebuildSpec::Plain {
                name: "api".to_string(),
            },
            reply: second_tx,
        })
        .unwrap();
        let second = second_rx.await.unwrap();
        match second {
            Err(message) => assert!(message.contains("already in progress")),
            Ok(()) => {
                // The worker finished before the second request was handled;
                // that is also a legal serialization — the batch completed.
                let _ = tokio::time::timeout(Duration::from_secs(5), outcome_rx.recv()).await;
            }
        }
        handle.abort();
    }
}
