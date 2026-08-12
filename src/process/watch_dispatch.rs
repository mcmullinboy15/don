//! The file watcher, wired to the processes it watches for.
//!
//! The watcher is an edge, like the API and the TUI, and edges address
//! supervisors through a [`ProcessRegistry`]. It used to be the exception:
//! its signals travelled to the scheduler, which looked the name up and
//! forwarded them to the very same mailbox. Nothing was decided on the way
//! through — the scheduler's whole contribution was an error message for a
//! name it did not recognise, which this answers just as well.
//!
//! Build-graph changes already went straight to the build manager. This is
//! the rest of the watcher taking the same route.
//!
//! [`ProcessRegistry`]: super::registry::ProcessRegistry

use super::registry::ProcessRegistry;
use super::{service_supervisor, task_supervisor};
use crate::watch::{WatchDispatch, WatchItemKind};

/// Routes "these files changed" to whoever owns the thing that changed.
pub(crate) struct SupervisorDispatch {
    services: ProcessRegistry<service_supervisor::ServiceCommand>,
    tasks: ProcessRegistry<task_supervisor::TaskCommand>,
    emitter: crate::output::LifecycleEmitter,
}

impl SupervisorDispatch {
    pub(crate) fn new(
        services: ProcessRegistry<service_supervisor::ServiceCommand>,
        tasks: ProcessRegistry<task_supervisor::TaskCommand>,
        emitter: crate::output::LifecycleEmitter,
    ) -> Self {
        Self {
            services,
            tasks,
            emitter,
        }
    }

    /// Tell supervisors their build graph moved.
    ///
    /// Each one asks the build manager to re-query its own patterns, the way
    /// it asks for its own builds — this only says who is affected. The
    /// workspace sentinel affects everyone.
    fn graph_changed(&self, name: &str) -> bool {
        if name != crate::watch::WORKSPACE_GRAPH_ITEM_NAME {
            return self.services.get(name).is_some_and(|handle| {
                handle.request(service_supervisor::ServiceCommand::BuildGraphChanged)
            }) || self.tasks.get(name).is_some_and(|handle| {
                handle.request(task_supervisor::TaskCommand::BuildGraphChanged)
            });
        }
        self.services
            .broadcast(|| service_supervisor::ServiceCommand::BuildGraphChanged)
            | self
                .tasks
                .broadcast(|| task_supervisor::TaskCommand::BuildGraphChanged)
    }
}

impl WatchDispatch for SupervisorDispatch {
    fn changed(&self, name: &str, kind: WatchItemKind) {
        let delivered = match kind {
            WatchItemKind::Service => self.services.get(name).is_some_and(|handle| {
                handle.request(service_supervisor::ServiceCommand::Rebuild(
                    service_supervisor::RebuildRequest {
                        forced: false,
                        // A change that lands while this service is already
                        // rebuilding folds into that cycle rather than
                        // starting a second one. The watcher used to hold
                        // that back itself, which is why it needed to know
                        // when a cycle ended.
                        source: service_supervisor::RebuildSource::FileChange,
                        reply: None,
                    },
                ))
            }),
            WatchItemKind::Task => self
                .tasks
                .get(name)
                .is_some_and(|handle| handle.request(task_supervisor::TaskCommand::Rerun)),
            WatchItemKind::BuildGraph => self.graph_changed(name),
        };
        if !delivered {
            // Either the name is not one we supervise, or teardown has
            // already ended its supervisor. Both are diagnostics, not
            // failures — nothing is waiting on this.
            self.emitter
                .service_debug_event(name, "watch: change had nowhere to go");
        }
    }
}
