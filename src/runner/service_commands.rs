//! What is left of the scheduler's side of a service command.
//!
//! Nothing here decides anything. Every lifecycle verb is now addressed at the
//! supervisor that owns the process (see [`crate::control`]), and admitted
//! there from the phase it published, the process it holds and the dependency
//! level it computes.
//!
//! Two handlers remain, and both exist for the same reason: a client's reply
//! is a happens-after marker. It rides *down* with the request and back *up*
//! on the report channel, so answering it here means what it has always meant
//! — the projections that outlive individual processes have been updated, and
//! a stop reply implies this service is no longer a satisfied dependency.

use super::{CommandError, CommandResult, Runner, ServiceStartIntent};
use tokio::sync::oneshot;

impl Runner {
    /// A supervisor settled a start: fold the wiring into the cross-process
    /// projections, then answer whoever asked for it.
    pub(in crate::runner) async fn handle_service_start_prepared(
        &mut self,
        name: &str,
        intent: ServiceStartIntent,
        result: Result<Box<super::service_supervisor::ServiceWired>, String>,
    ) {
        if self.shutting_down {
            self.stop_late_service_start(name.to_string(), result).await;
            return;
        }
        match result {
            Ok(wired) => {
                self.handle_service_wired(name, *wired).await;
                if let ServiceStartIntent::Reply { reply } = intent {
                    let _ = reply.send(Ok(()));
                }
            }
            Err(message) => {
                // The phase and the retry decision were the supervisor's and
                // are already published; this is the narration and the reply.
                self.output_manager.service_error_event(name, &message);
                if let ServiceStartIntent::Reply { reply } = intent {
                    let _ = reply.send(Err(CommandError::Failed {
                        name: name.to_string(),
                        message,
                    }));
                }
            }
        }
        self.refresh_runtime_port_manifest();
    }

    /// A supervisor finished executing a stop: end the projections that
    /// outlived the process, then answer whoever asked for it.
    pub(in crate::runner) async fn handle_service_stop_complete(
        &mut self,
        name: &str,
        result: Result<(), String>,
        reply: Option<oneshot::Sender<CommandResult>>,
    ) {
        if !self.services.contains_key(name) {
            return;
        }

        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }
        self.clear_service_custody(name);

        let answer = match result {
            Ok(()) => Ok(()),
            Err(message) => {
                self.output_manager.service_error_event(name, &message);
                Err(CommandError::Failed {
                    name: name.to_string(),
                    message,
                })
            }
        };
        if let Some(reply) = reply {
            let _ = reply.send(answer);
        }
        self.refresh_runtime_port_manifest();
    }
}
