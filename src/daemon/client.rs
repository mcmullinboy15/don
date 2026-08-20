//! Client for the daemon's control socket.
//!
//! Two very different callers use this:
//!
//! - `don start`, to announce itself and to say goodbye on shutdown. Those
//!   calls are best-effort by design — see [`register_best_effort`].
//! - `don daemon status|stop`, where the user asked a direct question and a
//!   missing daemon is a real answer worth printing.
//!
//! The transport is the same plain HTTP-over-unix-socket the project API
//! uses, reused from [`crate::client::unix_request`].

use super::registry::ProjectEntry;
use crate::client::ClientError;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Ceiling on any single control request.
///
/// `don start` sits behind these calls, so an unresponsive daemon must not be
/// able to stall a stack coming up. A local unix socket answers in
/// microseconds; anything approaching a second means something is wrong and
/// we would rather move on without the UI than make the user wait for it.
const REQUEST_TIMEOUT: Duration = Duration::from_millis(750);

/// Body of `GET /projects`.
#[derive(Debug, Deserialize)]
pub struct ProjectsResponse {
    pub projects: Vec<ProjectEntry>,
}

/// Body of `GET /info`.
#[derive(Debug, Deserialize)]
pub struct DaemonInfo {
    /// Version of the `don` binary running the daemon.
    pub version: String,
    /// PID of the daemon process.
    pub pid: u32,
    /// Address the web UI is bound to, e.g. `127.0.0.1:3666`. `None` when the
    /// daemon is running without a web server.
    pub web_addr: Option<String>,
    /// Number of projects currently registered.
    pub projects: usize,
}

/// Client for the daemon control socket.
pub struct DaemonClient {
    socket_path: PathBuf,
}

impl DaemonClient {
    /// Point a client at an explicit socket path.
    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    /// The socket this client talks to.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// `GET /info` — daemon version, pid, and web address.
    pub async fn info(&self) -> Result<DaemonInfo, ClientError> {
        let body = self.request("GET", "/info", None).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// `GET /projects` — every live project, pruned of dead entries.
    pub async fn projects(&self) -> Result<Vec<ProjectEntry>, ClientError> {
        let body = self.request("GET", "/projects", None).await?;
        let parsed: ProjectsResponse = serde_json::from_slice(&body)?;
        Ok(parsed.projects)
    }

    /// `POST /projects` — announce a running project.
    pub async fn register(&self, entry: &ProjectEntry) -> Result<(), ClientError> {
        let payload = serde_json::to_vec(entry)?;
        self.request("POST", "/projects", Some(&payload)).await?;
        Ok(())
    }

    /// `DELETE /projects/:id` — withdraw a project.
    pub async fn deregister(&self, id: &str) -> Result<(), ClientError> {
        let path = format!("/projects/{}", crate::client::urlencode(id));
        self.request("DELETE", &path, None).await?;
        Ok(())
    }

    /// `POST /shutdown` — ask the daemon to exit.
    ///
    /// Registered projects are untouched: the daemon doesn't own them, so
    /// stopping it only takes the UI away.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        self.request("POST", "/shutdown", None).await?;
        Ok(())
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Vec<u8>, ClientError> {
        let call = crate::client::unix_request(&self.socket_path, method, path, body);
        let (status, response) = match tokio::time::timeout(REQUEST_TIMEOUT, call).await {
            Ok(result) => result?,
            Err(_) => {
                return Err(ClientError::Invalid(format!(
                    "don daemon did not respond within {}ms",
                    REQUEST_TIMEOUT.as_millis()
                )));
            }
        };
        if (200..300).contains(&status) {
            Ok(response)
        } else {
            Err(crate::client::classify_error(status, &response))
        }
    }
}

/// Announce a project to the daemon, swallowing every failure.
///
/// Registration is decoration, not function: a project whose daemon is absent
/// (the common case — most people won't install one) must start exactly as
/// fast and exactly as reliably as it does today. Returns a message worth
/// logging at debug level, or `None` when there was nothing to say.
pub async fn register_best_effort(socket_path: PathBuf, entry: ProjectEntry) -> Option<String> {
    let client = DaemonClient::new(socket_path);
    match client.register(&entry).await {
        Ok(()) => Some(format!("registered with don daemon as '{}'", entry.name)),
        // Nothing listening is the expected state, not a problem to report.
        Err(ClientError::NotRunning { .. }) => None,
        Err(e) => Some(format!("could not register with don daemon: {e}")),
    }
}

/// Withdraw a project from the daemon, swallowing every failure.
///
/// Called on the shutdown path, where the user is waiting on Ctrl+C. A
/// daemon that has itself gone away, or is slow, must not add latency —
/// hence the same short timeout as registration and no error propagation.
/// A missed deregistration is harmless: the daemon prunes unreachable
/// projects the next time anything reads the registry.
pub async fn deregister_best_effort(socket_path: PathBuf, id: String) {
    let _ = DaemonClient::new(socket_path).deregister(&id).await;
}
