//! TCP proxy — Don listens on configured addresses and forwards connections
//! to services on random ephemeral ports.
//!
//! Each service with `proxy` entries gets a [`ServiceProxy`] that outlives
//! individual service restarts. The proxy uses a `watch` channel to track
//! the current backend address, enabling atomic zero-downtime switches.

use std::collections::HashMap;
use std::net::SocketAddr;

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::config::ProxyEntry;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("failed to bind proxy listener on '{addr}': {source}")]
    Bind {
        addr: String,
        source: std::io::Error,
    },
    #[error("failed to allocate ephemeral port: {0}")]
    EphemeralPort(std::io::Error),
}

/// A single proxy listener that forwards connections to a backend.
struct ProxyListener {
    /// The address Don is listening on (from config).
    listen_addr: SocketAddr,
    /// Sender to update the backend address when a service restarts.
    backend_tx: watch::Sender<Option<SocketAddr>>,
    /// Handle to the accept loop task.
    accept_handle: JoinHandle<()>,
}

/// Tracks an ephemeral port allocated for a proxy entry.
#[derive(Debug, Clone)]
pub(crate) struct EphemeralPort {
    /// The ephemeral address (127.0.0.1:<random>).
    pub addr: SocketAddr,
    /// The env var name, or `None` for LISTEN_FDS mode.
    pub env: Option<String>,
}

/// A set of proxy listeners for a single service.
pub(crate) struct ServiceProxy {
    listeners: Vec<ProxyListener>,
    ephemeral_ports: Vec<EphemeralPort>,
}

impl ServiceProxy {
    /// Bind proxy listeners for a service's proxy entries.
    ///
    /// For each entry, binds a public listener on `entry.listen` and allocates
    /// an ephemeral port for the backend. If `lazy_tx` is provided, the first
    /// incoming connection on any listener will send the service name on it.
    pub(crate) async fn bind(
        entries: &[ProxyEntry],
        lazy_tx: Option<mpsc::Sender<String>>,
        service_name: &str,
    ) -> Result<Self, ProxyError> {
        let mut listeners = Vec::with_capacity(entries.len());
        let mut ephemeral_ports = Vec::with_capacity(entries.len());

        for entry in entries {
            let listen_addr: SocketAddr = entry
                .listen
                .parse()
                .map_err(|e| ProxyError::Bind {
                    addr: entry.listen.clone(),
                    source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                })?;

            // Bind the public listener.
            let listener = TcpListener::bind(listen_addr)
                .await
                .map_err(|e| ProxyError::Bind {
                    addr: entry.listen.clone(),
                    source: e,
                })?;

            // Allocate an ephemeral port by binding to port 0.
            let ephemeral_addr = allocate_ephemeral_port().await?;

            ephemeral_ports.push(EphemeralPort {
                addr: ephemeral_addr,
                env: entry.env.clone(),
            });

            // Create the backend watch channel (starts with no backend).
            let (backend_tx, backend_rx) = watch::channel(None);

            // Spawn the accept loop.
            let accept_handle = tokio::spawn(proxy_accept_loop(
                listener,
                backend_rx,
                lazy_tx.clone(),
                service_name.to_string(),
            ));

            listeners.push(ProxyListener {
                listen_addr,
                backend_tx,
                accept_handle,
            });
        }

        Ok(ServiceProxy {
            listeners,
            ephemeral_ports,
        })
    }

    /// Update the backend addresses so new connections route to the new instance.
    ///
    /// Each listener's backend is updated to the corresponding ephemeral address.
    pub(crate) fn set_backend(&self) {
        for (listener, eph) in self.listeners.iter().zip(self.ephemeral_ports.iter()) {
            let _ = listener.backend_tx.send(Some(eph.addr));
        }
    }

    /// Clear all backend addresses. New connections will queue until a backend
    /// is set again.
    pub(crate) fn clear_backend(&self) {
        for listener in &self.listeners {
            let _ = listener.backend_tx.send(None);
        }
    }

    /// Allocate new ephemeral ports for a restart. Returns the old ports.
    pub(crate) async fn reallocate_ephemeral_ports(&mut self) -> Result<Vec<EphemeralPort>, ProxyError> {
        let old = self.ephemeral_ports.clone();
        for eph in &mut self.ephemeral_ports {
            eph.addr = allocate_ephemeral_port().await?;
        }
        Ok(old)
    }

    /// Return env vars for proxy entries that use env var mode.
    /// E.g. `{"PORT": "49152"}`.
    pub(crate) fn env_vars(&self) -> HashMap<String, String> {
        let mut vars = HashMap::new();
        for eph in &self.ephemeral_ports {
            if let Some(ref env_name) = eph.env {
                vars.insert(env_name.clone(), eph.addr.port().to_string());
            }
        }
        vars
    }

    /// Return ephemeral addresses for proxy entries that use LISTEN_FDS mode.
    pub(crate) fn listen_fds_addrs(&self) -> Vec<String> {
        self.ephemeral_ports
            .iter()
            .filter(|eph| eph.env.is_none())
            .map(|eph| eph.addr.to_string())
            .collect()
    }

    /// Create a handle that can be sent to a spawned task to set the backend
    /// once a ready check passes. The handle is `Send + Sync`.
    pub(crate) fn backend_handle(&self) -> ProxyBackendHandle {
        let pairs: Vec<_> = self
            .listeners
            .iter()
            .zip(self.ephemeral_ports.iter())
            .map(|(l, e)| (l.backend_tx.clone(), e.addr))
            .collect();
        ProxyBackendHandle { pairs }
    }

    /// Addresses Don is listening on (for display / logging).
    pub(crate) fn listen_addrs(&self) -> Vec<SocketAddr> {
        self.listeners.iter().map(|l| l.listen_addr).collect()
    }

    /// Shut down all proxy listeners and abort accept loops.
    pub(crate) fn shutdown(&self) {
        for listener in &self.listeners {
            listener.accept_handle.abort();
        }
    }
}

/// A lightweight, `Send + Sync` handle for activating proxy backends from
/// a spawned task (e.g. after a ready check passes on the rebuild path).
pub(crate) struct ProxyBackendHandle {
    pairs: Vec<(watch::Sender<Option<SocketAddr>>, SocketAddr)>,
}

impl ProxyBackendHandle {
    /// Set all backends to their ephemeral addresses.
    pub(crate) fn activate(&self) {
        for (tx, addr) in &self.pairs {
            let _ = tx.send(Some(*addr));
        }
    }
}

impl Drop for ServiceProxy {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Allocate an ephemeral port by binding to port 0, reading the assigned port,
/// then dropping the listener. There is a tiny TOCTOU window, but acceptable
/// for local dev tooling.
async fn allocate_ephemeral_port() -> Result<SocketAddr, ProxyError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(ProxyError::EphemeralPort)?;
    let addr = listener
        .local_addr()
        .map_err(ProxyError::EphemeralPort)?;
    drop(listener);
    Ok(addr)
}

/// Accept loop for a single proxy listener.
///
/// Accepts TCP connections, optionally triggers lazy start on the first one,
/// waits for a backend to be available, then spawns per-connection forwarding.
async fn proxy_accept_loop(
    listener: TcpListener,
    backend_rx: watch::Receiver<Option<SocketAddr>>,
    lazy_tx: Option<mpsc::Sender<String>>,
    service_name: String,
) {
    let mut consecutive_errors: u32 = 0;
    loop {
        let (client, _peer) = match listener.accept().await {
            Ok(conn) => {
                consecutive_errors = 0;
                conn
            }
            Err(e) => {
                consecutive_errors += 1;
                // Back off on repeated errors to avoid busy-spinning.
                // First few errors get a short delay; persistent errors
                // get longer pauses.
                let delay = std::time::Duration::from_millis(
                    (10 * consecutive_errors.min(100)) as u64,
                );
                eprintln!(
                    "[don] proxy {}: accept error: {e} (backoff {delay:?})",
                    listener.local_addr().map(|a| a.to_string()).unwrap_or_default(),
                );
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        // Trigger lazy start if configured. The runner checks service state
        // before acting, so duplicate sends are harmless. We use try_send to
        // avoid blocking — if the channel is full, a trigger is already queued.
        if let Some(ref tx) = lazy_tx {
            let _ = tx.try_send(service_name.clone());
        }

        // Wait for a backend address to become available.
        let backend_addr = {
            let mut rx = backend_rx.clone();
            loop {
                if let Some(addr) = *rx.borrow() {
                    break addr;
                }
                if rx.changed().await.is_err() {
                    // Channel closed — shutting down.
                    return;
                }
            }
        };

        // Spawn a forwarding task for this connection.
        tokio::spawn(proxy_connection(client, backend_addr));
    }
}

/// Forward traffic bidirectionally between client and backend.
///
/// Retries the backend connection with exponential backoff if the service
/// isn't listening yet (common during startup before the process binds its port).
async fn proxy_connection(mut client: TcpStream, backend_addr: SocketAddr) {
    let mut backend = None;
    for attempt in 0..20u32 {
        match TcpStream::connect(backend_addr).await {
            Ok(stream) => {
                backend = Some(stream);
                break;
            }
            Err(_) => {
                // Exponential backoff: 10ms, 20ms, 40ms, ... capped at 500ms.
                let delay = std::time::Duration::from_millis(
                    (10 * (1 << attempt.min(6))).min(500),
                );
                tokio::time::sleep(delay).await;
            }
        }
    }

    let Some(mut backend) = backend else {
        // Backend never became reachable — close client connection.
        let _ = client.shutdown().await;
        return;
    };

    // Shovel bytes in both directions until either side closes.
    let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
}
