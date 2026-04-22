//! TCP proxy — Don binds each configured `proxy` address and either forwards
//! to an ephemeral backend port (env mode) or hands the bound listener to
//! the service via `LISTEN_FDS` (listenfd mode).
//!
//! Each service with `proxy` entries gets a [`ServiceProxy`] that outlives
//! individual service restarts. For env entries, the proxy uses a `watch`
//! channel to track the current backend address, enabling atomic zero-
//! downtime switches. For listenfd entries, the bound listener's fd is
//! passed to the child — no forwarding at the don layer.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::io::unix::AsyncFd;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::config::{ProxyEntry, ProxyMode};

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

/// A forwarding listener: don accepts on the public address and shuttles
/// bytes to/from an ephemeral backend port that the service binds.
struct ForwardListener {
    listen_addr: SocketAddr,
    env_name: String,
    ephemeral_addr: SocketAddr,
    backend_tx: watch::Sender<Option<SocketAddr>>,
    accept_handle: JoinHandle<()>,
}

/// A listenfd listener: don holds the bound public listener and passes its
/// fd to the child. If `lazy_watcher` is Some, don is watching for POLLIN
/// so the first queued connection triggers a lazy start.
struct ListenfdListener {
    listen_addr: SocketAddr,
    /// `std::net::TcpListener` (not tokio's) because we need `AsRawFd` and
    /// stable fd semantics for passing into the child via `LISTEN_FDS`.
    /// Wrapped in an `Arc` so the POLLIN watcher can hold its own handle
    /// that survives across re-arms.
    listener: std::sync::Arc<std::net::TcpListener>,
    lazy_watcher: Option<JoinHandle<()>>,
}

/// A set of proxy listeners for a single service.
pub(crate) struct ServiceProxy {
    forward: Vec<ForwardListener>,
    listenfd: Vec<ListenfdListener>,
    service_name: String,
    lazy_tx: Option<mpsc::Sender<String>>,
}

impl ServiceProxy {
    /// Bind proxy listeners for a service's proxy entries.
    ///
    /// Env-mode entries bind a public listener, allocate an ephemeral backend
    /// port, and spawn a forwarding accept loop. Listenfd-mode entries bind
    /// the public listener and stop there; if `lazy_tx` is provided, a POLLIN
    /// watcher fires the lazy trigger on the first queued connection.
    pub(crate) async fn bind(
        entries: &[ProxyEntry],
        lazy_tx: Option<mpsc::Sender<String>>,
        service_name: &str,
        emitter: crate::output::LifecycleEmitter,
    ) -> Result<Self, ProxyError> {
        let mut forward = Vec::new();
        let mut listenfd = Vec::new();

        for entry in entries {
            let listen_addr: SocketAddr = entry
                .listen
                .parse()
                .map_err(|e| ProxyError::Bind {
                    addr: entry.listen.clone(),
                    source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                })?;

            match &entry.mode {
                ProxyMode::Env(env_name) => {
                    let listener = TcpListener::bind(listen_addr)
                        .await
                        .map_err(|e| ProxyError::Bind {
                            addr: entry.listen.clone(),
                            source: e,
                        })?;
                    let ephemeral_addr = allocate_ephemeral_port().await?;
                    let (backend_tx, backend_rx) = watch::channel(None);
                    let accept_handle = tokio::spawn(proxy_accept_loop(
                        listener,
                        backend_rx,
                        lazy_tx.clone(),
                        service_name.to_string(),
                        emitter.clone(),
                    ));
                    forward.push(ForwardListener {
                        listen_addr,
                        env_name: env_name.clone(),
                        ephemeral_addr,
                        backend_tx,
                        accept_handle,
                    });
                }
                ProxyMode::Listenfd => {
                    // `std::net::TcpListener::bind` is blocking — fine at
                    // startup, and gives us stable fd semantics for passing
                    // into the child. We deliberately do NOT flip the fd to
                    // non-blocking: `O_NONBLOCK` lives on the open file
                    // description and is shared across `dup`/`dup2`, so
                    // setting it here would also flip the child's fd 3,
                    // breaking a typical blocking `accept()` in the service.
                    // `AsyncFd::readable()` only needs readiness
                    // notifications, not non-blocking I/O — it never calls
                    // `accept` on this fd.
                    let listener = std::net::TcpListener::bind(listen_addr)
                        .map_err(|e| ProxyError::Bind {
                            addr: entry.listen.clone(),
                            source: e,
                        })?;
                    let listener = std::sync::Arc::new(listener);
                    let lazy_watcher = lazy_tx.as_ref().map(|tx| {
                        spawn_listenfd_watcher(
                            listener.clone(),
                            tx.clone(),
                            service_name.to_string(),
                        )
                    });
                    listenfd.push(ListenfdListener {
                        listen_addr,
                        listener,
                        lazy_watcher,
                    });
                }
            }
        }

        Ok(ServiceProxy {
            forward,
            listenfd,
            service_name: service_name.to_string(),
            lazy_tx,
        })
    }

    /// Update env-mode backend addresses so new connections route to the new
    /// instance. Listenfd entries are unaffected — the child owns the fd.
    pub(crate) fn set_backend(&self) {
        for fwd in &self.forward {
            let _ = fwd.backend_tx.send(Some(fwd.ephemeral_addr));
        }
    }

    /// Clear all env-mode backend addresses. New connections queue until a
    /// backend is set again.
    pub(crate) fn clear_backend(&self) {
        for fwd in &self.forward {
            let _ = fwd.backend_tx.send(None);
        }
    }

    /// Allocate new ephemeral ports for env-mode entries. Used on restart so
    /// the old port is gone before the new process tries to bind it. Returns
    /// `()` — no caller consumes the old addresses.
    pub(crate) async fn reallocate_ephemeral_ports(&mut self) -> Result<(), ProxyError> {
        for fwd in &mut self.forward {
            fwd.ephemeral_addr = allocate_ephemeral_port().await?;
        }
        Ok(())
    }

    /// Env var map for env-mode entries, e.g. `{"PORT": "49152"}`.
    pub(crate) fn env_vars(&self) -> HashMap<String, String> {
        let mut vars = HashMap::new();
        for fwd in &self.forward {
            vars.insert(fwd.env_name.clone(), fwd.ephemeral_addr.port().to_string());
        }
        vars
    }

    /// `LISTEN_FDS` / `LISTEN_FDNAMES` env vars for listenfd entries. Empty
    /// if the service has no listenfd proxy entries. `LISTEN_PID` is set by
    /// the shell shim at spawn time — see `process::mod::listen_pid_shim`.
    pub(crate) fn listenfd_env(&self) -> HashMap<String, String> {
        let mut env = HashMap::new();
        if self.listenfd.is_empty() {
            return env;
        }
        env.insert("LISTEN_FDS".to_string(), self.listenfd.len().to_string());
        let names: Vec<String> = self
            .listenfd
            .iter()
            .map(|l| l.listen_addr.to_string())
            .collect();
        env.insert("LISTEN_FDNAMES".to_string(), names.join(":"));
        env
    }

    /// Raw fds of the listenfd listeners, in declaration order. Each fd is a
    /// bound listening socket that the child will see at fd 3, 4, ….
    pub(crate) fn listenfd_raw_fds(&self) -> Vec<RawFd> {
        self.listenfd.iter().map(|l| l.listener.as_raw_fd()).collect()
    }

    /// Create a handle that can be sent to a spawned task to set env-mode
    /// backends once a ready check passes. The handle is `Send + Sync`.
    pub(crate) fn backend_handle(&self) -> ProxyBackendHandle {
        let pairs: Vec<_> = self
            .forward
            .iter()
            .map(|fwd| (fwd.backend_tx.clone(), fwd.ephemeral_addr))
            .collect();
        ProxyBackendHandle { pairs }
    }

    /// Addresses Don is listening on (for display / logging). Includes both
    /// modes in declaration order across each kind.
    pub(crate) fn listen_addrs(&self) -> Vec<SocketAddr> {
        let mut out: Vec<SocketAddr> = self.forward.iter().map(|f| f.listen_addr).collect();
        out.extend(self.listenfd.iter().map(|l| l.listen_addr));
        out
    }

    /// Re-arm lazy POLLIN watchers for listenfd entries. Called after the
    /// service stops and re-enters the `Lazy` state so the next queued
    /// connection triggers another start cycle. No-op if the service isn't
    /// lazy (no `lazy_tx`) or if a watcher is already armed.
    pub(crate) fn rearm_lazy_watchers(&mut self) {
        let Some(tx) = self.lazy_tx.clone() else {
            return;
        };
        for l in &mut self.listenfd {
            if l.lazy_watcher.as_ref().is_some_and(|h| !h.is_finished()) {
                continue;
            }
            l.lazy_watcher = Some(spawn_listenfd_watcher(
                l.listener.clone(),
                tx.clone(),
                self.service_name.clone(),
            ));
        }
    }

    /// Shut down all proxy work — abort forwarding accept loops and any
    /// lazy POLLIN watchers.
    pub(crate) fn shutdown(&self) {
        for fwd in &self.forward {
            fwd.accept_handle.abort();
        }
        for l in &self.listenfd {
            if let Some(ref h) = l.lazy_watcher {
                h.abort();
            }
        }
    }
}

/// Spawn a watcher that waits for POLLIN readability on `listener` (i.e.
/// a queued connection) and sends `service_name` on `lazy_tx`. The watcher
/// does not `accept` — the child will do that once it inherits the fd.
///
/// `AsyncFd::readable()` can return false positives per tokio's docs
/// ("the function may complete without the file descriptor being ready"),
/// typically from spurious epoll wakeups or from edge-triggered state
/// transitions on sibling events. For a listening socket, false positives
/// would trigger lazy starts with no real client behind them.
///
/// To verify, after `readable()` fires we re-check POLLIN using level-
/// triggered `poll(2)` with zero timeout — that returns `POLLIN` iff the
/// accept queue is *currently* non-empty. If empty, we call `clear_ready()`
/// and wait again; only a confirmed pending connection triggers `lazy_tx`.
///
/// Does not `accept` — the kernel's accept queue entry is preserved so the
/// child's first `accept` consumes the queued connection.
fn spawn_listenfd_watcher(
    listener: std::sync::Arc<std::net::TcpListener>,
    lazy_tx: mpsc::Sender<String>,
    service_name: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let raw_fd = listener.as_raw_fd();
        let Ok(async_fd) = AsyncFd::new(listener) else {
            return;
        };
        loop {
            let mut guard = match async_fd.readable().await {
                Ok(g) => g,
                Err(_) => return,
            };
            // Level-triggered verification: `poll(2)` returns the fd's
            // current state, not a cached wakeup. POLLIN on a listening
            // socket means the accept queue has at least one entry.
            if has_pending_connection(raw_fd) {
                let _ = lazy_tx.try_send(service_name);
                return;
            }
            guard.clear_ready();
        }
    })
}

/// Non-blocking check for a queued connection on a listening fd.
///
/// `poll(2)` with a zero timeout returns immediately with the fd's current
/// readiness. POLLIN on a listening socket = accept queue non-empty.
/// Non-consuming.
fn has_pending_connection(fd: RawFd) -> bool {
    let mut pollfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // Safety: pollfd is a valid initialized struct on the stack; poll()
    // reads one entry as specified by the count argument (1).
    let ret = unsafe { libc::poll(&mut pollfd, 1, 0) };
    ret > 0 && (pollfd.revents & libc::POLLIN) != 0
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
    emitter: crate::output::LifecycleEmitter,
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
                let addr = listener
                    .local_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_default();
                emitter.lifecycle_event(&format!(
                    "{service_name}: proxy {addr} accept error: {e} (backoff {delay:?})"
                ));
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
