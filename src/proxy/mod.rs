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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::io::unix::AsyncFd;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc, watch};
use tokio::task::JoinHandle;

use crate::config::{ProxyEntry, ProxyMode};

const PROXY_FD_RESERVE: u64 = 128;
const DEFAULT_PROXY_CONNECTION_LIMIT: u64 = 16_384;
const MIN_PROXY_CONNECTION_LIMIT: u64 = 16;

static PROXY_CONNECTION_POOL: OnceLock<ProxyConnectionPool> = OnceLock::new();

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
/// bytes to/from a backend that the service itself binds. The backend
/// address is either ephemeral (don allocates, injects as env var) or
/// fixed (service binds a known port on its own).
struct ForwardListener {
    listen_addr: SocketAddr,
    backend: ForwardBackend,
    backend_tx: watch::Sender<Option<SocketAddr>>,
    accept_handle: JoinHandle<()>,
}

/// How the backend address for a [`ForwardListener`] is chosen.
enum ForwardBackend {
    /// Don allocated an ephemeral port and injected it into the service's
    /// env under `env_name`. The service must read the env var to bind.
    Ephemeral { env_name: String, addr: SocketAddr },
    /// Service binds a known fixed address on its own. Don just forwards.
    /// No env var injected.
    Fixed(SocketAddr),
}

impl ForwardBackend {
    fn addr(&self) -> SocketAddr {
        match self {
            Self::Ephemeral { addr, .. } => *addr,
            Self::Fixed(addr) => *addr,
        }
    }
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
    active_forward_connections: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct ProxyConnectionAccounting {
    permits: Arc<Semaphore>,
    max_connections: usize,
    global_active_connections: Arc<AtomicUsize>,
    service_active_connections: Arc<AtomicUsize>,
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
        let active_forward_connections = Arc::new(AtomicUsize::new(0));

        for entry in entries {
            let listen_addr: SocketAddr = entry.listen.parse().map_err(|e| ProxyError::Bind {
                addr: entry.listen.clone(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
            })?;

            match &entry.mode {
                ProxyMode::Env(env_name) => {
                    let listener =
                        TcpListener::bind(listen_addr)
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
                        active_forward_connections.clone(),
                    ));
                    forward.push(ForwardListener {
                        listen_addr,
                        backend: ForwardBackend::Ephemeral {
                            env_name: env_name.clone(),
                            addr: ephemeral_addr,
                        },
                        backend_tx,
                        accept_handle,
                    });
                }
                ProxyMode::Forward(target) => {
                    let backend_addr: SocketAddr =
                        target.parse().map_err(|e| ProxyError::Bind {
                            addr: target.clone(),
                            source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                        })?;
                    let listener =
                        TcpListener::bind(listen_addr)
                            .await
                            .map_err(|e| ProxyError::Bind {
                                addr: entry.listen.clone(),
                                source: e,
                            })?;
                    let (backend_tx, backend_rx) = watch::channel(None);
                    let accept_handle = tokio::spawn(proxy_accept_loop(
                        listener,
                        backend_rx,
                        lazy_tx.clone(),
                        service_name.to_string(),
                        emitter.clone(),
                        active_forward_connections.clone(),
                    ));
                    forward.push(ForwardListener {
                        listen_addr,
                        backend: ForwardBackend::Fixed(backend_addr),
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
                    let listener =
                        std::net::TcpListener::bind(listen_addr).map_err(|e| ProxyError::Bind {
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
            active_forward_connections,
        })
    }

    /// Point every forwarding backend at its configured address — the
    /// ephemeral one for `Env` mode, the fixed one for `Forward` mode.
    /// Listenfd entries are unaffected (the child owns the fd directly).
    pub(crate) fn set_backend(&self) {
        for fwd in &self.forward {
            let _ = fwd.backend_tx.send(Some(fwd.backend.addr()));
        }
    }

    /// Clear all forwarding backends. New connections queue until a
    /// backend is set again.
    pub(crate) fn clear_backend(&self) {
        for fwd in &self.forward {
            let _ = fwd.backend_tx.send(None);
        }
    }

    /// Allocate new ephemeral ports for env-mode entries. Used on restart so
    /// the old port is gone before the new process tries to bind it. Fixed
    /// `Forward` entries are left alone — their address is user-provided
    /// and stable across restarts.
    pub(crate) async fn reallocate_ephemeral_ports(&mut self) -> Result<(), ProxyError> {
        for fwd in &mut self.forward {
            if let ForwardBackend::Ephemeral { addr, .. } = &mut fwd.backend {
                *addr = allocate_ephemeral_port().await?;
            }
        }
        Ok(())
    }

    /// Env var map for env-mode entries, e.g. `{"PORT": "49152"}`. Fixed
    /// `Forward` entries contribute nothing — the service already knows its
    /// port.
    pub(crate) fn env_vars(&self) -> HashMap<String, String> {
        let mut vars = HashMap::new();
        for fwd in &self.forward {
            if let ForwardBackend::Ephemeral { env_name, addr } = &fwd.backend {
                vars.insert(env_name.clone(), addr.port().to_string());
            }
        }
        vars
    }

    /// True if any proxy entry requires serial (no-overlap) restart. Fixed
    /// `Forward` backends can't have two processes binding the same port at
    /// once, so the caller must fully tear down the old instance before
    /// starting the new one.
    pub(crate) fn requires_full_exit_on_restart(&self) -> bool {
        self.forward
            .iter()
            .any(|f| matches!(f.backend, ForwardBackend::Fixed(_)))
    }

    /// Socket-activation env vars for listenfd entries. Empty if the service
    /// has no listenfd proxy entries. `LISTEN_FD=3` is a single-fd convenience
    /// for Node-style bootstraps; `LISTEN_FDS` / `LISTEN_FDNAMES` remain the
    /// systemd-compatible source of truth. `LISTEN_PID` is set by the shell
    /// shim at spawn time — see `process::mod::listen_pid_shim`.
    pub(crate) fn listenfd_env(&self) -> HashMap<String, String> {
        let mut env = HashMap::new();
        if self.listenfd.is_empty() {
            return env;
        }
        if self.listenfd.len() == 1 {
            env.insert("LISTEN_FD".to_string(), "3".to_string());
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
        self.listenfd
            .iter()
            .map(|l| l.listener.as_raw_fd())
            .collect()
    }

    /// Create a handle that can be sent to a spawned task to set env-mode
    /// backends once a ready check passes. The handle is `Send + Sync`.
    pub(crate) fn backend_handle(&self) -> ProxyBackendHandle {
        let pairs: Vec<_> = self
            .forward
            .iter()
            .map(|fwd| (fwd.backend_tx.clone(), fwd.backend.addr()))
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

    /// Active env/forward proxy connections owned by Don. Listenfd-mode
    /// sockets are accepted by the child process, so Don cannot count them.
    pub(crate) fn active_forward_connections(&self) -> Option<usize> {
        if self.forward.is_empty() {
            return None;
        }
        Some(self.active_forward_connections.load(Ordering::Relaxed))
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
    let addr = listener.local_addr().map_err(ProxyError::EphemeralPort)?;
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
    active_connections: Arc<AtomicUsize>,
) {
    let connection_pool = proxy_connection_pool();
    let accounting = ProxyConnectionAccounting {
        permits: connection_pool.permits,
        max_connections: connection_pool.max_connections,
        global_active_connections: connection_pool.active_connections,
        service_active_connections: active_connections,
    };
    proxy_accept_loop_with_permits(
        listener,
        backend_rx,
        lazy_tx,
        service_name,
        emitter,
        accounting,
    )
    .await;
}

async fn proxy_accept_loop_with_permits(
    listener: TcpListener,
    backend_rx: watch::Receiver<Option<SocketAddr>>,
    lazy_tx: Option<mpsc::Sender<String>>,
    service_name: String,
    emitter: crate::output::LifecycleEmitter,
    accounting: ProxyConnectionAccounting,
) {
    let mut consecutive_errors: u32 = 0;
    let mut connection_limit_reported = false;
    let listen_addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
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
                let delay =
                    std::time::Duration::from_millis((10 * consecutive_errors.min(100)) as u64);
                emitter.lifecycle_event(&format!(
                    "{service_name}: proxy {listen_addr} accept error: {e} (backoff {delay:?})"
                ));
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        let connection_guard = match accounting.permits.clone().try_acquire_owned() {
            Ok(permit) => {
                connection_limit_reported = false;
                ProxyConnectionGuard::new(
                    permit,
                    accounting.service_active_connections.clone(),
                    emitter.clone(),
                    service_name.clone(),
                    listen_addr.clone(),
                    accounting.max_connections,
                    accounting.global_active_connections.clone(),
                )
            }
            Err(TryAcquireError::NoPermits) => {
                if !connection_limit_reported {
                    connection_limit_reported = true;
                    let max_connections = accounting.max_connections;
                    let message = format!(
                        "{service_name}: proxy {listen_addr} connection limit reached; closing new connections ({max_connections}/{max_connections} active)"
                    );
                    emitter.lifecycle_event(&message);
                }
                let max_connections = accounting.max_connections;
                emitter.service_debug_event(
                    &service_name,
                    &format!(
                        "proxy {listen_addr} closed overflow connection ({max_connections}/{max_connections} active)"
                    ),
                );
                drop(client);
                continue;
            }
            Err(TryAcquireError::Closed) => return,
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
        tokio::spawn(proxy_connection(client, backend_addr, connection_guard));
    }
}

struct ProxyConnectionGuard {
    _permit: OwnedSemaphorePermit,
    active_connections: Arc<AtomicUsize>,
    emitter: crate::output::LifecycleEmitter,
    service_name: String,
    listen_addr: String,
    max_connections: usize,
    global_active_connections: Arc<AtomicUsize>,
}

impl ProxyConnectionGuard {
    fn new(
        permit: OwnedSemaphorePermit,
        active_connections: Arc<AtomicUsize>,
        emitter: crate::output::LifecycleEmitter,
        service_name: String,
        listen_addr: String,
        max_connections: usize,
        global_active_connections: Arc<AtomicUsize>,
    ) -> Self {
        active_connections.fetch_add(1, Ordering::Relaxed);
        let active = global_active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        emitter.service_debug_event(
            &service_name,
            &format!("proxy {listen_addr} accepted connection ({active}/{max_connections} active)"),
        );
        Self {
            _permit: permit,
            active_connections,
            emitter,
            service_name,
            listen_addr,
            max_connections,
            global_active_connections,
        }
    }
}

impl Drop for ProxyConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        let active = self
            .global_active_connections
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        self.emitter.service_debug_event(
            &self.service_name,
            &format!(
                "proxy {} closed connection ({}/{} active)",
                self.listen_addr, active, self.max_connections
            ),
        );
    }
}

/// Forward traffic bidirectionally between client and backend.
///
/// Retries the backend connection with exponential backoff if the service
/// isn't listening yet (common during startup before the process binds its port).
async fn proxy_connection(
    mut client: TcpStream,
    backend_addr: SocketAddr,
    _connection_guard: ProxyConnectionGuard,
) {
    let backend_candidates = backend_connect_candidates(backend_addr);
    let mut backend = None;
    for attempt in 0..20u32 {
        for candidate in &backend_candidates {
            match TcpStream::connect(candidate).await {
                Ok(stream) => {
                    backend = Some(stream);
                    break;
                }
                Err(_) => continue,
            }
        }

        if backend.is_some() {
            break;
        }

        // Exponential backoff: 10ms, 20ms, 40ms, ... capped at 500ms.
        let delay = std::time::Duration::from_millis((10 * (1 << attempt.min(6))).min(500));
        tokio::time::sleep(delay).await;
    }

    let Some(mut backend) = backend else {
        // Backend never became reachable — close client connection.
        let _ = client.shutdown().await;
        return;
    };

    // Shovel bytes in both directions until either side closes.
    let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
}

#[derive(Clone)]
struct ProxyConnectionPool {
    permits: Arc<Semaphore>,
    max_connections: usize,
    active_connections: Arc<AtomicUsize>,
}

fn proxy_connection_pool() -> ProxyConnectionPool {
    PROXY_CONNECTION_POOL
        .get_or_init(|| {
            let max_connections = proxy_connection_limit();
            ProxyConnectionPool {
                permits: Arc::new(Semaphore::new(max_connections)),
                max_connections,
                active_connections: Arc::new(AtomicUsize::new(0)),
            }
        })
        .clone()
}

fn proxy_connection_limit() -> usize {
    let soft_limit = current_nofile_soft_limit().unwrap_or(DEFAULT_PROXY_CONNECTION_LIMIT * 2);
    proxy_connection_limit_for_soft_nofile(soft_limit) as usize
}

fn proxy_connection_limit_for_soft_nofile(soft_limit: u64) -> u64 {
    let fd_backed_limit = soft_limit.saturating_sub(PROXY_FD_RESERVE) / 2;
    fd_backed_limit.clamp(MIN_PROXY_CONNECTION_LIMIT, DEFAULT_PROXY_CONNECTION_LIMIT)
}

fn current_nofile_soft_limit() -> Option<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // Safety: `limit` points at a valid initialized rlimit struct for
    // getrlimit() to fill.
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if ret != 0 || limit.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    Some(limit.rlim_cur)
}

fn backend_connect_candidates(backend_addr: SocketAddr) -> Vec<SocketAddr> {
    let mut candidates = vec![backend_addr];
    match backend_addr.ip() {
        std::net::IpAddr::V4(ip) if ip.is_loopback() => {
            candidates.push(SocketAddr::new(
                std::net::Ipv6Addr::LOCALHOST.into(),
                backend_addr.port(),
            ));
        }
        std::net::IpAddr::V6(ip) if ip.is_loopback() => {
            candidates.push(SocketAddr::new(
                std::net::Ipv4Addr::LOCALHOST.into(),
                backend_addr.port(),
            ));
        }
        _ => {}
    }
    candidates
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::{Semaphore, mpsc, watch};

    use crate::config::LogConfig;
    use crate::output::{LifecycleEmitter, OutputManager};

    use super::{
        DEFAULT_PROXY_CONNECTION_LIMIT, MIN_PROXY_CONNECTION_LIMIT, PROXY_FD_RESERVE,
        ProxyConnectionAccounting, backend_connect_candidates, proxy_accept_loop_with_permits,
        proxy_connection_limit_for_soft_nofile,
    };

    #[test]
    fn loopback_ipv4_backends_try_ipv6_loopback_too() {
        let candidates =
            backend_connect_candidates(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46165));

        assert_eq!(
            candidates,
            vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46165),
                SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 46165),
            ]
        );
    }

    #[test]
    fn loopback_ipv6_backends_try_ipv4_loopback_too() {
        let candidates =
            backend_connect_candidates(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 46165));

        assert_eq!(
            candidates,
            vec![
                SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 46165),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46165),
            ]
        );
    }

    #[test]
    fn non_loopback_backends_keep_single_target() {
        let target = IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0010));
        let candidates = backend_connect_candidates(SocketAddr::new(target, 46165));

        assert_eq!(candidates, vec![SocketAddr::new(target, 46165)]);
    }

    #[test]
    fn proxy_connection_limit_reserves_fds_for_non_proxy_work() {
        let soft_limit = 1024;

        assert_eq!(
            proxy_connection_limit_for_soft_nofile(soft_limit),
            (soft_limit - PROXY_FD_RESERVE) / 2
        );
    }

    #[test]
    fn proxy_connection_limit_is_capped_for_large_fd_limits() {
        assert_eq!(
            proxy_connection_limit_for_soft_nofile(1_000_000),
            DEFAULT_PROXY_CONNECTION_LIMIT
        );
    }

    #[test]
    fn proxy_connection_limit_keeps_a_small_floor() {
        assert_eq!(
            proxy_connection_limit_for_soft_nofile(64),
            MIN_PROXY_CONNECTION_LIMIT
        );
    }

    #[tokio::test]
    async fn proxy_accept_loop_does_not_reserve_permits_while_idle() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let (_backend_tx, backend_rx) = watch::channel(None);
        let (lazy_tx, mut lazy_rx) = mpsc::channel(1);
        let permits = Arc::new(Semaphore::new(1));
        let global_active_connections = Arc::new(AtomicUsize::new(0));
        let active_connections = Arc::new(AtomicUsize::new(0));
        let emitter = test_lifecycle_emitter().await;
        let accounting = ProxyConnectionAccounting {
            permits: permits.clone(),
            max_connections: 1,
            global_active_connections: global_active_connections.clone(),
            service_active_connections: active_connections.clone(),
        };

        let handle = tokio::spawn(proxy_accept_loop_with_permits(
            listener,
            backend_rx,
            Some(lazy_tx),
            "svc".to_string(),
            emitter,
            accounting,
        ));

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            permits.available_permits(),
            1,
            "idle listeners must not consume active connection permits"
        );
        assert_eq!(
            active_connections.load(Ordering::Relaxed),
            0,
            "idle listeners must not count as active connections"
        );
        assert_eq!(
            global_active_connections.load(Ordering::Relaxed),
            0,
            "idle listeners must not count against the global pool"
        );

        let _client = tokio::net::TcpStream::connect(listen_addr).await.unwrap();
        let triggered = tokio::time::timeout(std::time::Duration::from_secs(1), lazy_rx.recv())
            .await
            .unwrap();
        assert_eq!(triggered.as_deref(), Some("svc"));
        assert_eq!(
            active_connections.load(Ordering::Relaxed),
            1,
            "accepted connections waiting for a backend should be counted"
        );
        assert_eq!(
            global_active_connections.load(Ordering::Relaxed),
            1,
            "accepted connections should count against the global pool"
        );

        handle.abort();
        let _ = handle.await;
        assert_eq!(
            active_connections.load(Ordering::Relaxed),
            0,
            "abandoned connections should release active counts"
        );
        assert_eq!(
            global_active_connections.load(Ordering::Relaxed),
            0,
            "abandoned connections should release global active counts"
        );
    }

    async fn test_lifecycle_emitter() -> LifecycleEmitter {
        let log_config = LogConfig::Stdout;
        let services = [("svc", &log_config)];
        let output_manager = OutputManager::new(&services, tokio::io::sink())
            .await
            .unwrap();
        output_manager.clone_lifecycle_emitter()
    }
}
