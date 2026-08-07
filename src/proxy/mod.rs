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
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
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
    backend: ForwardBackend,
    backend_tx: watch::Sender<BackendStatus>,
    accept_handle: JoinHandle<()>,
}

/// What the accept loop should do with an incoming connection.
///
/// `addr: None` means "hold on" — the service is starting or restarting, so
/// connections queue until it comes back. `refusing` means "give up" — the
/// service is in a failed state, so a client is better off seeing the
/// connection close immediately than hanging on a service that isn't coming
/// back on its own. Refusal wins over a stale backend address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct BackendStatus {
    addr: Option<SocketAddr>,
    refusing: bool,
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
/// fd to the child.
///
/// Exactly one supervisor task watches the socket, and `mode_tx` tells it
/// what to do. A single task is not an implementation detail — `AsyncFd`
/// registers the raw fd with the reactor, and a second registration of the
/// same fd fails with `EEXIST`. Two independent watcher tasks would mean the
/// loser silently does nothing.
struct ListenfdListener {
    listen_addr: SocketAddr,
    /// `std::net::TcpListener` (not tokio's) because we need `AsRawFd` and
    /// stable fd semantics for passing into the child via `LISTEN_FDS`.
    /// Wrapped in an `Arc` so the supervisor can hold its own handle.
    listener: std::sync::Arc<std::net::TcpListener>,
    mode_tx: watch::Sender<ListenfdMode>,
    supervisor: JoinHandle<()>,
}

/// What don should be doing with a listenfd socket right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ListenfdMode {
    /// Hands off: the child owns the accept queue (or will once it starts).
    #[default]
    Idle,
    /// Lazy service: the first queued connection triggers a start. Don does
    /// not accept, so the connection is still there for the child.
    Lazy,
    /// Failed service with no process: drain and close queued connections.
    Refuse,
}

/// What don does with incoming connections for a service, derived by the
/// runner from that service's lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionPolicy {
    /// Normal operation: forward to the backend, or let the child accept.
    /// Connections queue while the service is starting or restarting.
    Serve,
    /// Lazy service awaiting its first connection.
    LazyTrigger,
    /// Service failed and no process is alive to serve anything, so close
    /// connections instead of parking clients on a dead socket.
    Refuse,
}

/// Runtime metadata for one proxy listener, in configuration declaration
/// order. The runner uses this to expose the actual public address without
/// depending on the proxy's internal listener ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProxyBinding {
    pub(crate) configured_addr: String,
    pub(crate) bound_addr: SocketAddr,
    pub(crate) mode: ProxyBindingMode,
    pub(crate) used_fallback: bool,
}

impl ProxyBinding {
    /// Address local clients should use. Wildcard bind addresses are replaced
    /// with their matching loopback address.
    pub(crate) fn connect_addr(&self) -> SocketAddr {
        let ip = match self.bound_addr.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            ip => ip,
        };
        SocketAddr::new(ip, self.bound_addr.port())
    }
}

/// Runtime proxy mode retained alongside a [`ProxyBinding`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProxyBindingMode {
    Env { env_name: String },
    Forward { target: SocketAddr },
    Listenfd,
}

/// A set of proxy listeners for a single service.
pub(crate) struct ServiceProxy {
    forward: Vec<ForwardListener>,
    listenfd: Vec<ListenfdListener>,
    bindings: Vec<ProxyBinding>,
    active_forward_connections: Arc<AtomicUsize>,
    policy: ConnectionPolicy,
}

/// The runner's read-only view of a service's proxy.
///
/// The live [`ServiceProxy`] belongs to the service's supervisor; everything
/// the runner needs for status, port references and the ports manifest is
/// here instead. `bindings` never changes after bind. `backend_env` and
/// `policy` are shadows the runner refreshes itself: it is the only sender of
/// policy changes, and every (re)spawn reports its backend env through the
/// wired message — so the shadow is current wherever it is read.
#[derive(Debug, Clone)]
pub(crate) struct ProxyView {
    /// Configured-to-actual binding metadata, in declaration order.
    pub(crate) bindings: Vec<ProxyBinding>,
    /// Live count of Don-owned forward connections, shared with the accept
    /// loops. `None` when the service has no env/forward entries.
    active_forward_connections: Option<Arc<AtomicUsize>>,
    /// Env-mode backend vars (e.g. `{"PORT": "49152"}`) as of the last
    /// spawn — reallocation on restart makes these per-process values.
    pub(crate) backend_env: HashMap<String, String>,
    /// The connection policy the runner last commanded.
    pub(crate) policy: ConnectionPolicy,
}

impl ProxyView {
    /// Human-readable entries using the actual bound public addresses.
    pub(crate) fn descriptions(&self) -> Vec<String> {
        descriptions_for(&self.bindings)
    }

    /// Whether the proxy is refusing connections under the last policy.
    pub(crate) fn is_refusing(&self) -> bool {
        self.policy == ConnectionPolicy::Refuse
    }

    /// Active Don-owned proxy connections; `None` for listenfd-only proxies.
    pub(crate) fn active_forward_connections(&self) -> Option<usize> {
        self.active_forward_connections
            .as_ref()
            .map(|count| count.load(Ordering::Relaxed))
    }

    /// Public address env vars — see [`ServiceProxy::public_env_vars`].
    pub(crate) fn public_env_vars(&self) -> HashMap<String, String> {
        public_env_vars_for(&self.bindings)
    }

    /// Runtime reference values for other services' inline env expansion.
    /// Values always describe public listener addresses, never env-mode
    /// backend ports: `$(database.PORT)` resolves to the public port for
    /// `proxy = { ..., env = "PORT" }`.
    pub(crate) fn env_reference_values(&self) -> HashMap<String, String> {
        env_reference_values_for(&self.bindings)
    }

    /// True if any proxy entry requires serial (no-overlap) restart. A fixed
    /// `Forward` backend can't have two processes binding the same port at
    /// once, so the old instance must fully exit before the new one starts.
    pub(crate) fn requires_full_exit_on_restart(&self) -> bool {
        self.bindings
            .iter()
            .any(|binding| matches!(binding.mode, ProxyBindingMode::Forward { .. }))
    }
}

enum PendingListener {
    Forward {
        listener: TcpListener,
        backend: ForwardBackend,
    },
    Listenfd {
        listener: std::net::TcpListener,
        listen_addr: SocketAddr,
    },
}

struct BoundListener<T> {
    listener: T,
    addr: SocketAddr,
    used_fallback: bool,
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
    ///
    /// All listeners are bound before any accept loop or lazy watcher is
    /// spawned. If a later entry fails, dropping the pending listeners releases
    /// every earlier port instead of leaving detached tasks holding them open.
    pub(crate) async fn bind(
        entries: &[ProxyEntry],
        fallback_ports: bool,
        lazy_tx: Option<mpsc::Sender<String>>,
        service_name: &str,
        emitter: crate::output::LifecycleEmitter,
    ) -> Result<Self, ProxyError> {
        let mut pending = Vec::with_capacity(entries.len());
        let mut bindings = Vec::with_capacity(entries.len());

        // Phase one: reserve every public listener and allocate env-mode
        // backends. No tasks are spawned until the whole set succeeds.
        for entry in entries {
            let configured_addr: SocketAddr =
                entry.listen.parse().map_err(|e| ProxyError::Bind {
                    addr: entry.listen.clone(),
                    source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                })?;

            match &entry.mode {
                ProxyMode::Env(env_name) => {
                    let bound = bind_tokio_listener(configured_addr, fallback_ports)
                        .await
                        .map_err(|source| ProxyError::Bind {
                            addr: entry.listen.clone(),
                            source,
                        })?;
                    let ephemeral_addr = allocate_ephemeral_port().await?;
                    bindings.push(ProxyBinding {
                        configured_addr: entry.listen.clone(),
                        bound_addr: bound.addr,
                        mode: ProxyBindingMode::Env {
                            env_name: env_name.clone(),
                        },
                        used_fallback: bound.used_fallback,
                    });
                    pending.push(PendingListener::Forward {
                        listener: bound.listener,
                        backend: ForwardBackend::Ephemeral {
                            env_name: env_name.clone(),
                            addr: ephemeral_addr,
                        },
                    });
                }
                ProxyMode::Forward(target) => {
                    let backend_addr: SocketAddr =
                        target.parse().map_err(|e| ProxyError::Bind {
                            addr: target.clone(),
                            source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                        })?;
                    let bound = bind_tokio_listener(configured_addr, fallback_ports)
                        .await
                        .map_err(|source| ProxyError::Bind {
                            addr: entry.listen.clone(),
                            source,
                        })?;
                    bindings.push(ProxyBinding {
                        configured_addr: entry.listen.clone(),
                        bound_addr: bound.addr,
                        mode: ProxyBindingMode::Forward {
                            target: backend_addr,
                        },
                        used_fallback: bound.used_fallback,
                    });
                    pending.push(PendingListener::Forward {
                        listener: bound.listener,
                        backend: ForwardBackend::Fixed(backend_addr),
                    });
                }
                ProxyMode::Listenfd => {
                    // `std::net::TcpListener` gives us stable fd semantics for
                    // the LISTEN_FDS handoff. It deliberately remains blocking:
                    // O_NONBLOCK is shared across dup/dup2 and would otherwise
                    // change the child's fd too.
                    let bound =
                        bind_std_listener(configured_addr, fallback_ports).map_err(|source| {
                            ProxyError::Bind {
                                addr: entry.listen.clone(),
                                source,
                            }
                        })?;
                    bindings.push(ProxyBinding {
                        configured_addr: entry.listen.clone(),
                        bound_addr: bound.addr,
                        mode: ProxyBindingMode::Listenfd,
                        used_fallback: bound.used_fallback,
                    });
                    pending.push(PendingListener::Listenfd {
                        listener: bound.listener,
                        listen_addr: bound.addr,
                    });
                }
            }
        }

        // Phase two: now that all reservations succeeded, start the work that
        // owns them for the lifetime of ServiceProxy.
        let mut forward = Vec::new();
        let mut listenfd = Vec::new();
        let active_forward_connections = Arc::new(AtomicUsize::new(0));

        for listener in pending {
            match listener {
                PendingListener::Forward { listener, backend } => {
                    let (backend_tx, backend_rx) = watch::channel(BackendStatus::default());
                    let accept_handle = tokio::spawn(proxy_accept_loop(
                        listener,
                        backend_rx,
                        lazy_tx.clone(),
                        service_name.to_string(),
                        emitter.clone(),
                        active_forward_connections.clone(),
                    ));
                    forward.push(ForwardListener {
                        backend,
                        backend_tx,
                        accept_handle,
                    });
                }
                PendingListener::Listenfd {
                    listener,
                    listen_addr,
                } => {
                    let listener = std::sync::Arc::new(listener);
                    // A lazy service starts armed; everything else hands the
                    // socket straight to its child.
                    let initial_mode = if lazy_tx.is_some() {
                        ListenfdMode::Lazy
                    } else {
                        ListenfdMode::Idle
                    };
                    let (mode_tx, mode_rx) = watch::channel(initial_mode);
                    let supervisor = tokio::spawn(listenfd_supervisor(
                        listener.clone(),
                        mode_rx,
                        lazy_tx.clone(),
                        service_name.to_string(),
                        listen_addr,
                        emitter.clone(),
                    ));
                    listenfd.push(ListenfdListener {
                        listen_addr,
                        listener,
                        mode_tx,
                        supervisor,
                    });
                }
            }
        }

        Ok(ServiceProxy {
            forward,
            listenfd,
            bindings,
            active_forward_connections,
            policy: if lazy_tx.is_some() {
                ConnectionPolicy::LazyTrigger
            } else {
                ConnectionPolicy::Serve
            },
        })
    }

    /// Point every forwarding backend at its configured address — the
    /// ephemeral one for `Env` mode, the fixed one for `Forward` mode.
    /// Listenfd entries are unaffected (the child owns the fd directly).
    pub(crate) fn set_backend(&self) {
        for fwd in &self.forward {
            let addr = fwd.backend.addr();
            fwd.backend_tx
                .send_modify(|status| status.addr = Some(addr));
        }
    }

    /// Clear all forwarding backends. New connections queue until a
    /// backend is set again.
    pub(crate) fn clear_backend(&self) {
        for fwd in &self.forward {
            fwd.backend_tx.send_modify(|status| status.addr = None);
        }
    }

    /// Set what happens to incoming connections. Returns whether the policy
    /// actually changed.
    ///
    /// The runner derives this from the service's lifecycle state. Queuing is
    /// right while a service is starting or restarting — the client waits a
    /// moment and gets served. [`ConnectionPolicy::Refuse`] is for a service
    /// that failed *and* has no process left, where a queued client would
    /// hang forever on a socket nothing is going to read.
    pub(crate) fn set_policy(&mut self, policy: ConnectionPolicy) -> bool {
        if self.policy == policy {
            return false;
        }
        self.policy = policy;

        let refusing = policy == ConnectionPolicy::Refuse;
        for fwd in &self.forward {
            fwd.backend_tx
                .send_modify(|status| status.refusing = refusing);
        }

        let mode = match policy {
            ConnectionPolicy::Serve => ListenfdMode::Idle,
            ConnectionPolicy::LazyTrigger => ListenfdMode::Lazy,
            ConnectionPolicy::Refuse => ListenfdMode::Refuse,
        };
        for l in &self.listenfd {
            let _ = l.mode_tx.send(mode);
        }

        true
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

    /// Public address env vars for this service. These describe Don's
    /// externally reachable listeners, which can differ from the configured
    /// addresses when fallback ports or explicit port 0 are used.
    pub(crate) fn public_env_vars(&self) -> HashMap<String, String> {
        public_env_vars_for(&self.bindings)
    }

    /// A [`ProxyView`] of this proxy's current metadata, for the runner's
    /// bookkeeping while the proxy itself lives with the supervisor.
    pub(crate) fn view(&self) -> ProxyView {
        ProxyView {
            bindings: self.bindings.clone(),
            active_forward_connections: (!self.forward.is_empty())
                .then(|| self.active_forward_connections.clone()),
            backend_env: self.env_vars(),
            policy: self.policy,
        }
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

    /// Configured-to-actual binding metadata in declaration order.
    pub(crate) fn bindings(&self) -> &[ProxyBinding] {
        &self.bindings
    }

    /// Addresses Don is listening on, in original declaration order.
    pub(crate) fn listen_addrs(&self) -> Vec<SocketAddr> {
        self.bindings
            .iter()
            .map(|binding| binding.bound_addr)
            .collect()
    }

    /// User-facing messages for listeners that could not claim their
    /// configured port and were moved to an OS-selected fallback port.
    pub(crate) fn fallback_descriptions(&self) -> Vec<String> {
        self.bindings
            .iter()
            .filter(|binding| binding.used_fallback)
            .map(|binding| {
                format!(
                    "{} is in use; using {}",
                    binding.configured_addr, binding.bound_addr
                )
            })
            .collect()
    }

    /// Shut down all proxy work — abort forwarding accept loops and the
    /// listenfd supervisors.
    pub(crate) fn shutdown(&self) {
        for fwd in &self.forward {
            fwd.accept_handle.abort();
        }
        for l in &self.listenfd {
            l.supervisor.abort();
        }
    }
}

async fn bind_tokio_listener(
    configured_addr: SocketAddr,
    fallback_ports: bool,
) -> Result<BoundListener<TcpListener>, std::io::Error> {
    match TcpListener::bind(configured_addr).await {
        Ok(listener) => {
            let addr = listener.local_addr()?;
            Ok(BoundListener {
                listener,
                addr,
                used_fallback: false,
            })
        }
        Err(error) if should_fallback(configured_addr, fallback_ports, &error) => {
            let listener = TcpListener::bind(fallback_addr(configured_addr)).await?;
            let addr = listener.local_addr()?;
            Ok(BoundListener {
                listener,
                addr,
                used_fallback: true,
            })
        }
        Err(error) => Err(error),
    }
}

fn bind_std_listener(
    configured_addr: SocketAddr,
    fallback_ports: bool,
) -> Result<BoundListener<std::net::TcpListener>, std::io::Error> {
    match std::net::TcpListener::bind(configured_addr) {
        Ok(listener) => {
            let addr = listener.local_addr()?;
            Ok(BoundListener {
                listener,
                addr,
                used_fallback: false,
            })
        }
        Err(error) if should_fallback(configured_addr, fallback_ports, &error) => {
            let listener = std::net::TcpListener::bind(fallback_addr(configured_addr))?;
            let addr = listener.local_addr()?;
            Ok(BoundListener {
                listener,
                addr,
                used_fallback: true,
            })
        }
        Err(error) => Err(error),
    }
}

fn should_fallback(
    configured_addr: SocketAddr,
    fallback_ports: bool,
    error: &std::io::Error,
) -> bool {
    fallback_ports && configured_addr.port() != 0 && error.kind() == std::io::ErrorKind::AddrInUse
}

fn fallback_addr(mut configured_addr: SocketAddr) -> SocketAddr {
    configured_addr.set_port(0);
    configured_addr
}

/// Public address env vars derived from binding metadata — the body of
/// [`ServiceProxy::public_env_vars`] and [`ProxyView::public_env_vars`].
fn public_env_vars_for(bindings: &[ProxyBinding]) -> HashMap<String, String> {
    let mut vars = HashMap::new();

    for (idx, binding) in bindings.iter().enumerate() {
        let addr = binding.connect_addr();
        vars.insert(format!("DON_PUBLIC_ADDR_{idx}"), addr.to_string());
        vars.insert(format!("DON_PUBLIC_PORT_{idx}"), addr.port().to_string());

        if idx == 0 {
            vars.insert("DON_PUBLIC_ADDR".to_string(), addr.to_string());
            vars.insert("DON_PUBLIC_PORT".to_string(), addr.port().to_string());
        }

        if let ProxyBindingMode::Env { env_name } = &binding.mode {
            let suffix = sanitize_env_suffix(env_name);
            if !suffix.is_empty() {
                vars.insert(format!("DON_PUBLIC_{suffix}"), addr.port().to_string());
                vars.insert(format!("DON_PUBLIC_{suffix}_ADDR"), addr.to_string());
                vars.insert(format!("DON_PUBLIC_{suffix}_PORT"), addr.port().to_string());
            }
        }
    }

    vars
}

/// Reference values (`$(service.PORT)` and friends) derived from binding
/// metadata. Always public listener addresses, never env-mode backend ports.
fn env_reference_values_for(bindings: &[ProxyBinding]) -> HashMap<String, String> {
    let mut vars = HashMap::new();

    for (idx, binding) in bindings.iter().enumerate() {
        let addr = binding.connect_addr();
        vars.insert(format!("addr_{idx}"), addr.to_string());
        vars.insert(format!("port_{idx}"), addr.port().to_string());
        vars.insert(format!("ADDR_{idx}"), addr.to_string());
        vars.insert(format!("PORT_{idx}"), addr.port().to_string());

        if idx == 0 {
            vars.insert("addr".to_string(), addr.to_string());
            vars.insert("port".to_string(), addr.port().to_string());
            vars.insert("ADDR".to_string(), addr.to_string());
            vars.insert("PORT".to_string(), addr.port().to_string());
        }

        if let ProxyBindingMode::Env { env_name } = &binding.mode {
            let suffix = sanitize_env_suffix(env_name);
            if !suffix.is_empty() {
                vars.insert(suffix.clone(), addr.port().to_string());
                vars.insert(format!("{suffix}_ADDR"), addr.to_string());
                vars.insert(format!("{suffix}_PORT"), addr.port().to_string());
            }
        }
    }

    vars
}

/// Human-readable listener entries derived from binding metadata.
fn descriptions_for(bindings: &[ProxyBinding]) -> Vec<String> {
    bindings
        .iter()
        .map(|binding| match &binding.mode {
            ProxyBindingMode::Env { env_name } => {
                format!("{} (env={env_name})", binding.bound_addr)
            }
            ProxyBindingMode::Forward { target } => {
                format!("{} → {target}", binding.bound_addr)
            }
            ProxyBindingMode::Listenfd => {
                format!("{} (listenfd)", binding.bound_addr)
            }
        })
        .collect()
}

fn sanitize_env_suffix(name: &str) -> String {
    name.chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_uppercase())
            } else if ch == '_' {
                Some('_')
            } else {
                None
            }
        })
        .collect()
}

/// The single task that watches one listenfd socket.
///
/// It owns the only `AsyncFd` registration for that fd and switches behavior
/// from a [`ListenfdMode`] watch channel, so the lazy trigger and the refusal
/// drain can never collide (a second `AsyncFd` on the same fd fails with
/// `EEXIST`, which would silently disable whichever job lost).
///
/// `AsyncFd::readable()` can return false positives per tokio's docs ("the
/// function may complete without the file descriptor being ready"), typically
/// from spurious epoll wakeups. Every mode therefore re-checks POLLIN with
/// level-triggered `poll(2)` (zero timeout), which reports whether the accept
/// queue is *currently* non-empty. That check is also what makes the blocking
/// `accept` in `Refuse` safe: the listener deliberately stays blocking because
/// its `O_NONBLOCK` flag is shared with the child's inherited fd.
///
/// Mode behavior:
/// - `Idle`: don does nothing; the child owns the accept queue.
/// - `Lazy`: fire the start trigger on the first queued connection, without
///   accepting — the kernel's queue entry is preserved so the child's first
///   `accept` consumes it. Fires once, then waits for the next mode change.
/// - `Refuse`: accept and immediately close, draining the queue. Only used
///   when the service has failed *and* has no live process, so nothing else
///   is accepting on this socket.
async fn listenfd_supervisor(
    listener: std::sync::Arc<std::net::TcpListener>,
    mut mode_rx: watch::Receiver<ListenfdMode>,
    lazy_tx: Option<mpsc::Sender<String>>,
    service_name: String,
    listen_addr: SocketAddr,
    emitter: crate::output::LifecycleEmitter,
) {
    let raw_fd = listener.as_raw_fd();
    let async_fd = match AsyncFd::new(listener) {
        Ok(async_fd) => async_fd,
        Err(e) => {
            // Without a registration don can neither trigger a lazy start nor
            // refuse connections on this socket. Say so — silence here reads
            // as "the feature is off" with no way to find out why.
            emitter.lifecycle_event(&format!(
                "{service_name}: proxy {listen_addr} cannot be watched: {e}"
            ));
            return;
        }
    };

    'mode: loop {
        let mode = *mode_rx.borrow_and_update();
        match mode {
            ListenfdMode::Idle => {
                if mode_rx.changed().await.is_err() {
                    return;
                }
            }
            ListenfdMode::Lazy => loop {
                tokio::select! {
                    // Biased: a mode change wins over a readiness wakeup, so
                    // leaving a mode takes effect at the first opportunity.
                    biased;
                    changed = mode_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        continue 'mode;
                    }
                    guard = async_fd.readable() => {
                        let Ok(mut guard) = guard else { return };
                        if has_pending_connection(raw_fd) {
                            if let Some(ref tx) = lazy_tx {
                                let _ = tx.try_send(service_name.clone());
                            }
                            // Fired. Stay put until the runner moves us on.
                            if mode_rx.changed().await.is_err() {
                                return;
                            }
                            continue 'mode;
                        }
                        guard.clear_ready();
                    }
                }
            },
            ListenfdMode::Refuse => loop {
                tokio::select! {
                    biased;
                    changed = mode_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        continue 'mode;
                    }
                    guard = async_fd.readable() => {
                        let Ok(mut guard) = guard else { return };
                        // Re-check the mode each iteration: draining is the one
                        // place don accepts on a socket a child may be about to
                        // own, so stop the instant we're told to.
                        while !mode_rx.has_changed().unwrap_or(true)
                            && has_pending_connection(raw_fd)
                        {
                            match async_fd.get_ref().accept() {
                                Ok((stream, _peer)) => {
                                    let _ = stream.shutdown(std::net::Shutdown::Both);
                                    emitter.service_debug_event(
                                        &service_name,
                                        &format!(
                                            "proxy {listen_addr} refused connection (service failed)"
                                        ),
                                    );
                                }
                                Err(_) => break,
                            }
                        }
                        guard.clear_ready();
                    }
                }
            },
        }
    }
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
    backend_rx: watch::Receiver<BackendStatus>,
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
    backend_rx: watch::Receiver<BackendStatus>,
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
        let (mut client, _peer) = match listener.accept().await {
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

        // Wait for a backend address to become available — or for the service
        // to enter a failed state, in which case the connection is refused
        // rather than parked indefinitely.
        let backend_addr = {
            let mut rx = backend_rx.clone();
            loop {
                let status = *rx.borrow();
                if status.refusing {
                    break None;
                }
                if let Some(addr) = status.addr {
                    break Some(addr);
                }
                if rx.changed().await.is_err() {
                    // Channel closed — shutting down.
                    return;
                }
            }
        };

        let Some(backend_addr) = backend_addr else {
            emitter.service_debug_event(
                &service_name,
                &format!("proxy {listen_addr} refused connection (service failed)"),
            );
            let _ = client.shutdown().await;
            continue;
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

    use crate::config::{LogConfig, ProxyEntry, ProxyMode};
    use crate::output::{LifecycleEmitter, OutputManager};

    use super::{
        BackendStatus, ConnectionPolicy, DEFAULT_PROXY_CONNECTION_LIMIT,
        MIN_PROXY_CONNECTION_LIMIT, PROXY_FD_RESERVE, ProxyBinding, ProxyBindingMode,
        ProxyConnectionAccounting, ProxyError, ServiceProxy, backend_connect_candidates,
        proxy_accept_loop_with_permits, proxy_connection_limit_for_soft_nofile,
    };

    #[tokio::test]
    async fn proxy_bind_port_selection_cases() {
        struct Case {
            name: &'static str,
            mode: ProxyMode,
            occupied: bool,
            configured_port_zero: bool,
            fallback_ports: bool,
            expect_success: bool,
            expect_fallback: bool,
        }

        let cases = vec![
            Case {
                name: "preferred env port remains unchanged",
                mode: ProxyMode::Env("PORT".to_string()),
                occupied: false,
                configured_port_zero: false,
                fallback_ports: true,
                expect_success: true,
                expect_fallback: false,
            },
            Case {
                name: "occupied env port falls back",
                mode: ProxyMode::Env("PORT".to_string()),
                occupied: true,
                configured_port_zero: false,
                fallback_ports: true,
                expect_success: true,
                expect_fallback: true,
            },
            Case {
                name: "occupied listenfd port falls back",
                mode: ProxyMode::Listenfd,
                occupied: true,
                configured_port_zero: false,
                fallback_ports: true,
                expect_success: true,
                expect_fallback: true,
            },
            Case {
                name: "disabled fallback preserves bind error",
                mode: ProxyMode::Listenfd,
                occupied: true,
                configured_port_zero: false,
                fallback_ports: false,
                expect_success: false,
                expect_fallback: false,
            },
            Case {
                name: "explicit port zero records actual address",
                mode: ProxyMode::Listenfd,
                occupied: false,
                configured_port_zero: true,
                fallback_ports: true,
                expect_success: true,
                expect_fallback: false,
            },
        ];

        let emitter = test_lifecycle_emitter().await;
        for case in cases {
            // A "free" port is obtained by binding 127.0.0.1:0, reading the
            // address and dropping the listener — don has to perform the real
            // bind itself, so the window between that drop and don's bind
            // cannot be closed from the test and any other process on the
            // machine can claim the port in it. A steal has an unambiguous
            // signature (don reports a fallback for a port we expected to be
            // free), so the *setup* is retried when it shows up. A genuine
            // regression produces that same signature on every attempt and
            // still fails the assertions below on the final one.
            const SETUP_ATTEMPTS: usize = 10;
            let mut attempt = 0;
            let (configured_addr, blocker, result) = loop {
                attempt += 1;
                let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let reserved_addr = reservation.local_addr().unwrap();
                let configured_addr = if case.configured_port_zero {
                    SocketAddr::new(reserved_addr.ip(), 0)
                } else {
                    reserved_addr
                };
                let blocker = if case.occupied {
                    Some(reservation)
                } else {
                    drop(reservation);
                    None
                };
                let entry = ProxyEntry {
                    listen: configured_addr.to_string(),
                    mode: case.mode.clone(),
                };

                let result = ServiceProxy::bind(
                    &[entry],
                    case.fallback_ports,
                    None,
                    case.name,
                    emitter.clone(),
                )
                .await;

                // Only a case that hands don a port it expects to still own
                // can be disturbed by a thief; occupied ports are held by a
                // live listener and port 0 never falls back.
                let port_stolen = !case.occupied
                    && !case.configured_port_zero
                    && matches!(
                        &result,
                        Ok(proxy) if proxy
                            .view()
                            .bindings
                            .first()
                            .is_some_and(|binding| binding.used_fallback)
                            != case.expect_fallback
                    );
                if port_stolen && attempt < SETUP_ATTEMPTS {
                    continue;
                }
                break (configured_addr, blocker, result);
            };

            if case.expect_success {
                let proxy = result.unwrap_or_else(|error| {
                    panic!("{}: expected bind success, got {error}", case.name)
                });
                let view = proxy.view();
                let binding = view
                    .bindings
                    .first()
                    .unwrap_or_else(|| panic!("{}: missing binding metadata", case.name));
                assert_eq!(
                    binding.bound_addr.ip(),
                    configured_addr.ip(),
                    "{}",
                    case.name
                );
                assert_ne!(binding.bound_addr.port(), 0, "{}", case.name);
                assert_eq!(binding.used_fallback, case.expect_fallback, "{}", case.name);
                if case.expect_fallback {
                    assert_ne!(
                        binding.bound_addr.port(),
                        configured_addr.port(),
                        "{}",
                        case.name
                    );
                    assert_eq!(proxy.fallback_descriptions().len(), 1, "{}", case.name);
                } else if configured_addr.port() != 0 {
                    assert_eq!(binding.bound_addr, configured_addr, "{}", case.name);
                    assert!(proxy.fallback_descriptions().is_empty(), "{}", case.name);
                } else {
                    assert!(proxy.fallback_descriptions().is_empty(), "{}", case.name);
                }
            } else {
                match result {
                    Err(ProxyError::Bind { source, .. }) => {
                        assert_eq!(
                            source.kind(),
                            std::io::ErrorKind::AddrInUse,
                            "{}",
                            case.name
                        );
                    }
                    Err(error) => panic!("{}: unexpected error: {error}", case.name),
                    Ok(_) => panic!("{}: expected bind failure", case.name),
                }
            }

            drop(blocker);
        }
    }

    #[tokio::test]
    async fn proxy_metadata_preserves_mixed_declaration_order() {
        let entries = vec![
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Listenfd,
            },
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Env("api_port".to_string()),
            },
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Forward("127.0.0.1:9".to_string()),
            },
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Listenfd,
            },
        ];
        let emitter = test_lifecycle_emitter().await;
        let proxy = ServiceProxy::bind(&entries, true, None, "mixed", emitter)
            .await
            .unwrap();
        let view = proxy.view();
        let bindings = &view.bindings;

        assert_eq!(bindings.len(), 4);
        assert!(matches!(&bindings[0].mode, ProxyBindingMode::Listenfd));
        assert!(matches!(
            &bindings[1].mode,
            ProxyBindingMode::Env { env_name } if env_name == "api_port"
        ));
        assert!(matches!(
            &bindings[2].mode,
            ProxyBindingMode::Forward { target } if *target == "127.0.0.1:9".parse().unwrap()
        ));
        assert!(matches!(&bindings[3].mode, ProxyBindingMode::Listenfd));

        let actual_addrs: Vec<SocketAddr> =
            bindings.iter().map(|binding| binding.bound_addr).collect();
        assert_eq!(proxy.listen_addrs(), actual_addrs);
        assert_eq!(
            proxy.view().descriptions(),
            vec![
                format!("{} (listenfd)", actual_addrs[0]),
                format!("{} (env=api_port)", actual_addrs[1]),
                format!("{} → 127.0.0.1:9", actual_addrs[2]),
                format!("{} (listenfd)", actual_addrs[3]),
            ]
        );

        let public_env = proxy.public_env_vars();
        for (idx, addr) in actual_addrs.iter().enumerate() {
            assert_eq!(
                public_env.get(&format!("DON_PUBLIC_ADDR_{idx}")),
                Some(&addr.to_string())
            );
            assert_eq!(
                public_env.get(&format!("DON_PUBLIC_PORT_{idx}")),
                Some(&addr.port().to_string())
            );
        }
        assert_eq!(
            public_env.get("DON_PUBLIC_ADDR"),
            Some(&actual_addrs[0].to_string())
        );
        assert_eq!(
            public_env.get("DON_PUBLIC_API_PORT"),
            Some(&actual_addrs[1].port().to_string())
        );
        assert_eq!(
            public_env.get("DON_PUBLIC_API_PORT_ADDR"),
            Some(&actual_addrs[1].to_string())
        );

        let references = proxy.view().env_reference_values();
        assert_eq!(references.get("addr"), Some(&actual_addrs[0].to_string()));
        assert_eq!(
            references.get("port_2"),
            Some(&actual_addrs[2].port().to_string())
        );
        assert_eq!(
            references.get("API_PORT"),
            Some(&actual_addrs[1].port().to_string())
        );
        assert_eq!(
            references.get("API_PORT_ADDR"),
            Some(&actual_addrs[1].to_string())
        );

        let listenfd_env = proxy.listenfd_env();
        assert_eq!(
            listenfd_env.get("LISTEN_FDNAMES"),
            Some(&format!("{}:{}", actual_addrs[0], actual_addrs[3]))
        );
        assert!(proxy.fallback_descriptions().is_empty());

        let cloned = proxy.view().bindings;
        drop(proxy);
        assert_eq!(cloned.len(), 4);
        assert_eq!(cloned[1].bound_addr, actual_addrs[1]);
    }

    #[test]
    fn wildcard_public_addresses_use_loopback_for_clients() {
        let cases = vec![
            (
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3000),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000),
            ),
            (
                SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 3000),
                SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 3000),
            ),
        ];

        for (bound_addr, expected) in cases {
            let binding = ProxyBinding {
                configured_addr: bound_addr.to_string(),
                bound_addr,
                mode: ProxyBindingMode::Listenfd,
                used_fallback: false,
            };
            assert_eq!(binding.connect_addr(), expected);
        }
    }

    #[tokio::test]
    async fn failed_bind_releases_all_earlier_pending_listeners() {
        let emitter = test_lifecycle_emitter().await;

        // The first entry needs an address that is free when don binds it, and
        // still free when the test re-binds it afterwards. Both ends of that
        // sequence are races against every other process on the machine: the
        // reserve-then-drop window before don's bind, and the gap between don
        // releasing the address and the test's re-bind. Neither can be closed
        // from the test, so the whole scenario is retried on a collision — a
        // don that really holds the listener fails every attempt, and the
        // assertions below fire on the last one.
        const SETUP_ATTEMPTS: usize = 10;
        for attempt in 1..=SETUP_ATTEMPTS {
            let last_attempt = attempt == SETUP_ATTEMPTS;

            let first_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let first_addr = first_reservation.local_addr().unwrap();
            drop(first_reservation);

            let _blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let blocked_addr = _blocker.local_addr().unwrap();
            let entries = vec![
                ProxyEntry {
                    listen: first_addr.to_string(),
                    mode: ProxyMode::Env("PORT".to_string()),
                },
                ProxyEntry {
                    listen: blocked_addr.to_string(),
                    mode: ProxyMode::Listenfd,
                },
            ];

            let result =
                ServiceProxy::bind(&entries, false, None, "transactional", emitter.clone()).await;

            match &result {
                // The intended failure: the *second* entry could not bind.
                Err(ProxyError::Bind { addr, .. }) if *addr == blocked_addr.to_string() => {}
                // Someone else took the first address inside the reservation
                // window, so this attempt never exercised the rollback.
                Err(ProxyError::Bind { addr, .. })
                    if *addr == first_addr.to_string() && !last_attempt =>
                {
                    continue;
                }
                Err(error) => panic!("expected the second entry to fail its bind, got {error}"),
                Ok(_) => panic!("expected the second entry to fail its bind, it succeeded"),
            }

            let rebound = tokio::net::TcpListener::bind(first_addr).await;
            if rebound.is_ok() {
                return;
            }
            assert!(
                !last_attempt,
                "the first listener remained owned after a later bind failed"
            );
        }
    }

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
    async fn proxy_refuses_connections_while_the_service_is_failed() {
        struct Case {
            name: &'static str,
            entries: Vec<ProxyMode>,
            /// Lazy services keep a start trigger armed on every listenfd
            /// socket. The refusal drain has to share that fd, not fight it.
            lazy: bool,
        }

        let cases = vec![
            Case {
                name: "env-mode forwarding listener",
                entries: vec![ProxyMode::Env("PORT".to_string())],
                lazy: false,
            },
            Case {
                name: "listenfd listener",
                entries: vec![ProxyMode::Listenfd],
                lazy: false,
            },
            Case {
                name: "lazy service with several listenfd listeners",
                entries: vec![ProxyMode::Listenfd, ProxyMode::Listenfd],
                lazy: true,
            },
            Case {
                name: "lazy service mixing env and listenfd",
                entries: vec![ProxyMode::Env("PORT".to_string()), ProxyMode::Listenfd],
                lazy: true,
            },
        ];

        let emitter = test_lifecycle_emitter().await;
        for case in cases {
            let entries: Vec<ProxyEntry> = case
                .entries
                .iter()
                .map(|mode| ProxyEntry {
                    listen: "127.0.0.1:0".to_string(),
                    mode: mode.clone(),
                })
                .collect();
            let (lazy_tx, _lazy_rx) = mpsc::channel(8);
            let mut proxy = ServiceProxy::bind(
                &entries,
                false,
                case.lazy.then_some(lazy_tx),
                "svc",
                emitter.clone(),
            )
            .await
            .unwrap();
            let addrs: Vec<SocketAddr> = proxy
                .view()
                .bindings
                .iter()
                .map(|binding| binding.bound_addr)
                .collect();

            // Healthy-but-not-ready: connections are parked, not closed, so a
            // client that arrives mid-restart is served once the service is up.
            let mut parked = Vec::new();
            let mut buf = [0u8; 1];
            for addr in &addrs {
                let mut early = tokio::net::TcpStream::connect(addr).await.unwrap();
                assert!(
                    read_timed_out(&mut early, &mut buf).await,
                    "{} ({addr}): connections should queue before a failure",
                    case.name
                );
                parked.push(early);
            }

            assert!(
                proxy.set_policy(ConnectionPolicy::Refuse),
                "{}: entering refusal should report a change",
                case.name
            );
            assert!(proxy.view().is_refusing(), "{}", case.name);

            // Every listener refuses — parked connections and new ones alike.
            for (addr, mut early) in addrs.iter().zip(parked) {
                assert_closed(
                    &mut early,
                    &mut buf,
                    &format!("{} ({addr}): parked connection after failure", case.name),
                )
                .await;
                let mut during = tokio::net::TcpStream::connect(addr).await.unwrap();
                assert_closed(
                    &mut during,
                    &mut buf,
                    &format!("{} ({addr}): new connection while failed", case.name),
                )
                .await;
            }

            // Leaving the failed state restores the queuing behavior.
            let recovered = if case.lazy {
                ConnectionPolicy::LazyTrigger
            } else {
                ConnectionPolicy::Serve
            };
            assert!(proxy.set_policy(recovered), "{}", case.name);
            assert!(!proxy.view().is_refusing(), "{}", case.name);
            for addr in &addrs {
                let mut after = tokio::net::TcpStream::connect(addr).await.unwrap();
                assert!(
                    read_timed_out(&mut after, &mut buf).await,
                    "{} ({addr}): connections should queue again after recovery",
                    case.name
                );
            }
        }
    }

    /// A lazy listenfd service must still fire its start trigger after it has
    /// been through a refusal cycle — the supervisor is one long-lived task,
    /// so a mishandled mode transition would leave it deaf.
    #[tokio::test]
    async fn lazy_trigger_survives_a_refusal_cycle() {
        let entries = vec![
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Listenfd,
            },
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Listenfd,
            },
        ];
        let (lazy_tx, mut lazy_rx) = mpsc::channel(8);
        let emitter = test_lifecycle_emitter().await;
        let mut proxy = ServiceProxy::bind(&entries, false, Some(lazy_tx), "svc", emitter)
            .await
            .unwrap();
        let addrs: Vec<SocketAddr> = proxy
            .view()
            .bindings
            .iter()
            .map(|binding| binding.bound_addr)
            .collect();

        // Fail, refuse, then recover to Lazy.
        assert!(proxy.set_policy(ConnectionPolicy::Refuse));
        let mut buf = [0u8; 1];
        let mut refused = tokio::net::TcpStream::connect(addrs[0]).await.unwrap();
        assert_closed(&mut refused, &mut buf, "refused while failed").await;
        proxy.set_policy(ConnectionPolicy::LazyTrigger);

        // The second listener — the one that never fired — must trigger too.
        let _client = tokio::net::TcpStream::connect(addrs[1]).await.unwrap();
        let triggered = tokio::time::timeout(std::time::Duration::from_secs(5), lazy_rx.recv())
            .await
            .expect("a queued connection should re-trigger the lazy start");
        assert_eq!(triggered.as_deref(), Some("svc"));
    }

    /// Assert the peer closed the connection promptly and cleanly. A read of
    /// 0 bytes is the orderly close don performs; anything else — data, a
    /// reset, or a timeout — fails, so this can't pass on an unrelated error.
    async fn assert_closed(stream: &mut tokio::net::TcpStream, buf: &mut [u8], context: &str) {
        use tokio::io::AsyncReadExt;
        let read = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(buf))
            .await
            .unwrap_or_else(|_| panic!("{context}: should have been closed promptly, still open"));
        match read {
            Ok(0) => {}
            Ok(n) => panic!("{context}: expected a close, read {n} bytes"),
            Err(e) => panic!("{context}: expected a clean close, got {e}"),
        }
    }

    /// Whether the connection stayed open (no data, no close) for a moment.
    async fn read_timed_out(stream: &mut tokio::net::TcpStream, buf: &mut [u8]) -> bool {
        use tokio::io::AsyncReadExt;
        tokio::time::timeout(std::time::Duration::from_millis(250), stream.read(buf))
            .await
            .is_err()
    }

    #[tokio::test]
    async fn proxy_accept_loop_does_not_reserve_permits_while_idle() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let (_backend_tx, backend_rx) = watch::channel(BackendStatus::default());
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
