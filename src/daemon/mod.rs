//! The system-wide don daemon.
//!
//! Don's per-project model is unchanged by this module: `don start` still
//! owns its own process group, its own `.don/` directory, and its own
//! unix-socket API. The daemon is a *broker* — it holds a registry of the
//! projects that are currently running and serves the web UI on top of them,
//! reverse-proxying each project's existing API. It never spawns or supervises
//! a service itself.
//!
//! That split is what keeps registration cheap and optional: a project that
//! can't reach the daemon just doesn't appear in the UI, and nothing about
//! the stack it's running changes.

pub mod paths;

pub use paths::{DaemonEnv, DaemonPaths, PathError};
