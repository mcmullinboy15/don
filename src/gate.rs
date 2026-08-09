//! Permission to run, published per process by the scheduler.
//!
//! The runner does not start anything. It decides *whether a process is
//! allowed to start* — dependencies satisfied, not shutting down, nothing
//! else in flight — and publishes that decision. Each supervisor watches its
//! own gate and starts itself when it is both permitted and idle.
//!
//! # Why a level, not an event
//!
//! A supervisor can be busy for a long time: a docker pull, a bazel build, a
//! shutdown grace period. If permission were an event it would be missed, and
//! the scheduler would have to remember to re-send — which is the
//! retry-loop-with-a-timer shape this design exists to avoid. So the gate is
//! a `watch`: [`GateReader::get`] is correct after missing any number of
//! changes, because it reports the *current* answer rather than a past one.
//!
//! # Why a level alone is not enough
//!
//! A level double-starts. Concretely: the gate opens, the supervisor starts,
//! the process dies instantly, the supervisor reaps — and the gate is still
//! open, because nothing about this process's own lifecycle closes it. An
//! idle supervisor would see permission and start again, at zero backoff,
//! ignoring `on_failure` and the crash-loop guard (both of which live in
//! `runner/service_health.rs` and are only reached via `ServiceExited`).
//!
//! The answer is that permission is necessary but not sufficient: a start
//! also needs *demand*, which the supervisor owns and which is **one-shot** —
//! cleared in the same synchronous step that begins the start. Because the
//! decision and the spend happen together inside one loop, with no channel
//! between them, demand needs no epoch or generation to identify it.
//!
//! # What a level does *not* tell you
//!
//! A gate says what this process's **dependencies** allow. It says nothing
//! about whether the process is wanted, or whether it is already running —
//! deliberately, because a gate that also encoded "is it wanted" would read
//! this process's own state, and `state(X) -> gate(X) -> state(X)` is a
//! self-loop. Reading only *dependencies'* states keeps every influence edge
//! a dependency edge, and the dependency graph is a validated DAG.
//!
//! So a level is sticky: once a dependency is up, the gate stays open for the
//! rest of the session, across starts, crashes and stops. Permission is not
//! an instruction. What turns it into a start is the supervisor's own demand,
//! and *that* is one-shot.
//!
//! # Why a level carries a revision
//!
//! Demand and permission are two facts that must be read *together*. They are
//! now held by two different actors, so a supervisor can hold fresh demand and
//! a stale level: a dependency starts re-running, its dependents' levels are
//! scheduled for recompute, and before that lands a connection gives one of
//! them demand. Acting on the level it can see would start a service whose
//! dependency is mid-rerun.
//!
//! So every publish pass stamps a monotonic `rev`, and a supervisor may only
//! spend demand against a level published *after* that demand arose. One pass
//! of the scheduler is therefore the synchronisation point where the two facts
//! meet — which is exactly what the runner used to do for free when it owned
//! both.
//!
//! # Ownership
//!
//! [`GateWriter`] is not `Clone` and is moved into the runner, so nothing else
//! can grant permission — the same enforcement-by-ownership as
//! [`crate::state_store::StateWriter`]. The name set is fixed at construction,
//! so the map needs no lock, for the reason
//! [`crate::process::registry`] documents.

use std::collections::HashMap;
use tokio::sync::watch;

/// A published level, stamped with the scheduler pass that produced it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Grant {
    pub(crate) level: Gate,
    /// The scheduler pass this was published by. Monotonic across all
    /// processes, so "published after my demand arose" is a comparison.
    pub(crate) rev: u64,
}

/// How far a process's dependencies let it go.
///
/// Ordered: `Blocked < Degraded < Open`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Gate {
    /// A dependency is still making progress. Nothing may start — waiting is
    /// the right thing to do, because the wait will end.
    Blocked,
    /// Every dependency has settled, but not all are satisfied: something
    /// failed, stopped, or is parked waiting for a human. Waiting will not
    /// end, so a process someone asked for *by name* may proceed; the
    /// scheduler's own starts may not.
    Degraded,
    /// Every dependency is satisfied.
    Open,
}

/// The write half: the scheduler's answer for every process.
///
/// Not `Clone` — it is moved into the runner at construction, so no other
/// component can grant permission.
pub(crate) struct GateWriter {
    txs: HashMap<String, watch::Sender<Grant>>,
    /// Bumped once per scheduler pass and stamped onto every level published
    /// in it. See the module doc: this is what lets a supervisor tell a level
    /// computed *after* its demand from one computed before.
    rev: u64,
    /// Publishing is a no-op until armed. Transitions during construction and
    /// setup therefore cannot grant permission.
    armed: bool,
}

impl GateWriter {
    /// Go live. Called once, when the scheduler starts scheduling.
    pub(crate) fn arm(&mut self) {
        self.armed = true;
    }

    /// Begin a scheduler pass. Every level published until the next call is
    /// stamped with the same revision.
    pub(crate) fn begin_pass(&mut self) {
        self.rev += 1;
    }

    /// Publish one process's level. Returns whether the *level* changed —
    /// the revision always advances, so holders can always tell freshness.
    pub(crate) fn set(&mut self, name: &str, level: Gate) -> bool {
        if !self.armed {
            return false;
        }
        let Some(tx) = self.txs.get(name) else {
            return false;
        };
        let changed = tx.borrow().level != level;
        tx.send_replace(Grant {
            level,
            rev: self.rev,
        });
        changed
    }
}

/// The read half, one per process.
///
/// A level read is correct after missing any number of changes, which is the
/// point: a supervisor busy through a docker pull or a build sees the current
/// answer when it finishes, not a notification it slept through.
#[derive(Debug)]
pub(crate) struct GateReader {
    rx: watch::Receiver<Grant>,
}

impl GateReader {
    /// The current grant: what dependencies allow, and when that was decided.
    pub(crate) fn get(&self) -> Grant {
        *self.rx.borrow()
    }

    /// The revision now, for stamping demand as it arises.
    pub(crate) fn rev(&self) -> u64 {
        self.rx.borrow().rev
    }

    /// Wait for the next change. Cancel-safe, so it can be a `select!` arm.
    ///
    /// `None` once the scheduler is gone — treat that as "blocked forever",
    /// not as an error, and stop selecting on this gate. A `watch::Receiver`
    /// whose sender has dropped returns `Err` immediately and *forever*, so a
    /// caller that keeps polling spins at 100% CPU.
    pub(crate) async fn changed(&mut self) -> Option<()> {
        self.rx.changed().await.ok()
    }
}

/// Create the writer plus one reader per process.
///
/// The name set is fixed here, for the same reason
/// [`crate::process::registry::ProcessRegistry`]'s is: the process set is
/// decided at construction, so there is nothing to synchronise.
pub(crate) fn channel<'a>(
    names: impl Iterator<Item = &'a String>,
) -> (GateWriter, HashMap<String, GateReader>) {
    let mut txs = HashMap::new();
    let mut readers = HashMap::new();
    for name in names {
        let (tx, rx) = watch::channel(Grant {
            level: Gate::Blocked,
            rev: 0,
        });
        txs.insert(name.clone(), tx);
        readers.insert(name.clone(), GateReader { rx });
    }
    (
        GateWriter {
            txs,
            rev: 0,
            armed: false,
        },
        readers,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn one(name: &str) -> (GateWriter, GateReader) {
        let names = [name.to_string()];
        let (mut writer, mut readers) = channel(names.iter());
        writer.arm();
        let reader = readers.remove(name).unwrap();
        (writer, reader)
    }

    fn publish(writer: &mut GateWriter, name: &str, level: Gate) {
        writer.begin_pass();
        writer.set(name, level);
    }

    #[test]
    fn nothing_is_permitted_before_the_scheduler_arms() {
        let names = ["api".to_string()];
        let (mut writer, mut readers) = channel(names.iter());
        let reader = readers.remove("api").unwrap();
        publish(&mut writer, "api", Gate::Open);
        assert_eq!(reader.get().level, Gate::Blocked);

        writer.arm();
        publish(&mut writer, "api", Gate::Open);
        assert_eq!(reader.get().level, Gate::Open);
    }

    #[test]
    fn a_level_survives_not_being_looked_at() {
        // The point of a level: a supervisor busy through the changes sees
        // the current answer when it finishes, not a missed notification.
        let (mut writer, reader) = one("api");
        publish(&mut writer, "api", Gate::Open);
        publish(&mut writer, "api", Gate::Blocked);
        publish(&mut writer, "api", Gate::Degraded);
        assert_eq!(reader.get().level, Gate::Degraded);
    }

    #[test]
    fn set_reports_only_real_level_changes() {
        let (mut writer, _reader) = one("api");
        writer.begin_pass();
        assert!(writer.set("api", Gate::Open));
        writer.begin_pass();
        assert!(
            !writer.set("api", Gate::Open),
            "republishing is not a change"
        );
        writer.begin_pass();
        assert!(writer.set("api", Gate::Degraded));
        assert!(
            !writer.set("ghost", Gate::Open),
            "unknown names change nothing"
        );
    }

    /// The revision is what stops a supervisor acting on a level that was
    /// decided before its demand existed — the race that splitting demand
    /// from permission introduces. See the module doc.
    #[test]
    fn a_republished_level_still_advances_the_revision() {
        let (mut writer, reader) = one("api");
        publish(&mut writer, "api", Gate::Open);
        let first = reader.rev();

        // Same level, new pass: a holder whose demand arose during the first
        // pass must be able to tell this one is newer.
        publish(&mut writer, "api", Gate::Open);
        assert!(
            reader.rev() > first,
            "an unchanged level must still carry a fresh revision"
        );
    }

    #[test]
    fn one_pass_stamps_every_process_alike() {
        let names = ["api".to_string(), "db".to_string()];
        let (mut writer, mut readers) = channel(names.iter());
        writer.arm();
        let api = readers.remove("api").unwrap();
        let db = readers.remove("db").unwrap();

        writer.begin_pass();
        writer.set("api", Gate::Open);
        writer.set("db", Gate::Degraded);
        assert_eq!(api.get().rev, db.get().rev, "one pass, one revision");
    }

    /// The ordering is the permission rule: a scheduled start needs `Open`, an
    /// explicitly requested one is content with `Degraded`.
    #[test]
    fn levels_are_ordered_by_how_much_they_permit() {
        assert!(Gate::Blocked < Gate::Degraded);
        assert!(Gate::Degraded < Gate::Open);
    }

    #[tokio::test]
    async fn a_dropped_scheduler_ends_the_wait_rather_than_spinning() {
        let (writer, mut reader) = one("api");
        drop(writer);
        assert_eq!(reader.changed().await, None);
    }
}
