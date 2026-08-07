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
//! the process dies instantly, the supervisor reaps and reports — and the
//! runner has not folded that report yet, so the gate is *still* open. The
//! supervisor is idle again, sees permission, and starts a second time,
//! ignoring the service's `on_failure` policy.
//!
//! So each level carries a ticket. [`Gate::Open`] has an `epoch`, minted only
//! on a `Blocked -> Open` edge, and a supervisor spends an epoch at most once
//! ([`GateReader::take`]). Republishing an open gate keeps its epoch, so it is
//! idempotent; a genuine re-permission bumps it. This is the same shape as the
//! `lazy_build_token` and `control_generation` guards already in the codebase.
//!
//! # The invariant that makes a permit safe to hold
//!
//! **`Open` implies the process is `Pending`.** Everything that would
//! invalidate a permission — starting, stopping, failing, a build starting —
//! also moves the process out of `Pending`, which closes the gate. That is
//! what lets a supervisor act on a permission it read a moment ago without
//! re-validating anything.
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

/// One process's permission to run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    /// Not permitted: dependencies unmet, a build in flight, the user stopped
    /// it, or teardown has begun.
    Blocked,
    /// Permitted. `epoch` identifies *this* grant; a holder spends it once.
    Open { epoch: u64 },
}

impl Gate {
    /// The epoch of an open gate.
    pub(crate) fn epoch(self) -> Option<u64> {
        match self {
            Self::Open { epoch } => Some(epoch),
            Self::Blocked => None,
        }
    }
}

/// The write half: the scheduler's answer for every process.
///
/// Not `Clone` — it is moved into the runner at construction, so no other
/// component can grant permission.
pub(crate) struct GateWriter {
    txs: HashMap<String, watch::Sender<Gate>>,
    /// Last epoch handed out, per process. Bumped only on a `Blocked -> Open`
    /// edge, which is what makes a republished grant idempotent.
    epochs: HashMap<String, u64>,
    /// Publishing is a no-op until armed. Transitions during construction and
    /// setup therefore cannot grant permission.
    armed: bool,
}

impl GateWriter {
    /// Go live. Called once, when the scheduler starts scheduling.
    pub(crate) fn arm(&mut self) {
        self.armed = true;
    }

    /// Publish one process's permission.
    ///
    /// Opening an already-open gate is a no-op: the epoch is preserved, so a
    /// supervisor that has already spent it does not start again. Returns
    /// whether this call *newly* granted permission, so a caller can hang
    /// edge-triggered work off the grant.
    pub(crate) fn set(&mut self, name: &str, allow: bool) -> bool {
        if !self.armed {
            return false;
        }
        let Some(tx) = self.txs.get(name) else {
            return false;
        };
        let current = *tx.borrow();
        match (current, allow) {
            (Gate::Open { .. }, true) | (Gate::Blocked, false) => false,
            (Gate::Blocked, true) => {
                let epoch = self.epochs.entry(name.to_string()).or_default();
                *epoch += 1;
                tx.send_replace(Gate::Open { epoch: *epoch });
                true
            }
            (Gate::Open { .. }, false) => {
                tx.send_replace(Gate::Blocked);
                false
            }
        }
    }

    /// Block every gate. The first act of teardown, so a permission published
    /// a moment ago cannot be spent into a process nobody will stop.
    pub(crate) fn block_all(&mut self) {
        // Deliberately ignores `armed`: shutdown must be able to revoke
        // permissions granted before it started.
        for tx in self.txs.values() {
            if matches!(*tx.borrow(), Gate::Open { .. }) {
                tx.send_replace(Gate::Blocked);
            }
        }
    }
}

/// The read half, one per process.
#[derive(Debug)]
pub(crate) struct GateReader {
    rx: watch::Receiver<Gate>,
    /// The last epoch this reader spent. The anti-double-start half of the
    /// level/ticket pair.
    spent: Option<u64>,
}

impl GateReader {
    /// The current permission — a *level* read, correct after missing any
    /// number of changes.
    pub(crate) fn get(&self) -> Gate {
        *self.rx.borrow()
    }

    /// Spend the current permission, if there is an unspent one.
    ///
    /// Returns the epoch on the first call per grant and `None` after, so a
    /// republished level cannot start a second process.
    pub(crate) fn take(&mut self) -> Option<u64> {
        let epoch = self.get().epoch()?;
        if self.spent == Some(epoch) {
            return None;
        }
        self.spent = Some(epoch);
        Some(epoch)
    }

    /// Treat the current grant as spent without acting on it.
    ///
    /// Used when a supervisor takes a job from its mailbox instead: a stop
    /// must not be immediately undone by a stale open gate.
    pub(crate) fn burn(&mut self) {
        if let Some(epoch) = self.get().epoch() {
            self.spent = Some(epoch);
        }
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
        let (tx, rx) = watch::channel(Gate::Blocked);
        txs.insert(name.clone(), tx);
        readers.insert(name.clone(), GateReader { rx, spent: None });
    }
    (
        GateWriter {
            txs,
            epochs: HashMap::new(),
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

    #[test]
    fn nothing_is_permitted_before_the_scheduler_arms() {
        let names = ["api".to_string()];
        let (mut writer, mut readers) = channel(names.iter());
        let mut reader = readers.remove("api").unwrap();
        writer.set("api", true);
        assert_eq!(reader.get(), Gate::Blocked);
        assert_eq!(reader.take(), None);

        // …and arming does not retroactively grant anything; the next
        // decision does.
        writer.arm();
        assert_eq!(reader.get(), Gate::Blocked);
        writer.set("api", true);
        assert!(reader.take().is_some());
    }

    #[test]
    fn a_grant_survives_not_being_looked_at() {
        // The whole point of a level: a supervisor busy through the grant
        // still sees it when it finishes.
        let (mut writer, mut reader) = one("api");
        writer.set("api", true);
        writer.set("api", true);
        writer.set("api", true);
        assert_eq!(reader.take(), Some(1), "one grant, one epoch");
    }

    #[test]
    fn a_grant_is_spendable_once() {
        struct Case {
            name: &'static str,
            /// Applied after the first spend.
            republish: bool,
            reblock: bool,
            want_second: Option<u64>,
        }

        let cases = vec![
            Case {
                name: "republishing the same permission does not re-grant",
                republish: true,
                reblock: false,
                want_second: None,
            },
            Case {
                name: "no further publish, no further grant",
                republish: false,
                reblock: false,
                want_second: None,
            },
            Case {
                name: "blocked then reopened is a genuinely new grant",
                republish: true,
                reblock: true,
                want_second: Some(2),
            },
        ];

        for case in cases {
            let (mut writer, mut reader) = one("api");
            writer.set("api", true);
            assert_eq!(reader.take(), Some(1), "{}: first spend", case.name);

            if case.reblock {
                writer.set("api", false);
            }
            if case.republish {
                writer.set("api", true);
            }
            assert_eq!(
                reader.take(),
                case.want_second,
                "{}: second spend",
                case.name
            );
        }
    }

    #[test]
    fn set_reports_only_the_granting_edge() {
        let (mut writer, _reader) = one("api");
        assert!(writer.set("api", true), "blocked -> open is the grant");
        assert!(!writer.set("api", true), "republishing is not a new grant");
        assert!(!writer.set("api", false), "revoking is not a grant");
        assert!(writer.set("api", true), "reopening is a new grant");
        assert!(!writer.set("ghost", true), "unknown names grant nothing");
    }

    #[test]
    fn burning_a_grant_stops_it_being_spent() {
        // A stop taken from the mailbox must not be undone by the open gate
        // that was published just before it.
        let (mut writer, mut reader) = one("api");
        writer.set("api", true);
        reader.burn();
        assert_eq!(reader.take(), None);

        // A later, genuine re-grant is still spendable.
        writer.set("api", false);
        writer.set("api", true);
        assert_eq!(reader.take(), Some(2));
    }

    #[test]
    fn shutdown_revokes_permission_it_did_not_grant() {
        let (mut writer, mut reader) = one("api");
        writer.set("api", true);
        writer.block_all();
        assert_eq!(reader.get(), Gate::Blocked);
        assert_eq!(reader.take(), None);
    }

    #[tokio::test]
    async fn a_dropped_scheduler_ends_the_wait_rather_than_spinning() {
        let (writer, mut reader) = one("api");
        drop(writer);
        assert_eq!(reader.changed().await, None);
    }
}
