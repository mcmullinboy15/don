//! Permission to run: what a process's dependencies allow.
//!
//! [`level`] is the whole of it — a pure function from this process's
//! `depends_on` list and a [`FactsSnapshot`] to how far it may go. Each
//! supervisor calls it against its own dependencies and starts itself when it
//! is both permitted and idle. Nothing publishes permission, because nothing
//! knows anything a supervisor cannot read for itself.
//!
//! # Why it reads a snapshot rather than each peer
//!
//! `api` depending on `db` and `cache` must see both as of one instant, or it
//! can act on `db` as it was a moment ago next to `cache` as it is now. The
//! merge in [`crate::facts`] is what provides that instant; this function just
//! reads it.
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
//! ignoring `on_failure` and the crash ceiling.
//!
//! Those now live *in* the supervisor
//! ([`crate::process::health::RestartPolicy`]), beside the loop that would do
//! the relaunching — so one-shot demand is no longer the only thing standing
//! between a crash and a tight loop. It is still what expresses the
//! distinction the policy depends on: **a crash is not a request**. A restart
//! the policy scheduled arrives as its own mailbox command, which is why it
//! does not consult demand at all.
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

use crate::config::Dependency;
use crate::facts::FactsSnapshot;

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
    /// end, so a process someone asked for *by name* may proceed; a start the
    /// graph would have made on its own may not.
    Degraded,
    /// Every dependency is satisfied.
    Open,
}

/// Whether one `depends_on` edge still blocks its dependent.
///
/// A blocking edge opens only when the dependency is satisfied. A non-blocking
/// edge is ordering-only: it also opens once the dependency has settled into a
/// failed or stopped state, so the dependent still starts *after* it, but is
/// not held hostage by it.
fn edge_open(dep: &Dependency, snapshot: &FactsSnapshot) -> bool {
    snapshot.satisfied(&dep.name) || (!dep.blocking && snapshot.settled(&dep.name))
}

/// How far `deps` let a process go, as of `snapshot`.
///
/// Three-valued because "may I run?" has two different answers depending on
/// who is asking. A process the graph brings up starts only when everything it
/// needs is actually *up*; a user who names one explicitly is willing to
/// proceed past a dependency that has stopped making progress, because waiting
/// for it would never end. See [`crate::process::Demand::permitted_by`].
///
/// Reads only the *dependencies'* facts — never the asking process's own.
/// That is what keeps every influence edge a dependency edge, and the
/// dependency graph is a validated DAG.
pub(crate) fn level(deps: &[Dependency], snapshot: &FactsSnapshot) -> Gate {
    if deps.iter().all(|dep| edge_open(dep, snapshot)) {
        return Gate::Open;
    }
    // Not all satisfied. If every unsatisfied one has *settled*, waiting will
    // not help — an explicit request may still proceed.
    if deps
        .iter()
        .all(|dep| snapshot.satisfied(&dep.name) || snapshot.settled(&dep.name))
    {
        return Gate::Degraded;
    }
    Gate::Blocked
}

/// The root failures these dependencies strand their dependent behind.
///
/// Resolved transitively without walking the graph: a stranded dependency has
/// already inherited *its* dependencies' roots, so reading one hop reads the
/// whole chain. `api -> worker -> db` reports `db`.
///
/// Non-blocking edges are ignored: their whole point is that a failure on the
/// other end must not cascade.
pub(crate) fn failed_roots(deps: &[Dependency], snapshot: &FactsSnapshot) -> Vec<String> {
    let mut roots: Vec<String> = Vec::new();
    for dep in deps.iter().filter(|dep| dep.blocking) {
        let inherited = snapshot.failed_roots(&dep.name);
        // A root failure reports itself; anything that inherited roots
        // contributes those instead, which is what collapses the chain.
        let contributed = if inherited.is_empty() {
            continue;
        } else {
            inherited
        };
        for root in contributed {
            if !roots.iter().any(|existing| existing == root) {
                roots.push(root.clone());
            }
        }
    }
    roots
}

/// The non-blocking dependencies a process is deliberately not waiting for.
///
/// Reported when it starts anyway, so a start that follows a visible failure
/// doesn't look like don ignored the graph.
pub(crate) fn skipped_non_blocking(deps: &[Dependency], snapshot: &FactsSnapshot) -> Vec<String> {
    deps.iter()
        .filter(|dep| !dep.blocking)
        .filter(|dep| !snapshot.satisfied(&dep.name) && snapshot.settled(&dep.name))
        .map(|dep| dep.name.clone())
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::facts::ProcessFacts;
    use crate::process::ServiceState;

    /// A dependency's facts, reduced to the two booleans the pure functions
    /// actually read.
    fn facts(satisfied: bool, settled: bool, roots: &[&str]) -> ProcessFacts {
        ProcessFacts {
            satisfied,
            settled,
            failed_roots: roots.iter().map(|r| (*r).to_string()).collect(),
            ..ProcessFacts::for_service("dep", ServiceState::Pending, None, Vec::new())
        }
    }

    fn snapshot(entries: &[(&str, ProcessFacts)]) -> FactsSnapshot {
        FactsSnapshot::from_pairs(
            entries
                .iter()
                .map(|(name, facts)| ((*name).to_string(), facts.clone())),
        )
    }

    fn dep(name: &str, blocking: bool) -> Dependency {
        Dependency {
            name: name.to_string(),
            blocking,
        }
    }

    #[test]
    fn dependency_levels_table() {
        struct Case {
            name: &'static str,
            deps: Vec<Dependency>,
            world: Vec<(&'static str, ProcessFacts)>,
            want: Gate,
        }

        let up = facts(true, false, &[]);
        let coming_up = facts(false, false, &[]);
        let dead = facts(false, true, &["db"]);

        let cases = vec![
            Case {
                name: "no dependencies is always open",
                deps: vec![],
                world: vec![],
                want: Gate::Open,
            },
            Case {
                name: "every blocking dependency satisfied",
                deps: vec![dep("db", true), dep("cache", true)],
                world: vec![("db", up.clone()), ("cache", up.clone())],
                want: Gate::Open,
            },
            Case {
                name: "one dependency still coming up blocks",
                deps: vec![dep("db", true), dep("cache", true)],
                world: vec![("db", up.clone()), ("cache", coming_up.clone())],
                want: Gate::Blocked,
            },
            Case {
                name: "a settled blocking dependency degrades rather than blocks",
                deps: vec![dep("db", true)],
                world: vec![("db", dead.clone())],
                want: Gate::Degraded,
            },
            Case {
                name: "a settled non-blocking dependency opens the edge outright",
                deps: vec![dep("db", false)],
                world: vec![("db", dead.clone())],
                want: Gate::Open,
            },
            Case {
                name: "a non-blocking dependency still coming up is still worth waiting for",
                deps: vec![dep("db", false)],
                world: vec![("db", coming_up.clone())],
                want: Gate::Blocked,
            },
            Case {
                name: "one settled and one still coming up blocks",
                deps: vec![dep("db", true), dep("cache", true)],
                world: vec![("db", dead.clone()), ("cache", coming_up.clone())],
                want: Gate::Blocked,
            },
            Case {
                name: "a dependency outside the active process set never satisfies",
                deps: vec![dep("ghost", true)],
                world: vec![],
                want: Gate::Blocked,
            },
        ];

        for case in cases {
            let got = level(&case.deps, &snapshot(&case.world));
            assert_eq!(got, case.want, "{}", case.name);
        }
    }

    #[test]
    fn failed_roots_table() {
        struct Case {
            name: &'static str,
            deps: Vec<Dependency>,
            world: Vec<(&'static str, ProcessFacts)>,
            want: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "a healthy dependency contributes nothing",
                deps: vec![dep("db", true)],
                world: vec![("db", facts(true, false, &[]))],
                want: vec![],
            },
            Case {
                name: "a root failure reports itself",
                deps: vec![dep("db", true)],
                world: vec![("db", facts(false, true, &["db"]))],
                want: vec!["db"],
            },
            Case {
                name: "a stranded dependency collapses to the root it inherited",
                deps: vec![dep("worker", true)],
                world: vec![("worker", facts(false, true, &["db"]))],
                want: vec!["db"],
            },
            Case {
                name: "two paths to one root report it once",
                deps: vec![dep("worker", true), dep("api", true)],
                world: vec![
                    ("worker", facts(false, true, &["db"])),
                    ("api", facts(false, true, &["db"])),
                ],
                want: vec!["db"],
            },
            Case {
                name: "a non-blocking edge never cascades",
                deps: vec![dep("metrics", false)],
                world: vec![("metrics", facts(false, true, &["metrics"]))],
                want: vec![],
            },
        ];

        for case in cases {
            let got = failed_roots(&case.deps, &snapshot(&case.world));
            assert_eq!(got, case.want, "{}", case.name);
        }
    }

    #[test]
    fn skipped_non_blocking_names_only_what_was_given_up_on() {
        let world = snapshot(&[
            ("dead", facts(false, true, &["dead"])),
            ("up", facts(true, false, &[])),
            ("coming", facts(false, false, &[])),
        ]);
        let deps = vec![
            dep("dead", false),
            dep("up", false),
            dep("coming", false),
            // A blocking edge is never "skipped" — it was waited for.
            dep("dead", true),
        ];
        assert_eq!(skipped_non_blocking(&deps, &world), ["dead".to_string()]);
    }

    /// The ordering *is* the permission rule: a start the graph makes on its
    /// own needs `Open`, an explicitly requested one is content with
    /// `Degraded`. See [`crate::process::Demand::permitted_by`].
    #[test]
    fn levels_are_ordered_by_how_much_they_permit() {
        assert!(Gate::Blocked < Gate::Degraded);
        assert!(Gate::Degraded < Gate::Open);
    }
}
