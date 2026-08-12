# Who owns what

Don is four cooperating actors, plus one per process for its output. This
document says what each one owns, what it may never do, and what the signals
between them are. It exists because the
boundaries erode in one specific way: someone needs a fact, notices another
component already has it, and reads it from there — and a year later nobody
can answer "why did X restart?" without reading every module.

> **The invariant, stated once:**
> **Commands flow down. Reports flow up. Peers never command peers.**
>
> Addressed commands into a process come only from the scheduler and the edges
> (API, TUI, watcher), via `ProcessRegistry` — so "who can stop a service?"
> stays a two-line grep. Processes report transitions; anything that cares
> *observes*. A supervisor never holds another process's handle.

## The actors

| Actor | Owns | Must never |
|---|---|---|
| **Scheduler** (`src/runner/`) | dependency graph, gate levels, the fold over reports, the state/endpoint/ports projections, shutdown order | hold a pid, a handle or a port binding; decide when a process restarts |
| **Supervisor** (`src/process/`) | one process end to end: prepare, spawn, ready check, health monitor, restart policy, its proxy, its rebuild cycle, the signal that ends it | command a peer; import anything from `runner` |
| **Build manager** (`src/build_tool/`) | *every* build — the first as much as a rebuild: coalescing across processes, workspace grouping, the bazel mutex, watch-path and binary-path resolution and the registrations they produce, the mtime scan that catches an edit made mid-build | know about dependencies or lifecycle state |
| **Watcher** (`src/watch/`) | file watching, debounce, watch registrations | decide what a change *means*, or track what it started |

A fifth actor is per-process but not a supervisor: **`src/output/actor.rs`** owns
one process's ring buffer, sink list and attach registration. It is what makes
"first attaching client mutes stdout" a single message rather than a comment
about which mutations must share a lock.

Two of these edges are enforced by `tests/module_edges_test.rs` rather than by
convention: `src/process/` must not reference `crate::runner`, and neither
must `src/tui/`. Both erode the same way — by reaching for a runner type that
already has the fact you want.

## Signals

```mermaid
flowchart TD
    subgraph SCHED["Scheduler — src/runner/"]
        G["dependency graph → gate levels"]
        F["fold ProcessReports"]
        P["projections:<br/>state · endpoints · ports.json"]
        T["shutdown, reverse dep order"]
    end

    subgraph SUP["Supervisors — src/process/, one per process"]
        S["prepare · spawn · ready · health<br/>restart policy · proxy · rebuild cycle"]
    end

    BM["Build manager<br/>src/build_tool/"]
    W["Watcher — src/watch/"]
    E["Edges: API · TUI · web"]

    G -->|"Gate{Blocked / Degraded / Open}"| S
    E -->|"Start · Stop · Restart · RunTask"| SCHED
    SCHED -->|"ServiceCommand / TaskCommand<br/>via ProcessRegistry"| S
    S -->|"ProcessReport<br/>Starting · Prepared · Ready · Exited<br/>StopComplete · Demand · HealthChanged<br/>ArtifactBuild"| F
    F --> G
    F --> P
    P -->|"snapshot · RunnerEvent"| E
    T -->|"Stop"| S

    S -->|"QueuePrepare / QueueRebuild<br/>ForceRebuild / QueueRequery"| BM
    BM -->|"PrepareOutcome · RebuildItemOutcome<br/>RequeryOutcome, per item"| S
    BM -->|"WatchUpdate"| W
    W -->|"Rebuild · Rerun · BuildGraphChanged<br/>via ProcessRegistry"| S
```

The watcher is an **edge**, not a stage: it addresses supervisors through the
same registry the API and the TUI use. It used to send its signals to the
scheduler, which looked the name up and forwarded them to the very same
mailbox. Nothing was decided on the way through.

Nothing comes back from the build manager to the scheduler either. Every kind
of batch — preparation, rebuild, re-query — is cross-item to *run* and
per-item in its *consequences*, so all three fan out to the supervisor that
asked.

Reports travel on one **lossless unbounded mpsc** to the scheduler, not on the
broadcast: the scheduler must never lag its own inputs. The broadcast is for
peers and edges, which resync from the snapshot when they lag.

## A start, end to end

```mermaid
sequenceDiagram
    participant Sch as Scheduler
    participant Sup as Supervisor
    participant Proc as Process

    Note over Sch: dependencies satisfied
    Sch->>Sup: Gate = Open{rev}
    Note over Sup: spends its one-shot demand<br/>(a level alone would double-start)
    Sup->>Sch: ServiceStarting
    Note over Sch: → Starting (closes the gate)
    Sup->>Proc: spawn
    Sup->>Sch: ServiceStartPrepared{wired: pid, ports}
    Note over Sch: → Running; custody funnel writes<br/>the snapshot, endpoints, ports.json
    Sup->>Sup: ready check, against its own proxy/docker state
    Sup->>Sch: ServiceReady{success}
    Note over Sch: → Ready
```

The scheduler says *when*, never *how*. It learns the pid from the report and
writes it straight into the projection — it keeps no copy of its own.

## A crash

```mermaid
sequenceDiagram
    participant Sup as Supervisor
    participant Pol as RestartPolicy
    participant Sch as Scheduler

    Note over Sup: output stream EOF — the process died
    Sup->>Sup: reap, narrate "exited unexpectedly…"
    Sup->>Pol: decide(Crash{lived, reached_ready})
    Pol-->>Sup: RestartScheduled{attempt, backoff} | GaveUp… | LazyRearm
    Note over Sup: arms its own backoff timer<br/>(a select arm, not a detached task)
    Sup->>Sch: ServiceExited{status, policy}
    Note over Sch: → Failed; records restart_pending<br/>so a stack in backoff is not "settled"
    Note over Sup: backoff fires → internal Restart
```

Whether a service starts again is the supervisor's answer, because every input
the question needs — did the prepare fail, did the ready check pass, how long
did this spawn live — is something it observed itself. The scheduler folds
enough to keep its own view honest and nothing more.

Two rules the crash path depends on:

- **Demand is one-shot.** A gate level alone double-starts: gate opens,
  process dies instantly, gate is still open, idle supervisor starts again at
  zero backoff. Demand is cleared in the same synchronous step that begins the
  start. A restart the policy scheduled arrives as its own mailbox command and
  does not consult demand — *a crash is not a request*.
- **The reset flag is load-bearing.** An explicit stop or restart clears the
  failure history; the policy's own retry must not, or the streak that bounds
  a crash loop is wiped by every attempt it schedules.

## Ending a process

The same rule as starting one: the scheduler says *when*, in reverse
dependency order, and the supervisor holding the process does it. That
includes the escalation — a second Ctrl+C is published as a watch that every
in-flight stop is racing against, so each supervisor cuts its own grace period
short and kills what it owns. The scheduler's whole contribution is one
"forcing immediate shutdown" line.

This is why `src/runner/` contains no `killpg`, and why the custody it reads
during teardown is a set of *names*.

## Rebuilding

A watched file changing means "these paths moved", and nothing more. The
watcher debounces, says so, and forgets it — it has no idea whether a rebuild
followed, whether one was already running, or how it went.

The supervisor owns the cycle end to end, which is what lets it answer the
question the watcher used to: a change arriving while a cycle is running does
not start a second one, it marks the artifact that cycle is about to produce
already stale, and the cycle queues its own follow-up when it ends. That
distinction is `RebuildSource` — a change coalesces, a rebuild asked for by
name supersedes.

> **Nothing reports that a rebuild finished.** There is no completion event,
> because there is no longer anyone waiting to hear about one. `RebuildComplete`,
> `TaskRerunComplete`, `RebuildCycleDone` and the watcher's own `Rebuilding`
> state all existed to close a loop that the cycle's owner now closes itself.

Task re-runs are the same shape. Whether a watched change means a run depends
on `auto_run`, on whether the task declares params a file change cannot
supply, and on whether its artifact is still building — three facts the task's
own supervisor has.

## Building

Startup, lazy JIT and rebuild are one sentence: *a supervisor that needs an
artifact asks the build manager for one.*

```mermaid
flowchart LR
    G["Scheduler<br/>gate = dependencies only"] -->|"Gate"| S
    S["Supervisor"] -->|"QueuePrepare / QueueRebuild{spec}<br/>QueueRequery{spec}"| M["Build manager<br/>coalesce · group · mutex"]
    M -->|"per-item outcome:<br/>Ready{binary_path} · Stale · Failed<br/>Built · UpToDate · Updated"| S
    M -->|"WatchUpdate"| W["Watcher"]
```

The scheduler does not appear in it. It learns that a build is running the
way it learns everything else — from a report — and folds it into `Building`
so `don status` can say so, so `initial_startup_settled` stays open while one
runs, and so a rebuild requested mid-build is deferred rather than raced. It
never decides that a build should happen, and the gate means only what
`src/gate.rs` says it means.

The load-bearing detail:

> **Dependencies gate *running*, not *building*.**

An artifact can be built before its dependencies are up — bazel does not care
whether postgres is listening. So a supervisor requests its build **when it is
constructed**, not when its gate opens. Every supervisor asks at once, the
debounce window coalesces them, and bazel gets one invocation for the whole
workspace. Building at gate-open would serialise builds along the dependency
chain, which is the one real regression this ordering avoids by construction.
Lazy services are the exception for the reason that makes the rule: nothing
wants one yet, so its supervisor asks on first demand.

One request is late by construction, and it is why nothing builds the instant
`Runner::new` returns. Watch paths are resolved *by* these builds and have to
reach the watcher before anything spawns — but the watcher does not exist
until the runner has set it up. The build manager parks preparation requests
until the runner says `WatchReady`, which is also what guarantees the whole
startup burst leaves as one batch. A supervisor then waits for its outcome
before spawning, so the registrations are always in place first.

### Two rules that come with it

**Build failures do not retry; runtime failures do.** Retrying a compile that
just failed recompiles the same broken code. The restart policy exists for
crashes, unhealthy probes and ready-check failures — things where waiting can
plausibly change the answer. A supervisor whose build fails withdraws its own
demand, so the failure never reaches `RestartPolicy` at all; only an explicit
request by name starts that process again.

**The startup mtime scan survives.** `run_batch_build_chain` stamps a
timestamp before building and afterwards checks whether any watched source or
BUILD file is newer, reporting `PrepareOutcome::Stale` — "you edited a file
while the build was running, so build again before starting". That looks like
the rebuild cycle's staleness flag but cannot be merged with it: the cycle
learns staleness from the *watcher*, and during this build nothing is watching
those paths yet, because the watch paths are resolved **by that build**. The
mtime scan is the only thing covering that bootstrap window. It lives in the
build manager with the rest of the chain, and the supervisor answers it by
asking again.

**A re-query is a build too.** A changed BUILD file means the watch patterns
resolved from it may be wrong, so the supervisor asks for a re-query the same
way it asks for anything else. The manager coalesces them, registers the paths
it resolves — it resolved them, so it delivers them — and answers each
supervisor with the one part that needs lifecycle: whether the process running
now was built from a graph that has since moved.

## Reading state

A status query must never queue behind whatever the scheduler is doing, so
reads go through projections rather than the command channel:

- **`state_store.rs`** — every process's `ProcessStatus`, plus
  `startup_complete`. `StateWriter` is not `Clone` and lives on the scheduler;
  `StateReader` is cloneable and read-only. For runtime detail the snapshot
  *is* the record, not a copy of one — pid, docker-ness and port mappings live
  only in `ProcessStatus::Service.runtime`.
- **`endpoints.rs`** — where every service can be reached. Supervisors render
  their own `$(peer.KEY)` env from it at start time, which is why a peer that
  moved is picked up without the request being reissued.
- **`gate.rs`** — per-process permission to run.
- **`output/actor.rs`** — one process's buffered output, its sinks and its
  attach registration, answered by that process's own output actor.
  `output/attach.rs` and `watch/report.rs` are the read paths onto it and onto
  the watcher.

The state store and the `RunnerEvent` broadcast update on exactly the same
transitions, which is what lets a consumer that missed an event resync from
the snapshot and get a consistent answer.
