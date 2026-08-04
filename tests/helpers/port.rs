//! Collision-free TCP port allocation for tests.
//!
//! Tests need a port number *before* the thing that will bind it exists: don's
//! listenfd proxy, a spawned python listener, an axum file server. So the port
//! has to be handed out and then bound by someone else a moment later, and the
//! gap in between is where the old implementation lost. It bound `127.0.0.1:0`,
//! read the assigned port, dropped the listener, and returned the number —
//! reserving nothing. Two things could then take the port first:
//!
//! * any `bind(0)` on the machine, because the number came out of the kernel's
//!   ephemeral range and went straight back into it;
//! * a second `cargo test` process, which would independently hand out the same
//!   number to one of its own tests.
//!
//! The symptom is rarely a clean "address already in use" at the test level.
//! `Runner::run` pre-binds proxy listeners before it binds the API unix socket,
//! and a proxy bind failure aborts startup with `RunnerError::Config` — so
//! `.don/don.sock` is never created and the test dies on a socket-never-appeared
//! timeout instead, which reads like a hang in the runner.
//!
//! This version closes both holes:
//!
//! 1. Ports come from a band *below* the kernel's ephemeral range, so a
//!    `bind(0)` anywhere on the machine is structurally incapable of being
//!    assigned one. This is the part that matters; avoiding collisions by
//!    probability is not the same thing as making them impossible.
//! 2. Each port is claimed with an exclusive `flock` on a file named after it,
//!    held until the process exits. That makes allocation exclusive across
//!    concurrent test processes as well as within one. `flock` rather than
//!    `O_EXCL` deliberately: the kernel drops the lock when a process dies, so
//!    a crashed or `SIGKILL`ed run leaves an inert file rather than a stale
//!    reservation nobody will ever clear.
//!
//! A residual race remains in theory — an unrelated program could bind the port
//! between our probe and the caller's bind — but it is no longer something the
//! test suite can do to itself.

use nix::fcntl::{Flock, FlockArg};
use std::fs::{File, OpenOptions};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

/// Every port this process has claimed, kept locked until it exits.
///
/// Dropping a `Flock` releases it, so these are deliberately never dropped: a
/// port has to stay reserved for as long as the test run might use it.
static RESERVATIONS: Mutex<Vec<Flock<File>>> = Mutex::new(Vec::new());

/// Rotates the starting point so repeated calls don't re-probe claimed ports.
static NEXT: AtomicU32 = AtomicU32::new(0);

/// How many ports the band spans.
const BAND_LEN: u32 = 8_192;

/// Never hand out anything below this — registered and commonly-squatted
/// service ports live down there.
const BAND_FLOOR: u32 = 10_240;

/// Find a free TCP port, reserved for the lifetime of this process.
///
/// The returned port will not be handed out again by this process or by any
/// other process using this helper, and the kernel will not assign it to a
/// `bind(0)` elsewhere.
pub fn free_port() -> u16 {
    let (start, end) = band();
    let span = end - start;

    // Start somewhere pid-dependent so concurrent test binaries scan disjoint
    // parts of the band instead of all fighting over its first few ports.
    let cursor = NEXT
        .fetch_add(1, Ordering::Relaxed)
        .wrapping_add(std::process::id());

    for i in 0..span {
        // `port` is inside `start..end`, which is inside u16 by construction.
        let port = (start + (cursor.wrapping_add(i) % span)) as u16;
        if let Some(lock) = claim(port) {
            RESERVATIONS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(lock);
            return port;
        }
    }
    panic!("no free port available in {start}..{end}");
}

/// Claim `port` exclusively, or return `None` if it is spoken for.
///
/// The `flock` settles it between test processes; the probe bind confirms that
/// nothing outside the test suite is already listening there.
fn claim(port: u16) -> Option<Flock<File>> {
    let dir = reservation_dir();
    std::fs::create_dir_all(&dir).ok()?;

    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(port.to_string()))
        .ok()?;

    // `flock` is per open-file-description, so a second `open` from this same
    // process conflicts too — that is what stops one run handing out a port
    // it already gave to an earlier test.
    let lock = Flock::lock(file, FlockArg::LockExclusiveNonblock).ok()?;

    // A listener dropped without ever accepting doesn't enter TIME_WAIT, so
    // this probe leaves the port immediately usable by the caller.
    TcpListener::bind(("127.0.0.1", port)).ok()?;

    Some(lock)
}

/// Where the per-port lock files live.
///
/// Namespaced by user because `/tmp` is shared on Linux and the directory would
/// otherwise be owned by whoever ran tests first. The files are empty and there
/// can never be more of them than the band is wide, so they need no cleanup —
/// and must *not* be deleted by a running suite, since live runs hold locks on
/// them.
fn reservation_dir() -> PathBuf {
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("LOGNAME"))
        .unwrap_or_else(|_| "shared".to_string());
    std::env::temp_dir().join(format!("don-test-ports-{user}"))
}

/// The `(start, end)` port band, exclusive of `end`.
fn band() -> (u32, u32) {
    band_for_floor(ephemeral_floor())
}

/// Choose the band, given where the kernel's ephemeral range starts.
///
/// Split out from [`band`] so the decision can be tested without a kernel to
/// lie to. Whatever `floor` says, the result always satisfies
/// `start >= BAND_FLOOR`, `end <= 65536` and `end - start == BAND_LEN` — that
/// invariant is the point of this function.
fn band_for_floor(floor: u32) -> (u32, u32) {
    // Normal case: sit immediately below the ephemeral range, where `bind(0)`
    // structurally cannot reach.
    if floor >= BAND_FLOOR + BAND_LEN {
        return (floor - BAND_LEN, floor);
    }
    // The machine is tuned so low that there is no room for a band beneath the
    // ephemeral range. Handing out ports under `BAND_FLOOR` would trade a
    // `bind(0)` collision for a collision with someone's real service, which is
    // a worse trade — so take the platform default band instead and let the
    // `flock` reservation carry the cross-process guarantee on its own.
    let end = default_ephemeral_floor();
    (end - BAND_LEN, end)
}

/// The lowest port the kernel will assign to `bind(0)`.
///
/// Linux publishes this and some machines are tuned away from the default, so
/// read it where it exists. It's a hint with a fallback, never a hard
/// requirement — the file doesn't exist on macOS.
fn ephemeral_floor() -> u32 {
    std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .ok()
        .and_then(|contents| contents.split_whitespace().next()?.parse::<u32>().ok())
        // A value that isn't a port means the file isn't what we think it is;
        // ignore it rather than derive a band that can't fit in a u16.
        .filter(|floor| *floor <= u16::MAX as u32)
        .unwrap_or_else(default_ephemeral_floor)
}

fn default_ephemeral_floor() -> u32 {
    if cfg!(target_os = "macos") {
        // Darwin's `net.inet.ip.portrange.first`.
        49_152
    } else {
        // Linux's `net.ipv4.ip_local_port_range` default low bound.
        32_768
    }
}

// These run once per integration-test binary, since `helpers` is compiled into
// each one. That's fine — they're pure and instant. Nothing expensive belongs
// here.
#[cfg(test)]
mod tests {
    use super::*;

    /// The fallback band, for cases that can't fit one under the reported floor.
    fn default_band() -> (u32, u32) {
        let end = default_ephemeral_floor();
        (end - BAND_LEN, end)
    }

    #[test]
    fn band_selection() {
        struct Case {
            name: &'static str,
            floor: u32,
            /// `None` means "must fall back to the platform default band".
            expected: Option<(u32, u32)>,
        }

        let cases = vec![
            Case {
                name: "linux default floor: band sits immediately below it",
                floor: 32_768,
                expected: Some((24_576, 32_768)),
            },
            Case {
                name: "macos default floor: band sits immediately below it",
                floor: 49_152,
                expected: Some((40_960, 49_152)),
            },
            Case {
                name: "a tuned-up floor is still honoured",
                floor: 60_000,
                expected: Some((51_808, 60_000)),
            },
            Case {
                name: "floor exactly at the boundary still uses the real floor",
                floor: BAND_FLOOR + BAND_LEN,
                expected: Some((BAND_FLOOR, BAND_FLOOR + BAND_LEN)),
            },
            Case {
                name: "one port below the boundary falls back",
                floor: BAND_FLOOR + BAND_LEN - 1,
                expected: None,
            },
            Case {
                name: "an unusually low floor falls back rather than dipping under BAND_FLOOR",
                floor: 15_000,
                expected: None,
            },
            Case {
                name: "a pathological floor falls back",
                floor: 1_024,
                expected: None,
            },
            Case {
                name: "a nonsense floor of zero falls back",
                floor: 0,
                expected: None,
            },
        ];

        for case in cases {
            let (start, end) = band_for_floor(case.floor);
            assert_eq!(
                (start, end),
                case.expected.unwrap_or_else(default_band),
                "case: {}",
                case.name
            );

            // The invariants that must hold whatever the kernel reports.
            assert!(start >= BAND_FLOOR, "case: {} — start {start}", case.name);
            assert!(end <= 65_536, "case: {} — end {end}", case.name);
            assert_eq!(end - start, BAND_LEN, "case: {}", case.name);
        }
    }

    /// The invariants above, swept across every plausible floor rather than the
    /// handful the table names — including the boundary neighbourhood, where an
    /// off-by-one would otherwise slip through.
    #[test]
    fn band_invariants_hold_for_every_floor() {
        for floor in 0..=u16::MAX as u32 {
            let (start, end) = band_for_floor(floor);
            assert!(start >= BAND_FLOOR, "floor {floor} gave start {start}");
            assert!(end <= 65_536, "floor {floor} gave end {end}");
            assert_eq!(end - start, BAND_LEN, "floor {floor}");
            // When a band fits below the reported floor, we must actually be
            // below it — that's what makes `bind(0)` collisions impossible.
            if floor >= BAND_FLOOR + BAND_LEN {
                assert_eq!(end, floor, "floor {floor} should be used as-is");
            }
        }
    }
}
