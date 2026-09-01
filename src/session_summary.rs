//! The runner's last word: what it was running, and for how long.
//!
//! A TUI session lives on the alternate screen, so when don exits the screen
//! is handed back and the logs, the status pane and every lifecycle event go
//! with it — the user is returned to a bare prompt with nothing to say what
//! just happened. This is the one line that survives.
//!
//! It is produced by the runner rather than by whoever happens to be printing
//! it, because the runner is the only party that knows both halves. A client
//! that attached partway through knows when *it* connected, not when the stack
//! came up, and a wrong uptime is worse than no uptime.

use std::fmt;
use std::time::Duration;

/// How a run ended, carried on [`crate::runner::RunnerEvent::ShutdownComplete`].
///
/// The counts are the run's process set — what the runner was managing, which
/// is what the status pane listed a moment ago — not a tally of what finished
/// successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionSummary {
    /// Services in the run's process set.
    pub services: usize,
    /// Tasks in the run's process set.
    pub tasks: usize,
    /// How long the runner was up, in whole seconds.
    pub elapsed_secs: u64,
}

impl SessionSummary {
    /// Build a summary from a run's process counts and elapsed time.
    pub fn new(services: usize, tasks: usize, elapsed: Duration) -> Self {
        Self {
            services,
            tasks,
            elapsed_secs: elapsed.as_secs(),
        }
    }
}

/// Renders as `stopped — 6 services, 3 tasks, 12m 04s`, with no `[don]`
/// prefix: the lifecycle emitter and the CLI each add their own framing.
impl fmt::Display for SessionSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = Vec::new();
        if self.services > 0 {
            parts.push(pluralize(self.services, "service"));
        }
        if self.tasks > 0 {
            parts.push(pluralize(self.tasks, "task"));
        }
        parts.push(format_elapsed(self.elapsed_secs));
        write!(f, "stopped — {}", parts.join(", "))
    }
}

fn pluralize(n: usize, noun: &str) -> String {
    if n == 1 {
        format!("1 {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

/// Coarse by design: the reader wants "roughly how long was that", not a
/// stopwatch reading. Seconds are zero-padded once minutes are present so the
/// two fields keep their widths and don't visually swap places.
fn format_elapsed(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3_600 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h {:02}m", secs / 3_600, (secs % 3_600) / 60)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_stopped_line() {
        struct Case {
            name: &'static str,
            services: usize,
            tasks: usize,
            secs: u64,
            want: &'static str,
        }

        let cases = vec![
            Case {
                name: "the everyday shape",
                services: 6,
                tasks: 3,
                secs: 12 * 60 + 4,
                want: "stopped — 6 services, 3 tasks, 12m 04s",
            },
            Case {
                name: "singulars are not '1 services'",
                services: 1,
                tasks: 1,
                secs: 9,
                want: "stopped — 1 service, 1 task, 9s",
            },
            Case {
                name: "a stack with no tasks doesn't say '0 tasks'",
                services: 4,
                tasks: 0,
                secs: 90,
                want: "stopped — 4 services, 1m 30s",
            },
            Case {
                name: "a task-only run doesn't say '0 services'",
                services: 0,
                tasks: 2,
                secs: 3,
                want: "stopped — 2 tasks, 3s",
            },
            Case {
                name: "an empty run still reports its duration",
                services: 0,
                tasks: 0,
                secs: 1,
                want: "stopped — 1s",
            },
        ];

        for case in cases {
            let got = SessionSummary {
                services: case.services,
                tasks: case.tasks,
                elapsed_secs: case.secs,
            }
            .to_string();
            assert_eq!(got, case.want, "{}", case.name);
        }
    }

    #[test]
    fn formats_elapsed_time() {
        struct Case {
            name: &'static str,
            secs: u64,
            want: &'static str,
        }

        let cases = vec![
            Case {
                name: "an instant run still reads as a duration",
                secs: 0,
                want: "0s",
            },
            Case {
                name: "seconds below a minute are bare",
                secs: 42,
                want: "42s",
            },
            Case {
                name: "exactly a minute crosses over",
                secs: 60,
                want: "1m 00s",
            },
            Case {
                name: "seconds are zero-padded beside minutes",
                secs: 12 * 60 + 4,
                want: "12m 04s",
            },
            Case {
                name: "the last second before an hour",
                secs: 3_599,
                want: "59m 59s",
            },
            Case {
                name: "hours drop seconds rather than grow a third field",
                secs: 3_600,
                want: "1h 00m",
            },
            Case {
                name: "a long afternoon",
                secs: 5 * 3_600 + 7 * 60 + 30,
                want: "5h 07m",
            },
        ];

        for case in cases {
            assert_eq!(format_elapsed(case.secs), case.want, "{}", case.name);
        }
    }

    /// The summary crosses the socket to attached clients, so a client built
    /// against a different point release still has to read it.
    #[test]
    fn round_trips_over_the_wire() {
        let summary = SessionSummary::new(6, 3, Duration::from_secs(724));
        let json = serde_json::to_string(&summary).unwrap();
        let back: SessionSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back, summary);
        assert_eq!(back.to_string(), "stopped — 6 services, 3 tasks, 12m 04s");
    }
}
