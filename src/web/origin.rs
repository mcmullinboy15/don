//! Origin checking for the web UI.
//!
//! The web UI binds a loopback TCP port, which is reachable by anything
//! running on this machine. That's accepted: someone who can run processes
//! here can already do everything don can do, so guarding against them buys
//! nothing.
//!
//! A web page you *visit* is a different matter — its author has no access to
//! your machine at all. Same-origin policy stops that page reading a response
//! from 127.0.0.1, but DNS rebinding is the standard way around it: point
//! `evil.example.com` at 127.0.0.1, and the browser now considers the page
//! same-origin with don, free to read your project paths, logs, and config.
//!
//! The tell is the `Host` header — a rebound request carries the attacker's
//! hostname, not a loopback one. Rejecting those is the whole of this module.
//!
//! Only the hostname is checked, deliberately not the port. A browser always
//! sends the authority it connected to, so the port can't disagree in a way
//! that signals an attack; comparing it only breaks reverse proxies, which
//! legitimately forward a different one. Don's own proxy does exactly that
//! when the daemon runs behind `proxy = { ... }`, and so does the Vite dev
//! server.
//!
//! Note what this does *not* stop: a blind cross-origin `POST` that ignores
//! the response still carries a loopback `Host` and still goes through. With
//! no token in play, a page you visit can trigger actions here without being
//! able to see the results.

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Reject requests that aren't addressed to a loopback name.
pub(crate) async fn guard(request: Request<Body>, next: Next) -> Response {
    if host_is_loopback(&request) {
        return next.run(request).await;
    }
    (
        StatusCode::MISDIRECTED_REQUEST,
        "don's web ui only answers to localhost",
    )
        .into_response()
}

/// Whether the request's `Host` names this machine.
fn host_is_loopback(request: &Request<Body>) -> bool {
    let Some(host) = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
    else {
        // HTTP/1.1 requires Host; anything without one isn't a browser.
        return false;
    };
    host_matches_loopback(host)
}

/// Strip any port and bracket pair, then match the name against loopback.
fn host_matches_loopback(host: &str) -> bool {
    let name = match host.rsplit_once(':') {
        // Only a trailing all-digit segment is a port; `[::1]` has neither.
        Some((before, after)) if !after.is_empty() && after.chars().all(|c| c.is_ascii_digit()) => {
            before
        }
        _ => host,
    };
    matches!(
        name.trim_start_matches('[').trim_end_matches(']'),
        "localhost" | "127.0.0.1" | "::1"
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn host_header_accepts_only_loopback_names() {
        struct Case {
            name: &'static str,
            host: &'static str,
            expect: bool,
        }

        let cases = vec![
            Case {
                name: "localhost with a port",
                host: "localhost:3666",
                expect: true,
            },
            Case {
                name: "ipv4 loopback with a port",
                host: "127.0.0.1:3666",
                expect: true,
            },
            Case {
                name: "ipv6 loopback with a port",
                host: "[::1]:3666",
                expect: true,
            },
            Case {
                name: "ipv6 loopback without a port",
                host: "[::1]",
                expect: true,
            },
            Case {
                name: "localhost without a port",
                host: "localhost",
                expect: true,
            },
            // A reverse proxy forwards the port it was reached on, which is
            // not the one this server bound. Don's own proxy does this when
            // the daemon runs behind `proxy = { ... }`.
            Case {
                name: "a different port is fine — proxies forward their own",
                host: "127.0.0.1:9999",
                expect: true,
            },
            Case {
                name: "rebound hostname is rejected",
                host: "evil.example.com:3666",
                expect: false,
            },
            Case {
                name: "rebound hostname resolving to loopback is still rejected",
                host: "localtest.me:3666",
                expect: false,
            },
            Case {
                name: "a lan address is rejected",
                host: "192.168.1.10:3666",
                expect: false,
            },
            Case {
                name: "empty host is rejected",
                host: "",
                expect: false,
            },
            Case {
                name: "a hostname ending in a non-numeric segment isn't split",
                host: "evil.example.com",
                expect: false,
            },
        ];

        for case in cases {
            assert_eq!(
                host_matches_loopback(case.host),
                case.expect,
                "case: {}",
                case.name
            );
        }
    }
}
