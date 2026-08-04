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
//! hostname, not don's address. Rejecting those is the whole of this module.
//!
//! Note what this does *not* stop: a blind cross-origin `POST` that ignores
//! the response still carries the correct `Host` and still goes through. With
//! no token in play, a page you visit can trigger actions here without being
//! able to see the results.

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;

/// State the origin guard needs.
#[derive(Clone)]
pub(crate) struct OriginState {
    /// Port the UI is served on, used to validate the `Host` header.
    pub port: u16,
}

/// Reject requests that aren't addressed to this server on loopback.
pub(crate) async fn guard(
    State(state): State<Arc<OriginState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if host_is_loopback(&request, state.port) {
        return next.run(request).await;
    }
    (
        StatusCode::MISDIRECTED_REQUEST,
        "don's web ui only answers to localhost",
    )
        .into_response()
}

/// Accept only `Host` values that name this machine on the port we bound.
fn host_is_loopback(request: &Request<Body>, port: u16) -> bool {
    let Some(host) = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
    else {
        // HTTP/1.1 requires Host; anything without one isn't a browser.
        return false;
    };
    host_matches_loopback(host, port)
}

/// Split a `Host` header into name and port and check both.
fn host_matches_loopback(host: &str, port: u16) -> bool {
    let (name, host_port) = match host.rsplit_once(':') {
        // An IPv6 literal without a port: "[::1]".
        Some((_, after)) if !after.chars().all(|c| c.is_ascii_digit()) => (host, None),
        Some((before, after)) => (before, after.parse::<u16>().ok()),
        None => (host, None),
    };
    if let Some(host_port) = host_port
        && host_port != port
    {
        return false;
    }
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
    fn host_header_accepts_only_loopback_on_the_right_port() {
        struct Case {
            name: &'static str,
            host: &'static str,
            port: u16,
            expect: bool,
        }

        let cases = vec![
            Case {
                name: "localhost with matching port",
                host: "localhost:3666",
                port: 3666,
                expect: true,
            },
            Case {
                name: "ipv4 loopback with matching port",
                host: "127.0.0.1:3666",
                port: 3666,
                expect: true,
            },
            Case {
                name: "ipv6 loopback with matching port",
                host: "[::1]:3666",
                port: 3666,
                expect: true,
            },
            Case {
                name: "ipv6 loopback without a port",
                host: "[::1]",
                port: 3666,
                expect: true,
            },
            Case {
                name: "localhost without a port",
                host: "localhost",
                port: 3666,
                expect: true,
            },
            Case {
                name: "rebound hostname is rejected",
                host: "evil.example.com:3666",
                port: 3666,
                expect: false,
            },
            Case {
                name: "rebound hostname resolving to loopback is still rejected",
                host: "localtest.me:3666",
                port: 3666,
                expect: false,
            },
            Case {
                name: "wrong port is rejected",
                host: "127.0.0.1:9999",
                port: 3666,
                expect: false,
            },
            Case {
                name: "a lan address is rejected",
                host: "192.168.1.10:3666",
                port: 3666,
                expect: false,
            },
            Case {
                name: "empty host is rejected",
                host: "",
                port: 3666,
                expect: false,
            },
        ];

        for case in cases {
            assert_eq!(
                host_matches_loopback(case.host, case.port),
                case.expect,
                "case: {}",
                case.name
            );
        }
    }
}
