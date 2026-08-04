//! Authentication for the web UI.
//!
//! Every other don API rides on a unix socket chmod'd to 0600, which is
//! authentication enough: only the owning user can open it. The web UI
//! cannot do that — browsers speak TCP — and a loopback port is reachable by
//! every process on the machine, including anything running in a browser tab
//! on a page the user didn't write. Since this API can stop services, that
//! gap has to be closed explicitly:
//!
//! 1. **Bind loopback only** (done by the caller), so nothing off-box can
//!    reach it at all.
//! 2. **Check the `Host` header**, so a hostile site can't point a DNS name
//!    at 127.0.0.1 and have the browser send requests here on its behalf
//!    (DNS rebinding). A rebound request carries the attacker's hostname.
//! 3. **Require a shared token** that lives in a 0600 file next to the
//!    socket, so reaching the port isn't the same as being allowed to use it.
//!
//! `don ui` reads the token file and opens a URL carrying it; the first
//! request exchanges it for a cookie so it stops appearing in the address
//! bar and in browser history.

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::path::Path;
use std::sync::Arc;

/// Cookie the token is stored in after the first authorized request.
const COOKIE_NAME: &str = "don_ui_token";

/// Errors reading or creating the token file.
#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("failed to read the web ui token at '{}': {source}", path.display())]
    Read {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write the web ui token to '{}': {source}", path.display())]
    Write {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read random bytes from /dev/urandom: {0}")]
    Random(#[source] std::io::Error),
}

/// The shared secret guarding the web UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token(String);

impl Token {
    /// Read the token at `path`, creating one if it isn't there yet.
    ///
    /// Regenerating on every daemon start would invalidate open browser tabs
    /// on every restart, so the token is persistent — it is a capability for
    /// the machine's owner, not a session key.
    pub fn load_or_create(path: &Path) -> Result<Self, TokenError> {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let trimmed = contents.trim();
                if !trimmed.is_empty() {
                    return Ok(Self(trimmed.to_string()));
                }
                // An empty file is a botched write; replace it.
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(TokenError::Read {
                    path: path.to_path_buf(),
                    source,
                });
            }
        }

        let token = Self::generate()?;
        token.write_to(path)?;
        Ok(token)
    }

    /// Generate a fresh 256-bit token.
    ///
    /// Read straight from `/dev/urandom` rather than pulling in an RNG crate:
    /// don is Unix-only, and this is the one place in the codebase that needs
    /// randomness.
    pub fn generate() -> Result<Self, TokenError> {
        Ok(Self(hex::encode(read_urandom()?)))
    }

    fn write_to(&self, path: &Path) -> Result<(), TokenError> {
        use std::io::Write;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| TokenError::Write {
                path: path.to_path_buf(),
                source,
            })?;
        }
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).map_err(|source| TokenError::Write {
            path: path.to_path_buf(),
            source,
        })?;
        file.write_all(self.0.as_bytes())
            .map_err(|source| TokenError::Write {
                path: path.to_path_buf(),
                source,
            })
    }

    /// The token as it appears in a URL or header.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Compare against a candidate without leaking length or content through
    /// timing. Overkill for a loopback service, but the cost is nothing.
    pub fn matches(&self, candidate: &str) -> bool {
        constant_time_eq(self.0.as_bytes(), candidate.as_bytes())
    }
}

/// Read 32 bytes of randomness.
fn read_urandom() -> Result<Vec<u8>, TokenError> {
    use std::io::Read;
    let mut file = std::fs::File::open("/dev/urandom").map_err(TokenError::Random)?;
    let mut buf = vec![0u8; 32];
    file.read_exact(&mut buf).map_err(TokenError::Random)?;
    Ok(buf)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// State the auth middleware needs.
#[derive(Clone)]
pub(crate) struct AuthState {
    pub token: Token,
    /// Port the UI is served on, used to validate the `Host` header.
    pub port: u16,
}

/// Reject requests that aren't addressed to loopback, or that don't carry the
/// token.
///
/// A valid `?token=` on a page request is exchanged for a cookie and
/// redirected, so the secret leaves the URL bar immediately.
pub(crate) async fn guard(
    State(state): State<Arc<AuthState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !host_is_loopback(&request, state.port) {
        return (
            StatusCode::MISDIRECTED_REQUEST,
            "don's web ui only answers to localhost",
        )
            .into_response();
    }

    let uri = request.uri().clone();
    if let Some(supplied) = token_from_query(uri.query()) {
        if !state.token.matches(&supplied) {
            return unauthorized();
        }
        // Strip the token from the URL and hand the browser a cookie instead.
        let clean = strip_token_param(&uri);
        return (
            StatusCode::FOUND,
            [
                (header::LOCATION, clean),
                (
                    header::SET_COOKIE,
                    format!("{COOKIE_NAME}={}; Path=/; HttpOnly; SameSite=Strict", state.token.as_str()),
                ),
            ],
        )
            .into_response();
    }

    let supplied = bearer_token(&request).or_else(|| cookie_token(&request));
    match supplied {
        Some(value) if state.token.matches(&value) => next.run(request).await,
        _ => unauthorized(),
    }
}

/// Accept only `Host` values that name this machine on the port we bound.
///
/// A request rebound from `evil.example.com` carries that name and is
/// rejected here even though it arrived on the loopback socket.
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

fn bearer_token(request: &Request<Body>) -> Option<String> {
    let value = request
        .headers()
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let rest = value.strip_prefix("Bearer ").or_else(|| value.strip_prefix("bearer "))?;
    Some(rest.trim().to_string())
}

fn cookie_token(request: &Request<Body>) -> Option<String> {
    let cookies = request.headers().get(header::COOKIE)?.to_str().ok()?;
    cookie_value(cookies, COOKIE_NAME)
}

/// Pull one cookie out of a `Cookie:` header value.
fn cookie_value(header_value: &str, name: &str) -> Option<String> {
    header_value.split(';').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key.trim() == name).then(|| value.trim().to_string())
    })
}

fn token_from_query(query: Option<&str>) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == "token").then(|| percent_decode(value))
    })
}

/// Rebuild the URI without the `token` parameter.
fn strip_token_param(uri: &axum::http::Uri) -> String {
    let path = uri.path();
    let remaining: Vec<&str> = uri
        .query()
        .into_iter()
        .flat_map(|q| q.split('&'))
        .filter(|pair| !pair.starts_with("token="))
        .filter(|pair| !pair.is_empty())
        .collect();
    if remaining.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{}", remaining.join("&"))
    }
}

/// Minimal percent-decoder — tokens are hex, so this only has to survive an
/// over-eager URL encoder rather than handle every escape.
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes.get(i) {
            Some(b'%') if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            Some(byte) => {
                out.push(*byte);
                i += 1;
            }
            None => break,
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A 401 that tells the user how to get in, rather than just refusing.
fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        concat!(
            "<!doctype html><meta charset=utf-8>",
            "<title>don</title>",
            "<style>body{font:16px/1.6 system-ui,sans-serif;margin:4rem auto;max-width:34rem;",
            "color:#e6e6e6;background:#16161a}code{background:#26262c;padding:.15em .4em;",
            "border-radius:4px}</style>",
            "<h1>Not authorized</h1>",
            "<p>This browser hasn't been given don's web UI token.</p>",
            "<p>Run <code>don ui</code> on this machine to open an authorized link.</p>",
        ),
    )
        .into_response()
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

    #[test]
    fn token_comparison_rejects_near_misses() {
        let token = Token("abc123".to_string());
        struct Case {
            name: &'static str,
            candidate: &'static str,
            expect: bool,
        }

        let cases = vec![
            Case {
                name: "exact match",
                candidate: "abc123",
                expect: true,
            },
            Case {
                name: "wrong value",
                candidate: "abc124",
                expect: false,
            },
            Case {
                name: "prefix is not enough",
                candidate: "abc",
                expect: false,
            },
            Case {
                name: "suffix is not enough",
                candidate: "abc1234",
                expect: false,
            },
            Case {
                name: "empty",
                candidate: "",
                expect: false,
            },
            Case {
                name: "case matters",
                candidate: "ABC123",
                expect: false,
            },
        ];

        for case in cases {
            assert_eq!(
                token.matches(case.candidate),
                case.expect,
                "case: {}",
                case.name
            );
        }
    }

    #[test]
    fn extracts_token_from_query_and_cookies() {
        struct Case {
            name: &'static str,
            query: Option<&'static str>,
            expect: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "only param",
                query: Some("token=deadbeef"),
                expect: Some("deadbeef"),
            },
            Case {
                name: "among others",
                query: Some("foo=1&token=deadbeef&bar=2"),
                expect: Some("deadbeef"),
            },
            Case {
                name: "percent-encoded",
                query: Some("token=dead%20beef"),
                expect: Some("dead beef"),
            },
            Case {
                name: "absent",
                query: Some("foo=1"),
                expect: None,
            },
            Case {
                name: "no query at all",
                query: None,
                expect: None,
            },
            Case {
                name: "a param merely ending in token is not it",
                query: Some("nottoken=x"),
                expect: None,
            },
        ];

        for case in cases {
            assert_eq!(
                token_from_query(case.query).as_deref(),
                case.expect,
                "case: {}",
                case.name
            );
        }

        assert_eq!(
            cookie_value("don_ui_token=abc; other=1", COOKIE_NAME).as_deref(),
            Some("abc")
        );
        assert_eq!(
            cookie_value("other=1; don_ui_token=abc", COOKIE_NAME).as_deref(),
            Some("abc")
        );
        assert_eq!(cookie_value("other=1", COOKIE_NAME), None);
    }

    #[test]
    fn stripping_the_token_preserves_the_rest_of_the_url() {
        struct Case {
            name: &'static str,
            uri: &'static str,
            expect: &'static str,
        }

        let cases = vec![
            Case {
                name: "token only",
                uri: "/?token=abc",
                expect: "/",
            },
            Case {
                name: "token with other params",
                uri: "/projects/x?token=abc&tab=logs",
                expect: "/projects/x?tab=logs",
            },
            Case {
                name: "token last",
                uri: "/x?tab=logs&token=abc",
                expect: "/x?tab=logs",
            },
            Case {
                name: "no query",
                uri: "/x",
                expect: "/x",
            },
        ];

        for case in cases {
            let uri: axum::http::Uri = case.uri.parse().unwrap();
            assert_eq!(strip_token_param(&uri), case.expect, "case: {}", case.name);
        }
    }

    #[test]
    fn generated_tokens_are_unique_and_hex() {
        let a = Token::generate().unwrap();
        let b = Token::generate().unwrap();
        assert_ne!(a, b, "two generated tokens must differ");
        assert_eq!(a.as_str().len(), 64, "32 bytes as hex");
        assert!(a.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn load_or_create_persists_and_reuses() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("token");

        let first = Token::load_or_create(&path).unwrap();
        assert!(path.exists());
        let second = Token::load_or_create(&path).unwrap();
        assert_eq!(first, second, "an existing token must be reused");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "token file must be owner-only");
        }

        // An empty file is treated as absent rather than as a valid token.
        std::fs::write(&path, "").unwrap();
        let third = Token::load_or_create(&path).unwrap();
        assert_ne!(third, first);
        assert!(!third.as_str().is_empty());
    }
}
