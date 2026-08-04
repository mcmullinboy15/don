//! Static assets for the web UI, compiled into the binary.
//!
//! `don` ships as a single file installed from a shell script, a Homebrew
//! formula, or `cargo install` — there is nowhere to put a `share/` directory
//! and no step that would populate one. So the built frontend is embedded.
//!
//! `rust-embed` reads from disk in debug builds and embeds in release ones,
//! which means a `npm run dev`-style loop works during development (rebuild
//! the bundle, reload the page — no `cargo build`) while a shipped binary
//! stays self-contained.
//!
//! Because `web/dist` must exist at compile time and crates.io will never run
//! npm, the built bundle is committed. See `web/README.md`.

use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

#[derive(rust_embed::Embed)]
#[folder = "web/dist"]
struct Assets;

/// Serve an embedded asset, falling back to `index.html`.
///
/// The fallback is what makes client-side routing work: a browser asked to
/// load `/projects/abc123` directly must receive the app shell, not a 404.
/// Requests that look like asset fetches (they have a file extension) get a
/// real 404 instead, so a missing script fails loudly rather than being
/// handed a page of HTML.
pub(crate) async fn serve(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    if let Some(response) = lookup(path) {
        return response;
    }

    if has_extension(path) {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }

    lookup("index.html").unwrap_or_else(missing_bundle)
}

fn lookup(path: &str) -> Option<Response> {
    let file = Assets::get(path)?;
    let mime = mime_for(path);
    Some(([(header::CONTENT_TYPE, mime)], file.data.into_owned()).into_response())
}

/// Whether a request path names a file rather than an app route.
fn has_extension(path: &str) -> bool {
    path.rsplit('/')
        .next()
        .is_some_and(|segment| segment.contains('.'))
}

/// Guess a content type from the extension.
///
/// A handful of hard-coded types beats a mime database: this only ever serves
/// the bundle don itself built, so the set of extensions is known.
fn mime_for(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, ext)| ext) {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("map") => "application/json",
        _ => "application/octet-stream",
    }
}

/// The bundle is missing entirely — a build problem, not a user problem, so
/// say which build step didn't run.
fn missing_bundle() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        "don was built without its web ui bundle.\n\
         Build it with `npm --prefix web ci && npm --prefix web run build`, \
         then rebuild don.\n",
    )
        .into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_asset_paths_from_app_routes() {
        struct Case {
            name: &'static str,
            path: &'static str,
            is_asset: bool,
        }

        let cases = vec![
            Case {
                name: "bundled script",
                path: "assets/index-abc123.js",
                is_asset: true,
            },
            Case {
                name: "root document",
                path: "index.html",
                is_asset: true,
            },
            Case {
                name: "app route",
                path: "projects/89d8f2c967fc",
                is_asset: false,
            },
            Case {
                name: "nested app route",
                path: "projects/89d8f2c967fc/logs/api",
                is_asset: false,
            },
            Case {
                name: "a dot in an earlier segment doesn't make it an asset",
                path: "projects/v1.2/logs",
                is_asset: false,
            },
        ];

        for case in cases {
            assert_eq!(
                has_extension(case.path),
                case.is_asset,
                "case: {}",
                case.name
            );
        }
    }

    #[test]
    fn maps_extensions_to_content_types() {
        struct Case {
            path: &'static str,
            mime: &'static str,
        }

        let cases = vec![
            Case {
                path: "index.html",
                mime: "text/html; charset=utf-8",
            },
            Case {
                path: "assets/app.js",
                mime: "text/javascript; charset=utf-8",
            },
            Case {
                path: "assets/app.css",
                mime: "text/css; charset=utf-8",
            },
            Case {
                path: "icon.svg",
                mime: "image/svg+xml",
            },
            Case {
                path: "font.woff2",
                mime: "font/woff2",
            },
            Case {
                path: "mystery",
                mime: "application/octet-stream",
            },
        ];

        for case in cases {
            assert_eq!(mime_for(case.path), case.mime, "path: {}", case.path);
        }
    }

    #[tokio::test]
    async fn unknown_routes_fall_back_to_the_app_shell() {
        // The bundle always contains index.html, so an app route resolves.
        let response = serve("/projects/abc".parse().unwrap()).await;
        assert_eq!(response.status(), StatusCode::OK);

        // A missing *asset* must 404 rather than silently serving HTML.
        let response = serve("/assets/does-not-exist.js".parse().unwrap()).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
