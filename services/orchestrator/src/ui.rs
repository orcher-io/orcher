//! The dashboard: static files served on the HTTP port.
//!
//! The dashboard (orcher-io/ui) is a single-page app with no server of its
//! own: it calls the REST API from the browser, on the origin that served it.
//! The engine serves its files after every route it already has, so `/api/v1`,
//! `/health` and `/metrics` are untouched, and answers any other path with
//! `index.html`, where the app routes it.
//!
//! The files are public — the API, not this, decides what a caller may see.

use std::convert::Infallible;
use std::path::{Path, PathBuf};

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use tower::ServiceExt;
use tower_http::services::{ServeDir, ServeFile};

use crate::config::UiConfig;

/// Paths the engine's own routes own. A miss under one of them is the caller's
/// mistake, answered 404 — never the dashboard's page with a 200, which an API
/// client would take for success.
const RESERVED: [&str; 3] = ["/api", "/health", "/metrics"];

/// Content-Security-Policy for the dashboard's pages. Its static export
/// carries inline scripts and styles, hence 'unsafe-inline'; everything it
/// loads or calls is its own origin.
const CSP: &str = "default-src 'self'; script-src 'self' 'unsafe-inline'; \
                   style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; \
                   font-src 'self' data:; connect-src 'self'; object-src 'none'; \
                   base-uri 'self'; form-action 'self'; frame-ancestors 'none'";

/// Whether the engine serves the dashboard, and if not, why.
#[derive(Debug, PartialEq, Eq)]
pub enum Dashboard {
    Serve(PathBuf),
    Off,
    Missing(PathBuf),
}

impl Dashboard {
    pub fn from_config(config: &UiConfig) -> Self {
        if !config.enabled {
            return Self::Off;
        }
        let dir = PathBuf::from(&config.dir);
        if dir.join("index.html").is_file() {
            Self::Serve(dir)
        } else {
            Self::Missing(dir)
        }
    }
}

/// The fallback service that serves the dashboard in `dir`.
pub fn service(
    dir: &Path,
) -> impl tower::Service<
    Request,
    Response = Response,
    Error = Infallible,
    Future = impl Send + 'static,
> + Clone
       + Send
       + 'static {
    let files = ServeDir::new(dir)
        .append_index_html_on_directories(true)
        .fallback(ServeFile::new(dir.join("index.html")));
    tower::service_fn(move |request: Request| {
        let files = files.clone();
        async move { Ok::<_, Infallible>(serve(files, request).await) }
    })
}

async fn serve(files: ServeDir<ServeFile>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    if is_reserved(&path) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let mut response = match files.oneshot(request).await {
        Ok(response) => response.map(Body::new),
        Err(never) => match never {},
    };
    let headers = response.headers_mut();
    // Next names every file under _next/static by its content: it never changes
    let cache = if path.starts_with("/_next/static/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}

fn is_reserved(path: &str) -> bool {
    RESERVED
        .iter()
        .any(|prefix| path == *prefix || path.starts_with(&format!("{prefix}/")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::Router;

    /// A dashboard on disk: an index page and one hashed static asset.
    fn dashboard() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<html>dashboard</html>").unwrap();
        std::fs::create_dir_all(dir.path().join("_next/static/chunks")).unwrap();
        std::fs::write(
            dir.path().join("_next/static/chunks/app-1a2b.js"),
            "console.log(1)",
        )
        .unwrap();
        dir
    }

    /// The HTTP port as the engine builds it: its own routes, then the dashboard.
    fn engine(dir: &Path) -> Router {
        let api = Router::new().route("/workflows", get(|| async { "workflows" }));
        Router::new()
            .route("/health", get(|| async { "healthy" }))
            .route("/metrics", get(|| async { "metrics" }))
            .nest("/api/v1", api)
            .fallback_service(service(dir))
    }

    async fn get_path(router: &Router, path: &str) -> (StatusCode, Response) {
        let response = router
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        (response.status(), response)
    }

    async fn body(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn the_engine_routes_answer_as_before() {
        let dir = dashboard();
        let router = engine(dir.path());
        for (path, expected) in [
            ("/api/v1/workflows", "workflows"),
            ("/health", "healthy"),
            ("/metrics", "metrics"),
        ] {
            let (status, response) = get_path(&router, path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(body(response).await, expected, "{path}");
        }
    }

    #[tokio::test]
    async fn a_miss_under_an_engine_route_is_404_not_the_dashboard() {
        let dir = dashboard();
        let router = engine(dir.path());
        for path in [
            "/api/v1/workflowz",
            "/api/v2/anything",
            "/api",
            "/health/nope",
            "/metrics/x",
        ] {
            let (status, response) = get_path(&router, path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert!(!body(response).await.contains("dashboard"), "{path}");
        }
    }

    #[tokio::test]
    async fn the_dashboard_answers_its_root_its_files_and_any_deep_link() {
        let dir = dashboard();
        let router = engine(dir.path());
        for path in ["/", "/workflows/abc-123", "/settings"] {
            let (status, response) = get_path(&router, path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "no-cache",
                "{path}"
            );
            assert!(response.headers()[header::CONTENT_SECURITY_POLICY]
                .to_str()
                .unwrap()
                .contains("frame-ancestors 'none'"));
            assert_eq!(body(response).await, "<html>dashboard</html>", "{path}");
        }
        let (status, response) = get_path(&router, "/_next/static/chunks/app-1a2b.js").await;
        assert_eq!(status, StatusCode::OK);
        assert!(response.headers()[header::CACHE_CONTROL]
            .to_str()
            .unwrap()
            .contains("immutable"));
        assert_eq!(body(response).await, "console.log(1)");
    }

    #[tokio::test]
    async fn no_path_reaches_outside_the_dashboard() {
        let dir = dashboard();
        let secret = dir.path().parent().unwrap().join("outside.txt");
        std::fs::write(&secret, "secret").unwrap();
        let router = engine(dir.path());
        for path in [
            "/../outside.txt",
            "/%2e%2e/outside.txt",
            "/_next/../../outside.txt",
        ] {
            let (_, response) = get_path(&router, path).await;
            assert!(!body(response).await.contains("secret"), "{path}");
        }
        std::fs::remove_file(secret).unwrap();
    }

    #[test]
    fn served_only_when_on_and_present() {
        let dir = dashboard();
        let on = |dir: &Path| UiConfig {
            enabled: true,
            dir: dir.display().to_string(),
        };
        assert_eq!(
            Dashboard::from_config(&on(dir.path())),
            Dashboard::Serve(dir.path().into())
        );
        assert_eq!(
            Dashboard::from_config(&UiConfig {
                enabled: false,
                ..on(dir.path())
            }),
            Dashboard::Off
        );
        let empty = tempfile::tempdir().unwrap();
        assert_eq!(
            Dashboard::from_config(&on(empty.path())),
            Dashboard::Missing(empty.path().into())
        );
    }
}
