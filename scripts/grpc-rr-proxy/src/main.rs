//! A gRPC relay that sends each request to the next engine in turn.
//!
//! Used by `scripts/cluster-multinode-e2e.sh --stress` to put several engines
//! behind one address the way a per-request (L7) load balancer does. A TCP
//! proxy would not do: a gRPC client multiplexes every call over one HTTP/2
//! connection, so a TCP proxy pins all of a worker's calls to whichever engine
//! that connection reached. This one terminates HTTP/2 and picks a backend per
//! request, so consecutive calls from one worker land on different engines —
//! the traffic shape that exposes a node handling a workflow it does not own.
//!
//! Like a load balancer with connect-failure retries, a request whose backend
//! refuses the connection goes to the next one; nothing is retried once sent.
//! Request bodies are buffered to make that possible, which limits it to
//! unary calls — all the SDK workers make.
//!
//! Usage: grpc-rr-proxy <listen-addr> <backend-addr> [<backend-addr> ...]

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, Uri};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::{TokioExecutor, TokioIo};

type Body = BoxBody<Bytes, hyper::Error>;

struct Relay {
    backends: Vec<String>,
    next: AtomicUsize,
    client: Client<HttpConnector, Full<Bytes>>,
}

impl Relay {
    async fn handle(&self, request: Request<Incoming>) -> Response<Body> {
        let (parts, body) = request.into_parts();
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => return grpc_error(13, &format!("reading request: {e}")),
        };
        let path = parts
            .uri
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "/".to_string());

        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let mut last_error = String::new();
        for offset in 0..self.backends.len() {
            let backend = &self.backends[(start + offset) % self.backends.len()];
            let uri: Uri = match format!("http://{backend}{path}").parse() {
                Ok(uri) => uri,
                Err(e) => return grpc_error(13, &format!("bad backend uri: {e}")),
            };
            let mut outgoing = Request::builder().method(parts.method.clone()).uri(uri);
            for (name, value) in &parts.headers {
                outgoing = outgoing.header(name, value);
            }
            let outgoing = match outgoing.body(Full::new(body.clone())) {
                Ok(r) => r,
                Err(e) => return grpc_error(13, &format!("building request: {e}")),
            };
            match self.client.request(outgoing).await {
                Ok(response) => return response.map(|b| b.boxed()),
                // Refused before anything was sent: the next backend may take it.
                Err(e) if e.is_connect() => {
                    last_error = format!("{backend}: {e}");
                    continue;
                }
                Err(e) => return grpc_error(14, &format!("{backend}: {e}")),
            }
        }
        grpc_error(14, &format!("no backend reachable ({last_error})"))
    }
}

/// A trailers-only gRPC error response.
fn grpc_error(code: u32, message: &str) -> Response<Body> {
    let message: String = message
        .chars()
        .filter(|c| c.is_ascii_graphic() || *c == ' ')
        .collect();
    Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .header("grpc-status", code.to_string())
        .header("grpc-message", message)
        .body(Empty::new().map_err(|never| match never {}).boxed())
        .expect("static response")
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let listen = args
        .next()
        .expect("usage: grpc-rr-proxy <listen> <backend>...");
    let backends: Vec<String> = args.collect();
    assert!(!backends.is_empty(), "at least one backend is required");

    let client = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build_http();
    let relay = Arc::new(Relay {
        backends,
        next: AtomicUsize::new(0),
        client,
    });

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .expect("bind listen address");
    eprintln!(
        "grpc-rr-proxy: {listen} -> {} (per request, round robin)",
        relay.backends.join(", ")
    );
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                eprintln!("accept: {e}");
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let relay = relay.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let relay = relay.clone();
                async move { Ok::<_, Infallible>(relay.handle(request).await) }
            });
            if let Err(e) = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                eprintln!("connection: {e}");
            }
        });
    }
}
