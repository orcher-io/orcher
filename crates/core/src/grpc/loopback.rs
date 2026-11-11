//! Calling this process's own gRPC services without a network.
//!
//! The REST API is written against the gRPC services: a REST handler is a
//! gRPC client. When the engine serves both, those calls stay in the process.
//! [`loopback`] makes a pair: a [`Loopback`] that dials, and a
//! [`LoopbackIncoming`] that the gRPC server serves as one more listener.
//! Each connection is an in-memory pipe, so there is no port to bind or
//! expose and no TLS to set up, and the server applies the same layers to it
//! as to a socket — a call that arrives this way is authenticated like any
//! other, with the credential the caller sent.

use futures::Stream;
use hyper_util::rt::TokioIo;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};

/// Bytes buffered in each direction of a connection.
const PIPE_BYTES: usize = 64 * 1024;

/// A dialer and the listener it reaches.
pub fn loopback() -> (Loopback, LoopbackIncoming) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (Loopback { sender }, LoopbackIncoming { receiver })
}

/// Opens connections to the server serving the paired [`LoopbackIncoming`].
#[derive(Clone)]
pub struct Loopback {
    sender: mpsc::UnboundedSender<DuplexStream>,
}

impl Loopback {
    /// A channel whose connections are made by this dialer. Connecting is
    /// lazy, as a reconnect is: each connection is a new pipe.
    pub fn channel(&self) -> Channel {
        let sender = self.sender.clone();
        // The address is never resolved; it names the authority the requests
        // carry.
        Endpoint::from_static("http://engine.loopback").connect_with_connector_lazy(
            tower::service_fn(move |_| {
                let (client, server) = tokio::io::duplex(PIPE_BYTES);
                let sent = sender.send(server).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "the engine's gRPC server is not serving in-process connections",
                    )
                });
                async move { sent.map(|()| TokioIo::new(client)) }
            }),
        )
    }
}

/// The connections [`Loopback`] opens, for a gRPC server to serve.
pub struct LoopbackIncoming {
    receiver: mpsc::UnboundedReceiver<DuplexStream>,
}

impl Stream for LoopbackIncoming {
    type Item = std::io::Result<DuplexStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx).map(|next| next.map(Ok))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_proto::orcher::v1::namespace_service_client::NamespaceServiceClient;
    use orcher_proto::orcher::v1::namespace_service_server::{
        NamespaceService, NamespaceServiceServer,
    };
    use orcher_proto::orcher::v1::*;
    use tonic::{Request, Response, Status};

    /// Answers every lookup with the name it was asked for.
    struct Echo;

    #[tonic::async_trait]
    impl NamespaceService for Echo {
        async fn get_namespace(
            &self,
            request: Request<GetNamespaceRequest>,
        ) -> Result<Response<GetNamespaceResponse>, Status> {
            Ok(Response::new(GetNamespaceResponse {
                namespace: Some(NamespaceInfo {
                    name: request.into_inner().name,
                    ..Default::default()
                }),
            }))
        }
        async fn create_namespace(
            &self,
            _: Request<CreateNamespaceRequest>,
        ) -> Result<Response<CreateNamespaceResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn list_namespaces(
            &self,
            _: Request<ListNamespacesRequest>,
        ) -> Result<Response<ListNamespacesResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn update_namespace(
            &self,
            _: Request<UpdateNamespaceRequest>,
        ) -> Result<Response<UpdateNamespaceResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn deprecate_namespace(
            &self,
            _: Request<DeprecateNamespaceRequest>,
        ) -> Result<Response<DeprecateNamespaceResponse>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn delete_namespace(
            &self,
            _: Request<DeleteNamespaceRequest>,
        ) -> Result<Response<DeleteNamespaceResponse>, Status> {
            Err(Status::unimplemented(""))
        }
    }

    #[tokio::test]
    async fn a_call_reaches_the_server_serving_the_paired_listener() {
        let (dialer, incoming) = loopback();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(NamespaceServiceServer::new(Echo))
                .serve_with_incoming(incoming),
        );
        let mut client = NamespaceServiceClient::new(dialer.channel());
        for name in ["first", "second"] {
            let request = GetNamespaceRequest {
                name: name.to_string(),
            };
            let answer = client.get_namespace(request).await.unwrap().into_inner();
            assert_eq!(answer.namespace.unwrap().name, name);
        }
    }

    #[tokio::test]
    async fn a_call_with_no_server_is_refused() {
        let (dialer, incoming) = loopback();
        drop(incoming);
        let mut client = NamespaceServiceClient::new(dialer.channel());
        let refused = client
            .get_namespace(GetNamespaceRequest::default())
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused}");
    }
}
