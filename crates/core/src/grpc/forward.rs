//! Relaying a request to the node that owns its workflow.
//!
//! With clustering on, each workflow belongs to one node: its id hashes to a
//! shard, the live members split the shards into contiguous ranges, and only
//! the range owner may write the workflow's state — the write fence refuses
//! anyone else. A worker does not know any of that. It talks to whichever node
//! its load balancer picked, and with more than one node behind a per-request
//! balancer, a completion lands on a node that does not own the workflow about
//! half the time. That node used to process it anyway: every step before the
//! first fenced write went through, the fenced write was refused, and the
//! worker got an error for a batch that was partly applied. Measured with two
//! nodes behind round robin, a thousand one-task workflows took minutes and
//! some never finished.
//!
//! So before any side effect, the node that received the request decides
//! where it belongs. If another live node owns the workflow and can be dialed,
//! the whole request is relayed there and the owner's answer is returned
//! unchanged; otherwise it is handled here.
//!
//! # No trust between nodes
//!
//! The relay carries the caller's own credentials — `authorization` and the
//! organisation headers — and the owner authenticates and authorises the
//! request exactly as if the worker had sent it directly. A node gains no
//! authority by relaying: it could not get anything done on the owner that the
//! worker could not.
//!
//! # One hop
//!
//! A relayed request is marked with [`FORWARDED_BY_HEADER`] and is always
//! handled where it arrives. Two nodes whose views of the membership disagree
//! for a moment would otherwise pass a request back and forth; with the mark,
//! the second node handles it, its fence decides, and a refusal is answered as
//! retryable.
//!
//! # When the owner cannot be reached
//!
//! If the connection to the owner is refused — nothing listens at its address,
//! so its process is gone — nothing was sent, and the request is handled
//! here. The owner is then treated as unreachable for a few seconds
//! ([`ReachablePeers`]): requests for its workflows are not sent its way, and
//! range-strict ownership lets this node take its shards first-touch, as
//! before forwarding existed. The database still arbitrates — a shard the dead
//! owner held stays refused until its heartbeat goes stale — so this changes
//! how soon work moves, never who may write.
//!
//! Only a refusal counts. A connection that times out, or fails its TLS
//! handshake, says nothing about whether the owner is alive — it may be slow,
//! or misconfigured — and taking a live node's shards over that would leave
//! two nodes fighting for them. Those answer the caller with a retryable error
//! and change nothing. So does a relay the owner accepts but does not answer
//! in time: its channel is dropped, so the next relay reconnects rather than
//! queueing behind a hung connection, and HTTP/2 keep-alive notices a dead one
//! between relays. If the connection breaks after sending, the owner may or
//! may not have applied the request, so it is not repeated here either.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::future::{BoxFuture, FutureExt, Shared};
use orcher_cluster::{Membership, NodeId, PeerDirectory, ShardMap, ShardOwnership};
use orcher_proto::orcher::v1::execution_service_client::ExecutionServiceClient;
use orcher_proto::orcher::v1::{
    CompleteWorkflowExecutionRequest, CompleteWorkflowExecutionResponse,
    ReleaseWorkflowExecutionRequest, ReleaseWorkflowExecutionResponse,
};
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Status;
use tracing::{debug, warn};

use crate::grpc::tenant::{LEGACY_ORGANIZATION_KEYS, ORGANIZATION_METADATA_KEY};

/// Marks a request as already relayed once, naming the node that relayed it.
/// A request carrying it is always handled where it arrives.
pub const FORWARDED_BY_HEADER: &str = "x-orcher-forwarded-by";

/// What the owner needs to authenticate the caller and to trace the request.
///
/// A list rather than "everything": transport headers belong to each hop, and
/// a header the relay did not mean to pass on — [`FORWARDED_BY_HEADER`] among
/// them — must not ride along by accident.
const RELAYED_METADATA: &[&str] = &[
    "authorization",
    ORGANIZATION_METADATA_KEY,
    LEGACY_ORGANIZATION_KEYS[0],
    LEGACY_ORGANIZATION_KEYS[1],
    "traceparent",
    "tracestate",
    "x-trace-id",
    "x-request-id",
    // What the worker can receive, which bounds the tasks the owner hands
    // back with its answer.
    crate::grpc::payload_limits::MAX_RECEIVE_MESSAGE_BYTES_HEADER,
];

const RPC_COMPLETE_WORKFLOW_EXECUTION: &str = "CompleteWorkflowExecution";
const RPC_RELEASE_WORKFLOW_EXECUTION: &str = "ReleaseWorkflowExecution";

/// Attempts at opening a connection to the owner before handling the request
/// here instead. The second covers a connection refused while the owner was
/// restarting its listener; more would only delay the fallback.
const CONNECT_ATTEMPTS: u32 = 2;
const CONNECT_RETRY_PAUSE: Duration = Duration::from_millis(50);

/// HTTP/2 keep-alive on peer channels: a ping this often, and a connection
/// whose ping goes unanswered this long is dropped, so a peer that hung or
/// vanished is found between relays rather than by the next relay's timeout.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// How often cached channels are checked against the live membership, so a
/// peer that left — or re-registered at another address — does not keep a
/// channel open forever.
const PRUNE_EVERY: Duration = Duration::from_secs(5);

/// The cluster view a forwarding decision reads: who this node is, who owns a
/// workflow, and where that owner can be dialed. `DbCluster` is one.
pub trait OwnerDirectory: Send + Sync {
    /// This node.
    fn this_node(&self) -> &NodeId;
    /// The node whose range covers `workflow_id`'s shard; `None` when no
    /// member is known.
    fn owner_of_workflow(&self, workflow_id: &str) -> Option<NodeId>;
    /// Every node in the live set.
    fn live_nodes(&self) -> Vec<NodeId>;
    /// Where `node` can be dialed; `None` when it cannot be.
    fn address_of(&self, node: &NodeId) -> Option<String>;
}

impl<T: Membership + ShardMap + PeerDirectory> OwnerDirectory for T {
    fn this_node(&self) -> &NodeId {
        Membership::this_node(self)
    }
    fn owner_of_workflow(&self, workflow_id: &str) -> Option<NodeId> {
        self.owner_of(self.shard_of(workflow_id))
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        Membership::live_nodes(self)
    }
    fn address_of(&self, node: &NodeId) -> Option<String> {
        PeerDirectory::address_of(self, node)
    }
}

/// How long a peer that refused a connection is treated as unreachable.
///
/// Long enough not to dial a dead process on every request, short enough that
/// a peer that was only restarting is relayed to again soon after; the
/// membership heartbeat removes a peer that stays dead.
const UNREACHABLE_FOR: Duration = Duration::from_secs(5);

/// A cluster view that also remembers which peers just refused a connection,
/// and answers "cannot be dialed" for them until [`UNREACHABLE_FOR`] passes.
///
/// Shared by the forwarder, which records refusals and stops relaying to the
/// peer, and by range-strict shard ownership, which then lets this node take
/// the peer's shards first-touch instead of refusing every request for them
/// until the peer leaves the live set.
pub struct ReachablePeers {
    directory: Arc<dyn OwnerDirectory>,
    refused: DashMap<NodeId, Instant>,
}

impl ReachablePeers {
    /// Wrap `directory`, with every peer reachable to begin with.
    pub fn new(directory: Arc<dyn OwnerDirectory>) -> Self {
        ReachablePeers {
            directory,
            refused: DashMap::new(),
        }
    }

    /// `node` refused a connection just now.
    pub fn mark_unreachable(&self, node: &NodeId) {
        self.refused.insert(node.clone(), Instant::now());
    }

    fn recently_refused(&self, node: &NodeId) -> bool {
        let refused_at = self.refused.get(node).map(|at| *at);
        match refused_at {
            Some(at) if at.elapsed() < UNREACHABLE_FOR => true,
            Some(_) => {
                self.refused.remove(node);
                false
            }
            None => false,
        }
    }
}

impl PeerDirectory for ReachablePeers {
    fn address_of(&self, node: &NodeId) -> Option<String> {
        if self.recently_refused(node) {
            return None;
        }
        self.directory.address_of(node)
    }
}

/// Where a request should be handled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Here.
    Local,
    /// On the owner.
    Owner {
        /// The node owning the workflow.
        node: NodeId,
        /// Where it is dialed.
        address: String,
    },
}

/// Settings for relaying to peers.
#[derive(Debug, Clone)]
pub struct ForwardConfig {
    /// Deadline for a relayed call when the caller set none.
    pub default_timeout: Duration,
    /// How long to wait for a connection to a peer before handling the
    /// request here.
    pub connect_timeout: Duration,
    /// Present when the engine serves TLS, so peers are dialed the same way.
    pub tls: Option<ClientTlsConfig>,
    /// The largest message relayed either way: the engine's own limit, since a
    /// relayed completion is the worker's, and the owner's answer is the
    /// worker's too.
    pub max_message_bytes: usize,
}

impl Default for ForwardConfig {
    fn default() -> Self {
        ForwardConfig {
            default_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(1),
            tls: None,
            max_message_bytes: crate::grpc::payload_limits::DEFAULT_MAX_MESSAGE_BYTES,
        }
    }
}

/// Why no connection to a peer could be opened.
#[derive(Debug, Clone)]
struct ConnectFailure {
    /// Nothing listens at the address: the only failure that shows the peer's
    /// process is gone.
    refused: bool,
    message: String,
}

type Connecting = Shared<BoxFuture<'static, Result<Channel, ConnectFailure>>>;

/// A peer's channel, or the one attempt under way to open it. Every relay
/// that needs the peer while it connects awaits that attempt instead of
/// starting its own, so a burst of relays to a peer opens one connection.
#[derive(Clone)]
enum Slot {
    Ready(Channel),
    Connecting(Connecting),
}

/// The result of trying to relay a request.
#[derive(Debug)]
pub enum Relayed<Req, Resp> {
    /// The owner answered, successfully or not; return it to the caller as is.
    Answered(Result<tonic::Response<Resp>, Status>),
    /// Nothing reached the owner. The request is handed back to be handled here.
    NotSent(Req),
    /// The owner does not know this RPC: it runs a version from before the
    /// RPC existed, and did nothing. The request is handed back, for the
    /// caller to handle here if it can do so safely while the owner lives.
    Unsupported(Req),
}

/// Decides whether a request belongs to another node and relays it there.
pub struct OwnerForwarder {
    directory: Arc<dyn OwnerDirectory>,
    peers: Arc<ReachablePeers>,
    /// This node's shard ownership, for refusing locally — before any side
    /// effect — a request whose workflow another node owns.
    ownership: Option<Arc<ShardOwnership>>,
    config: ForwardConfig,
    /// One channel per peer address, opened on first use and reused; a channel
    /// multiplexes every concurrent relay to that peer over one connection.
    channels: DashMap<String, Slot>,
    /// When cached channels were last checked against the live membership.
    last_prune: std::sync::Mutex<Instant>,
}

impl std::fmt::Debug for OwnerForwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnerForwarder")
            .field("node", self.directory.this_node())
            .field("peers_connected", &self.channels.len())
            .finish()
    }
}

impl OwnerForwarder {
    /// A forwarder deciding by `directory` and dialing peers per `config`.
    pub fn new(directory: Arc<dyn OwnerDirectory>, config: ForwardConfig) -> Self {
        OwnerForwarder {
            peers: Arc::new(ReachablePeers::new(directory.clone())),
            directory,
            ownership: None,
            config,
            channels: DashMap::new(),
            last_prune: std::sync::Mutex::new(Instant::now()),
        }
    }

    /// The peer view this forwarder dials from, including the peers it found
    /// unreachable — what range-strict ownership must decide by, so that the
    /// two agree on which owners a request can be relayed to.
    pub fn peers(&self) -> Arc<ReachablePeers> {
        self.peers.clone()
    }

    /// Check ownership before handling a request here (see
    /// [`ensure_owned_here`](Self::ensure_owned_here)).
    pub fn with_ownership(mut self, ownership: Arc<ShardOwnership>) -> Self {
        self.ownership = Some(ownership);
        self
    }

    /// Where the request for `workflow_id`, carrying `metadata`, is handled.
    ///
    /// Here when it was already relayed once, when this node owns the
    /// workflow, when no node does (the live set is unknown), and when the
    /// owner has no address that can be dialed. Otherwise on the owner.
    pub fn route(&self, workflow_id: &str, metadata: &MetadataMap) -> Route {
        if metadata.contains_key(FORWARDED_BY_HEADER) {
            return Route::Local;
        }
        let workflow_id = canonical_workflow_id(workflow_id);
        let Some(owner) = self.directory.owner_of_workflow(&workflow_id) else {
            return Route::Local;
        };
        if &owner == self.directory.this_node() {
            return Route::Local;
        }
        match self.peers.address_of(&owner) {
            Some(address) => Route::Owner {
                node: owner,
                address,
            },
            None => Route::Local,
        }
    }

    /// Refuse, as retryable, a request for a workflow this node may not write.
    ///
    /// The fence would refuse it anyway, but only at the first fenced write,
    /// after every earlier command in the batch had been applied — a partly
    /// applied batch the caller then sends again. Asking first costs a map
    /// lookup: the fence caches the generation it acquires, and this acquires
    /// exactly what the first fenced write would have.
    pub async fn ensure_owned_here(&self, workflow_id: &str) -> Result<(), Status> {
        let Some(ownership) = &self.ownership else {
            return Ok(());
        };
        let workflow_id = canonical_workflow_id(workflow_id);
        match ownership.acquire_for(&workflow_id).await {
            Ok(Some(_)) => Ok(()),
            Ok(None) => {
                let message = format!(
                    "{}: workflow {workflow_id} belongs to another node; retry",
                    orcher_storage::sharding::OWNERSHIP_LOST
                );
                warn!("{message}");
                Err(Status::unavailable(message))
            }
            Err(e) => Err(Status::unavailable(format!(
                "could not confirm this node owns workflow {workflow_id}: {e}; retry"
            ))),
        }
    }

    /// Relay a `CompleteWorkflowExecution` to `owner` at `address`.
    ///
    /// `received_at` is when the caller's request arrived: the relay may use
    /// what is left of the caller's deadline, never more, and never more than
    /// the configured relay timeout.
    pub async fn complete_workflow_execution(
        &self,
        owner: &NodeId,
        address: &str,
        metadata: &MetadataMap,
        body: CompleteWorkflowExecutionRequest,
        received_at: Instant,
    ) -> Relayed<CompleteWorkflowExecutionRequest, CompleteWorkflowExecutionResponse> {
        self.relay(
            RPC_COMPLETE_WORKFLOW_EXECUTION,
            owner,
            address,
            metadata,
            body,
            received_at,
            |mut client, request| async move { client.complete_workflow_execution(request).await },
        )
        .await
    }

    /// Relay a `ReleaseWorkflowExecution` to `owner` at `address`, as
    /// [`complete_workflow_execution`](Self::complete_workflow_execution) does
    /// a completion: the release gives the claim back through the same fence,
    /// which the owner holds.
    pub async fn release_workflow_execution(
        &self,
        owner: &NodeId,
        address: &str,
        metadata: &MetadataMap,
        body: ReleaseWorkflowExecutionRequest,
        received_at: Instant,
    ) -> Relayed<ReleaseWorkflowExecutionRequest, ReleaseWorkflowExecutionResponse> {
        self.relay(
            RPC_RELEASE_WORKFLOW_EXECUTION,
            owner,
            address,
            metadata,
            body,
            received_at,
            |mut client, request| async move { client.release_workflow_execution(request).await },
        )
        .await
    }

    /// Relay one request to `owner` at `address` by `call`, and say whether
    /// the owner answered or the request is to be handled here.
    #[allow(clippy::too_many_arguments)]
    async fn relay<Req, Resp, Call, Answer>(
        &self,
        rpc: &'static str,
        owner: &NodeId,
        address: &str,
        metadata: &MetadataMap,
        body: Req,
        received_at: Instant,
        call: Call,
    ) -> Relayed<Req, Resp>
    where
        Req: Clone,
        Call: FnOnce(ExecutionServiceClient<Channel>, tonic::Request<Req>) -> Answer,
        Answer: std::future::Future<Output = Result<tonic::Response<Resp>, Status>>,
    {
        use orcher_monitoring::metrics::sharding::record_forwarded_request as record;
        let channel = match self.channel(address).await {
            Ok(channel) => channel,
            Err(failure) if failure.refused => {
                self.peers.mark_unreachable(owner);
                record(rpc, "unreachable");
                warn!(
                    peer = %address,
                    error = %failure.message,
                    "the workflow's owner refused the connection; handling the request here"
                );
                return Relayed::NotSent(body);
            }
            Err(failure) => {
                // Slow or misconfigured, not gone: nothing here may take its
                // shards, so the caller retries instead.
                record(rpc, "connect_failed");
                warn!(
                    peer = %address,
                    error = %failure.message,
                    "could not connect to the workflow's owner"
                );
                return Relayed::Answered(Err(Status::unavailable(format!(
                    "the node owning this workflow could not be reached: {}",
                    failure.message
                ))));
            }
        };

        let Some(timeout) = relay_budget(
            incoming_timeout(metadata),
            received_at.elapsed(),
            self.config.default_timeout,
        ) else {
            record(rpc, "timeout");
            return Relayed::Answered(Err(Status::deadline_exceeded(
                "the caller's deadline passed before the request could be relayed",
            )));
        };
        // Kept in case the connection turns out to be refused, when nothing was
        // sent and the request is handled here after all. A cached channel to
        // a peer that has since died only says so when a call is made on it.
        let fallback = body.clone();
        let request = relay_request(body, metadata, self.directory.this_node(), timeout);
        let client = ExecutionServiceClient::new(channel)
            .max_decoding_message_size(self.config.max_message_bytes)
            .max_encoding_message_size(self.config.max_message_bytes);
        let answer = match tokio::time::timeout(timeout, call(client, request)).await {
            Ok(answer) => answer,
            Err(_) => {
                // A hung owner: drop the channel so the next relay reconnects
                // rather than queueing on the same connection.
                self.channels.remove(address);
                record(rpc, "timeout");
                warn!(peer = %address, ?timeout, "the workflow's owner did not answer in time");
                return Relayed::Answered(Err(Status::unavailable(format!(
                    "the node owning this workflow did not answer within {timeout:?}"
                ))));
            }
        };

        match answer {
            Ok(response) => {
                record(rpc, "forwarded");
                Relayed::Answered(Ok(response))
            }
            Err(status) if is_connection_refused(&status) => {
                self.channels.remove(address);
                self.peers.mark_unreachable(owner);
                record(rpc, "unreachable");
                warn!(
                    peer = %address,
                    "the workflow's owner refused the connection; handling the request here"
                );
                Relayed::NotSent(fallback)
            }
            Err(status) if is_transport_failure(&status) => {
                // The connection broke under the call. Whether the owner applied
                // it is unknown, so it is not repeated here; the caller retries.
                self.channels.remove(address);
                record(rpc, "failed");
                warn!(peer = %address, error = %status, "relay to the workflow's owner failed");
                Relayed::Answered(Err(Status::unavailable(format!(
                    "the node owning this workflow could not be reached: {}",
                    status.message()
                ))))
            }
            // Answered by the owner's router before any handler ran, so
            // nothing happened there. Seen during a rolling upgrade, when the
            // owner has not been upgraded yet.
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                record(rpc, "unimplemented");
                warn!(
                    peer = %address,
                    rpc,
                    "the workflow's owner does not know this request; handing it back"
                );
                Relayed::Unsupported(fallback)
            }
            Err(status) => {
                record(rpc, "owner_error");
                Relayed::Answered(Err(status))
            }
        }
    }

    /// The channel to `address`, opening one if there is none, or joining the
    /// attempt already under way.
    async fn channel(&self, address: &str) -> Result<Channel, ConnectFailure> {
        self.prune_if_due();
        // The map guard is released before any await: a DashMap shard lock held
        // across one blocks every other relay hashing to that shard.
        let connecting = {
            let slot = self.channels.get(address).map(|slot| slot.clone());
            match slot {
                Some(Slot::Ready(channel)) => return Ok(channel),
                Some(Slot::Connecting(connecting)) => connecting,
                None => {
                    let endpoint = self.endpoint(address).map_err(|e| ConnectFailure {
                        refused: false,
                        message: format!("peer address is not a usable endpoint: {e}"),
                    })?;
                    let connecting = connect(endpoint, self.config.connect_timeout)
                        .boxed()
                        .shared();
                    // Another relay may have started one in the meantime; join
                    // whichever is in the map.
                    let slot = self
                        .channels
                        .entry(address.to_string())
                        .or_insert(Slot::Connecting(connecting))
                        .clone();
                    match slot {
                        Slot::Ready(channel) => return Ok(channel),
                        Slot::Connecting(connecting) => connecting,
                    }
                }
            }
        };

        let result = connecting.clone().await;
        // Settle the slot, unless a later attempt already replaced this one.
        let current = self.channels.get(address).map(|slot| slot.clone());
        if let Some(Slot::Connecting(in_map)) = current {
            if in_map.ptr_eq(&connecting) {
                match &result {
                    Ok(channel) => {
                        debug!(peer = %address, "connected to peer for owner forwarding");
                        self.channels
                            .insert(address.to_string(), Slot::Ready(channel.clone()));
                    }
                    Err(_) => {
                        self.channels.remove(address);
                    }
                }
            }
        }
        result
    }

    /// Drop channels to addresses no live member has any more. At most every
    /// [`PRUNE_EVERY`], so a relay pays for it rarely.
    fn prune_if_due(&self) {
        {
            let mut last = self.last_prune.lock().unwrap_or_else(|e| e.into_inner());
            if last.elapsed() < PRUNE_EVERY {
                return;
            }
            *last = Instant::now();
        }
        self.prune_channels();
    }

    /// Drop every cached channel whose address is not a live member's.
    pub fn prune_channels(&self) {
        let live: std::collections::HashSet<String> = self
            .directory
            .live_nodes()
            .iter()
            .filter_map(|node| self.directory.address_of(node))
            .collect();
        self.channels.retain(|address, _| live.contains(address));
    }

    fn endpoint(&self, address: &str) -> Result<Endpoint, tonic::transport::Error> {
        let uri = if address.starts_with("http://") || address.starts_with("https://") {
            address.to_string()
        } else if self.config.tls.is_some() {
            format!("https://{address}")
        } else {
            format!("http://{address}")
        };
        let mut endpoint = Endpoint::from_shared(uri)?
            .connect_timeout(self.config.connect_timeout)
            .tcp_nodelay(true)
            .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
            .keep_alive_while_idle(true);
        if let Some(tls) = &self.config.tls {
            endpoint = endpoint.tls_config(tls.clone())?;
        }
        Ok(endpoint)
    }
}

/// Open a connection to `endpoint`: TCP, then TLS if configured, then HTTP/2,
/// all within `timeout`. A refused connection is tried once more — it may be a
/// listener restarting — and anything else is reported as it is.
async fn connect(endpoint: Endpoint, timeout: Duration) -> Result<Channel, ConnectFailure> {
    let mut attempt = 1;
    loop {
        let failure = match tokio::time::timeout(timeout, endpoint.connect()).await {
            Ok(Ok(channel)) => return Ok(channel),
            Ok(Err(e)) => ConnectFailure {
                refused: error_chain_is_refused(&e),
                message: error_chain(&e),
            },
            Err(_) => ConnectFailure {
                refused: false,
                message: format!("connecting timed out after {timeout:?}"),
            },
        };
        if failure.refused && attempt < CONNECT_ATTEMPTS {
            attempt += 1;
            tokio::time::sleep(CONNECT_RETRY_PAUSE).await;
            continue;
        }
        return Err(failure);
    }
}

/// How long a relay may take: what is left of the caller's deadline after the
/// time already spent here, capped by the relay timeout. `None` when the
/// caller's deadline has already passed.
fn relay_budget(
    caller_deadline: Option<Duration>,
    spent: Duration,
    relay_timeout: Duration,
) -> Option<Duration> {
    let budget = match caller_deadline {
        Some(deadline) => deadline.saturating_sub(spent).min(relay_timeout),
        None => relay_timeout,
    };
    (!budget.is_zero()).then_some(budget)
}

/// The workflow id in the form every shard lookup hashes, so a request that
/// spells a UUID differently still resolves to the same shard.
fn canonical_workflow_id(workflow_id: &str) -> String {
    uuid::Uuid::parse_str(workflow_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| workflow_id.to_string())
}

/// Build the request sent to the owner: the caller's body, the caller's
/// credentials and tracing headers, the relay mark, and the deadline.
fn relay_request<T>(
    body: T,
    incoming: &MetadataMap,
    this_node: &NodeId,
    timeout: Duration,
) -> tonic::Request<T> {
    let mut request = tonic::Request::new(body);
    let outgoing = request.metadata_mut();
    for key in RELAYED_METADATA {
        for value in incoming.get_all(*key) {
            outgoing.append(*key, value.clone());
        }
    }
    let mark = MetadataValue::try_from(this_node.as_str())
        .unwrap_or_else(|_| MetadataValue::from_static("1"));
    outgoing.insert(FORWARDED_BY_HEADER, mark);
    request.set_timeout(timeout);
    request
}

/// The deadline the caller gave, from its `grpc-timeout` header.
fn incoming_timeout(metadata: &MetadataMap) -> Option<Duration> {
    let raw = metadata.get("grpc-timeout")?.to_str().ok()?;
    parse_grpc_timeout(raw)
}

/// `grpc-timeout` is at most eight digits and a unit: H, M, S, m, u or n.
fn parse_grpc_timeout(raw: &str) -> Option<Duration> {
    if raw.len() < 2 || raw.len() > 9 {
        return None;
    }
    let (digits, unit) = raw.split_at(raw.len() - 1);
    let value: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(value.checked_mul(3600)?),
        "M" => Duration::from_secs(value.checked_mul(60)?),
        "S" => Duration::from_secs(value),
        "m" => Duration::from_millis(value),
        "u" => Duration::from_micros(value),
        "n" => Duration::from_nanos(value),
        _ => return None,
    })
}

/// Whether a failed call failed in the transport rather than being answered by
/// the owner. An answer from the owner — even `UNAVAILABLE` — carries no
/// transport error as its source.
fn is_transport_failure(status: &Status) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(status);
    while let Some(error) = source {
        if error.is::<tonic::transport::Error>() || error.is::<std::io::Error>() {
            return true;
        }
        source = error.source();
    }
    false
}

/// Whether the transport failure was a connection refused: the owner's process
/// is gone, and nothing was sent.
fn is_connection_refused(status: &Status) -> bool {
    std::error::Error::source(status).is_some_and(error_chain_is_refused)
}

/// Whether `error` or anything under it is an I/O "connection refused".
fn error_chain_is_refused(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = current {
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::ConnectionRefused {
                return true;
            }
        }
        current = error.source();
    }
    false
}

/// `error` and its causes, for a log line: a transport error's own message is
/// usually just "transport error".
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![error.to_string()];
    let mut current = error.source();
    while let Some(error) = current {
        parts.push(error.to_string());
        current = error.source();
    }
    parts.join(": ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_cluster::{ShardId, StaticCluster};

    /// A fixed cluster whose peers answer at `<name>:50051`, except those
    /// listed as having registered a bind address.
    struct Cluster {
        view: StaticCluster,
        undialable: Vec<NodeId>,
    }

    impl Membership for Cluster {
        fn this_node(&self) -> &NodeId {
            self.view.this_node()
        }
        fn live_nodes(&self) -> Vec<NodeId> {
            self.view.live_nodes()
        }
    }
    impl ShardMap for Cluster {
        fn shard_of(&self, workflow_id: &str) -> ShardId {
            self.view.shard_of(workflow_id)
        }
        fn owner_of(&self, shard: ShardId) -> Option<NodeId> {
            self.view.owner_of(shard)
        }
        fn owns(&self, shard: ShardId) -> bool {
            self.view.owns(shard)
        }
    }
    impl PeerDirectory for Cluster {
        fn address_of(&self, node: &NodeId) -> Option<String> {
            if self.undialable.contains(node) {
                None
            } else {
                Some(format!("{}:50051", node.as_str()))
            }
        }
    }

    fn forwarder(this: &str, members: &[&str], undialable: &[&str]) -> OwnerForwarder {
        let view = StaticCluster::new(
            NodeId::new(this),
            members.iter().map(|m| NodeId::new(*m)).collect(),
            orcher_cluster::DEFAULT_VSHARDS,
        );
        let cluster = Cluster {
            view,
            undialable: undialable.iter().map(|n| NodeId::new(*n)).collect(),
        };
        OwnerForwarder::new(Arc::new(cluster), ForwardConfig::default())
    }

    /// A workflow id whose owner, from `this`'s view, is `owner`.
    fn workflow_owned_by(f: &OwnerForwarder, owner: &str) -> String {
        (0..10_000)
            .map(|_| uuid::Uuid::new_v4().to_string())
            .find(|wf| f.directory.owner_of_workflow(wf) == Some(NodeId::new(owner)))
            .expect("some workflow hashes into the owner's range")
    }

    #[test]
    fn a_workflow_this_node_owns_is_handled_here() {
        let f = forwarder("a", &["a", "b"], &[]);
        let wf = workflow_owned_by(&f, "a");
        assert_eq!(f.route(&wf, &MetadataMap::new()), Route::Local);
    }

    #[test]
    fn a_workflow_another_node_owns_goes_to_that_node() {
        let f = forwarder("a", &["a", "b"], &[]);
        let wf = workflow_owned_by(&f, "b");
        assert_eq!(
            f.route(&wf, &MetadataMap::new()),
            Route::Owner {
                node: NodeId::new("b"),
                address: "b:50051".to_string()
            }
        );
    }

    #[test]
    fn a_relayed_request_is_never_relayed_again() {
        let f = forwarder("a", &["a", "b"], &[]);
        let wf = workflow_owned_by(&f, "b");
        let mut metadata = MetadataMap::new();
        metadata.insert(FORWARDED_BY_HEADER, "b".parse().unwrap());
        assert_eq!(f.route(&wf, &metadata), Route::Local);
    }

    #[test]
    fn an_owner_without_a_dialable_address_is_handled_here() {
        let f = forwarder("a", &["a", "b"], &["b"]);
        let wf = workflow_owned_by(&f, "b");
        assert_eq!(f.route(&wf, &MetadataMap::new()), Route::Local);
    }

    #[test]
    fn an_unknown_live_set_is_handled_here() {
        let f = forwarder("a", &[], &[]);
        assert_eq!(
            f.route(&uuid::Uuid::new_v4().to_string(), &MetadataMap::new()),
            Route::Local
        );
    }

    #[test]
    fn a_differently_spelled_uuid_routes_like_the_canonical_one() {
        let f = forwarder("a", &["a", "b"], &[]);
        let wf = workflow_owned_by(&f, "b");
        let upper = wf.to_uppercase();
        assert_eq!(
            f.route(&upper, &MetadataMap::new()),
            f.route(&wf, &MetadataMap::new())
        );
    }

    #[test]
    fn the_callers_credentials_and_tenant_ride_along_and_nothing_else() {
        let mut incoming = MetadataMap::new();
        incoming.insert("authorization", "Bearer orch_key".parse().unwrap());
        incoming.insert(ORGANIZATION_METADATA_KEY, "org-1".parse().unwrap());
        incoming.insert("x-org-id", "org-1".parse().unwrap());
        incoming.insert("traceparent", "00-abc-def-01".parse().unwrap());
        incoming.insert("user-agent", "sdk/1".parse().unwrap());
        incoming.insert("x-unrelated", "nope".parse().unwrap());

        let request = relay_request(
            (),
            &incoming,
            &NodeId::new("node-a"),
            Duration::from_secs(7),
        );
        let out = request.metadata();

        assert_eq!(out.get("authorization").unwrap(), "Bearer orch_key");
        assert_eq!(out.get(ORGANIZATION_METADATA_KEY).unwrap(), "org-1");
        assert_eq!(out.get("x-org-id").unwrap(), "org-1");
        assert_eq!(out.get("traceparent").unwrap(), "00-abc-def-01");
        assert_eq!(out.get(FORWARDED_BY_HEADER).unwrap(), "node-a");
        assert!(out.get("grpc-timeout").is_some(), "the deadline travels");
        assert!(out.get("user-agent").is_none());
        assert!(out.get("x-unrelated").is_none());
    }

    #[test]
    fn a_callers_forwarded_mark_is_replaced_not_trusted() {
        // Even if a caller sent the mark itself, the relay writes its own.
        let mut incoming = MetadataMap::new();
        incoming.insert(FORWARDED_BY_HEADER, "spoofed".parse().unwrap());
        let request = relay_request(
            (),
            &incoming,
            &NodeId::new("node-a"),
            Duration::from_secs(1),
        );
        let marks: Vec<_> = request
            .metadata()
            .get_all(FORWARDED_BY_HEADER)
            .iter()
            .collect();
        assert_eq!(marks.len(), 1);
        assert_eq!(marks[0], "node-a");
    }

    #[test]
    fn grpc_timeouts_parse_in_every_unit() {
        assert_eq!(parse_grpc_timeout("2S"), Some(Duration::from_secs(2)));
        assert_eq!(
            parse_grpc_timeout("1500m"),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(parse_grpc_timeout("3M"), Some(Duration::from_secs(180)));
        assert_eq!(parse_grpc_timeout("1H"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_grpc_timeout("250u"), Some(Duration::from_micros(250)));
        assert_eq!(parse_grpc_timeout("99n"), Some(Duration::from_nanos(99)));
        assert_eq!(parse_grpc_timeout("S"), None);
        assert_eq!(parse_grpc_timeout("123456789S"), None);
        assert_eq!(parse_grpc_timeout("10x"), None);
    }

    #[test]
    fn an_answer_from_the_owner_is_not_a_transport_failure() {
        assert!(!is_transport_failure(&Status::unavailable(
            "shard ownership lost"
        )));
        assert!(!is_transport_failure(&Status::permission_denied("no")));
    }

    #[tokio::test]
    async fn a_call_on_a_dead_connection_is_a_refused_transport_failure() {
        // What a cached channel produces once its peer has gone away.
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let status = ExecutionServiceClient::new(channel)
            .complete_workflow_execution(CompleteWorkflowExecutionRequest::default())
            .await
            .expect_err("nothing listens there");
        assert!(is_transport_failure(&status), "{status:?}");
        assert!(is_connection_refused(&status), "{status:?}");
    }

    #[tokio::test]
    async fn an_owner_that_cannot_be_reached_hands_the_request_back() {
        // Nothing listens on port 1: the connection is refused, so nothing was
        // sent and the request must come back to be handled locally.
        let f = forwarder("a", &["a", "b"], &[]);
        let body = CompleteWorkflowExecutionRequest {
            workflow_id: "wf".to_string(),
            ..Default::default()
        };
        let b = NodeId::new("b");
        match f
            .complete_workflow_execution(
                &b,
                "127.0.0.1:1",
                &MetadataMap::new(),
                body,
                Instant::now(),
            )
            .await
        {
            Relayed::NotSent(body) => assert_eq!(body.workflow_id, "wf"),
            Relayed::Answered(answer) => panic!("nothing listens there, yet: {answer:?}"),
            Relayed::Unsupported(_) => panic!("nothing listens there to say so"),
        }
        // And the owner is not dialed again for a while: its requests are
        // handled here, and range-strict ownership may take its shards.
        let wf = workflow_owned_by(&f, "b");
        assert_eq!(f.route(&wf, &MetadataMap::new()), Route::Local);
        assert_eq!(f.peers().address_of(&b), None);
    }

    #[tokio::test]
    async fn a_refused_call_on_a_cached_channel_hands_the_request_back() {
        // A channel cached while the owner was up, called after it died.
        let f = forwarder("a", &["a", "b"], &[]);
        f.channels.insert(
            "127.0.0.1:1".to_string(),
            Slot::Ready(Endpoint::from_static("http://127.0.0.1:1").connect_lazy()),
        );
        let body = CompleteWorkflowExecutionRequest {
            workflow_id: "wf".to_string(),
            ..Default::default()
        };
        match f
            .complete_workflow_execution(
                &NodeId::new("b"),
                "127.0.0.1:1",
                &MetadataMap::new(),
                body,
                Instant::now(),
            )
            .await
        {
            Relayed::NotSent(body) => assert_eq!(body.workflow_id, "wf"),
            Relayed::Answered(answer) => panic!("refused, so nothing was sent, yet: {answer:?}"),
            Relayed::Unsupported(_) => panic!("refused, so nothing answered"),
        }
        assert!(f.channels.is_empty(), "the dead channel is dropped");
    }

    /// A peer that accepts connections and then says nothing, or `greeting`
    /// and hangs up. Returns its address and how many connections it took.
    async fn peer(
        greeting: Option<&'static [u8]>,
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut stream, _)) = listener.accept().await {
                count.fetch_add(1, Ordering::SeqCst);
                match greeting {
                    Some(bytes) => {
                        let _ = stream.write_all(bytes).await;
                        drop(stream);
                    }
                    None => held.push(stream),
                }
            }
        });
        (address, accepted)
    }

    fn body() -> CompleteWorkflowExecutionRequest {
        CompleteWorkflowExecutionRequest {
            workflow_id: "wf".to_string(),
            ..Default::default()
        }
    }

    fn forwarder_with(config: ForwardConfig) -> OwnerForwarder {
        let mut f = forwarder("a", &["a", "b"], &[]);
        f.config = config;
        f
    }

    #[tokio::test]
    async fn a_failed_tls_handshake_answers_retryably_and_marks_nothing() {
        // Something answers, but not with TLS: a misconfigured peer, not a
        // dead one. Its shards must stay its own.
        let (address, _) = peer(Some(b"this is not a TLS server\r\n")).await;
        let f = forwarder_with(ForwardConfig {
            tls: Some(ClientTlsConfig::new().domain_name("peer.test")),
            ..ForwardConfig::default()
        });
        let b = NodeId::new("b");
        match f
            .complete_workflow_execution(&b, &address, &MetadataMap::new(), body(), Instant::now())
            .await
        {
            Relayed::Answered(Err(status)) => assert_eq!(status.code(), tonic::Code::Unavailable),
            other => panic!("a handshake failure must be answered as retryable: {other:?}"),
        }
        assert!(
            f.peers().address_of(&b).is_some(),
            "a TLS failure must not mark the owner unreachable"
        );
    }

    #[tokio::test]
    async fn a_connect_timeout_answers_retryably_and_marks_nothing() {
        // The peer accepts and never completes the handshake: slow, not gone.
        let (address, _) = peer(None).await;
        let f = forwarder_with(ForwardConfig {
            connect_timeout: Duration::from_millis(200),
            tls: Some(ClientTlsConfig::new().domain_name("peer.test")),
            ..ForwardConfig::default()
        });
        let b = NodeId::new("b");
        let started = Instant::now();
        match f
            .complete_workflow_execution(&b, &address, &MetadataMap::new(), body(), Instant::now())
            .await
        {
            Relayed::Answered(Err(status)) => assert_eq!(status.code(), tonic::Code::Unavailable),
            other => panic!("a connect timeout must be answered as retryable: {other:?}"),
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "bounded by the connect timeout"
        );
        assert!(
            f.peers().address_of(&b).is_some(),
            "a timeout must not mark the owner unreachable"
        );
    }

    #[tokio::test]
    async fn a_hung_owner_costs_the_relay_timeout_and_its_channel() {
        // Accepts, never answers. The caller allowed a minute; the relay must
        // give up at its own cap, drop the channel, and not mark the owner.
        let (address, _) = peer(None).await;
        let f = forwarder_with(ForwardConfig {
            default_timeout: Duration::from_millis(300),
            ..ForwardConfig::default()
        });
        f.channels.insert(
            address.clone(),
            Slot::Ready(
                Endpoint::from_shared(format!("http://{address}"))
                    .unwrap()
                    .connect_lazy(),
            ),
        );
        let mut metadata = MetadataMap::new();
        metadata.insert("grpc-timeout", "60S".parse().unwrap());
        let b = NodeId::new("b");
        let started = Instant::now();
        match f
            .complete_workflow_execution(&b, &address, &metadata, body(), Instant::now())
            .await
        {
            Relayed::Answered(Err(status)) => assert_eq!(status.code(), tonic::Code::Unavailable),
            other => panic!("a hung owner must be answered as retryable: {other:?}"),
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "took {:?}: the caller's minute, not the relay cap",
            started.elapsed()
        );
        assert!(f.channels.is_empty(), "the hung channel is dropped");
        assert!(f.peers().address_of(&b).is_some());
    }

    #[test]
    fn the_relay_gets_what_is_left_of_the_callers_deadline_at_most() {
        let cap = Duration::from_secs(30);
        assert_eq!(
            relay_budget(Some(Duration::from_secs(10)), Duration::from_secs(4), cap),
            Some(Duration::from_secs(6))
        );
        assert_eq!(
            relay_budget(Some(Duration::from_secs(120)), Duration::ZERO, cap),
            Some(cap)
        );
        assert_eq!(relay_budget(None, Duration::from_secs(5), cap), Some(cap));
        assert_eq!(
            relay_budget(Some(Duration::from_secs(1)), Duration::from_secs(2), cap),
            None,
            "a deadline already passed leaves nothing to relay with"
        );
    }

    #[tokio::test]
    async fn concurrent_relays_to_one_peer_open_one_connection() {
        use std::sync::atomic::Ordering;
        let (address, accepted) = peer(None).await;
        let f = Arc::new(forwarder_with(ForwardConfig {
            connect_timeout: Duration::from_millis(500),
            tls: Some(ClientTlsConfig::new().domain_name("peer.test")),
            ..ForwardConfig::default()
        }));
        let attempts: Vec<_> = (0..20)
            .map(|_| {
                let f = f.clone();
                let address = address.clone();
                tokio::spawn(async move { f.channel(&address).await.is_ok() })
            })
            .collect();
        for attempt in attempts {
            let _ = attempt.await;
        }
        assert_eq!(accepted.load(Ordering::SeqCst), 1, "one attempt, shared");
    }

    #[test]
    fn channels_to_addresses_no_member_has_are_dropped() {
        let f = forwarder("a", &["a", "b"], &[]);
        let lazy = |a: &str| {
            Slot::Ready(
                Endpoint::from_shared(format!("http://{a}"))
                    .unwrap()
                    .connect_lazy(),
            )
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                f.channels.insert("b:50051".to_string(), lazy("b:50051"));
                f.channels
                    .insert("gone:50051".to_string(), lazy("gone:50051"));
                f.prune_channels();
            });
        assert!(f.channels.contains_key("b:50051"));
        assert!(!f.channels.contains_key("gone:50051"));
    }
}
