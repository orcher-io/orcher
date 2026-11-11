//! What a caller may do: scopes.
//!
//! One vocabulary for every credential: an API key records the scopes it was
//! issued with, an identity provider's token carries them in a claim, and an
//! embedding application (cloud) maps its roles onto them. The engine checks
//! the scope each call needs ([`for_grpc_method`], [`for_rest`]) against the
//! caller's ([`Scopes::allows`]), on every API it serves.
//!
//! Scopes are named `resource:action`, and only ever added: a release that
//! needs a new permission adds a scope, and credentials issued before it
//! simply do not hold it.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

/// One permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    /// Read workflows: their status, results, journals and listings, queries,
    /// and the namespaces they live in.
    WorkflowsRead,
    /// Change workflows: start, send events, update, cancel, terminate,
    /// reset, batches, and invoke actors. Includes [`Scope::WorkflowsRead`].
    WorkflowsWrite,
    /// Run work: poll for and complete workflow activations, tasks and actor
    /// operations, heartbeat, and register as a worker.
    WorkersRun,
    /// Create, change and delete namespaces.
    NamespacesManage,
    /// Create, list and revoke API keys.
    ApiKeysManage,
}

impl Scope {
    /// Every scope there is.
    pub const ALL: [Scope; 5] = [
        Scope::WorkflowsRead,
        Scope::WorkflowsWrite,
        Scope::WorkersRun,
        Scope::NamespacesManage,
        Scope::ApiKeysManage,
    ];

    /// The scope's name, as credentials carry it.
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::WorkflowsRead => "workflows:read",
            Scope::WorkflowsWrite => "workflows:write",
            Scope::WorkersRun => "workers:run",
            Scope::NamespacesManage => "namespaces:manage",
            Scope::ApiKeysManage => "api-keys:manage",
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Scope {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        Scope::ALL
            .into_iter()
            .find(|scope| scope.as_str() == s)
            .ok_or(())
    }
}

/// The scopes a caller holds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Scopes(BTreeSet<Scope>);

impl Scopes {
    /// Every scope: an unauthenticated deployment's caller, and a credential
    /// issued before scopes existed.
    pub fn all() -> Self {
        Self(Scope::ALL.into_iter().collect())
    }

    /// No scope at all.
    pub fn none() -> Self {
        Self::default()
    }

    /// The scopes named in `names`: `*` for all of them. A name this engine
    /// does not know (a scope a newer release added) grants nothing here, and
    /// is returned in the second half so the caller can say so.
    pub fn parse<'a>(names: impl IntoIterator<Item = &'a str>) -> (Self, Vec<String>) {
        let mut scopes = BTreeSet::new();
        let mut unknown = Vec::new();
        for name in names {
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            if name == "*" {
                return (Self::all(), unknown);
            }
            match name.parse() {
                Ok(scope) => {
                    scopes.insert(scope);
                }
                Err(()) => unknown.push(name.to_string()),
            }
        }
        (Self(scopes), unknown)
    }

    /// Whether these scopes grant `required`.
    pub fn allows(&self, required: Scope) -> bool {
        self.0.contains(&required)
            || (required == Scope::WorkflowsRead && self.0.contains(&Scope::WorkflowsWrite))
    }

    /// Whether these scopes grant everything `other` does: a credential with
    /// `self` may issue one with `other`, and no more.
    pub fn covers(&self, other: &Scopes) -> bool {
        other.0.iter().all(|scope| self.allows(*scope))
    }

    /// The scopes' names, in a stable order.
    pub fn names(&self) -> Vec<&'static str> {
        self.0.iter().map(|scope| scope.as_str()).collect()
    }
}

/// The scope a gRPC call needs, from its path (`/orcher.v1.Service/Method`);
/// `None` for a method this engine does not know, which is refused rather
/// than let through.
pub fn for_grpc_method(path: &str) -> Option<Scope> {
    let (service, method) = path.strip_prefix('/')?.split_once('/')?;
    let service = service.strip_prefix("orcher.v1.")?;
    use Scope::*;
    Some(match (service, method) {
        (
            "WorkflowService",
            "StartWorkflow"
            | "SendEvent"
            | "SendEventWithStartWorkflow"
            | "CancelWorkflow"
            | "TerminateWorkflow"
            | "ResetWorkflow"
            | "UpdateWorkflow"
            | "BatchCancelWorkflow"
            | "BatchTerminateWorkflow"
            | "BatchSendEvent"
            | "BatchStartWorkflow",
        ) => WorkflowsWrite,
        ("WorkflowService", "QueryWorkflow" | "GetWorkflowStatus" | "GetWorkflowResult") => {
            WorkflowsRead
        }
        ("ExecutionService", _) if EXECUTION_METHODS.contains(&method) => WorkersRun,
        ("QueryService", _) if QUERY_METHODS.contains(&method) => WorkflowsRead,
        ("NamespaceService", "GetNamespace" | "ListNamespaces") => WorkflowsRead,
        (
            "NamespaceService",
            "CreateNamespace" | "UpdateNamespace" | "DeprecateNamespace" | "DeleteNamespace",
        ) => NamespacesManage,
        ("WorkerService", "RegisterWorker" | "WorkerHeartbeat" | "DeregisterWorker") => WorkersRun,
        ("ActorService", "InvokeOperation") => WorkflowsWrite,
        (
            "ActorService",
            "PollActorOperation"
            | "CompleteActorOperation"
            | "GetState"
            | "SetState"
            | "DeleteState"
            | "ListStateKeys"
            | "RegisterHandlers"
            | "Heartbeat",
        ) => WorkersRun,
        _ => return None,
    })
}

const EXECUTION_METHODS: [&str; 15] = [
    "PollWorkflowExecution",
    "PollTaskExecution",
    "PollWorkflowExecutions",
    "PollTaskExecutions",
    "CompleteWorkflowExecution",
    "FailWorkflowExecution",
    "CompleteTaskExecution",
    "FailTaskExecution",
    "CancelTaskExecution",
    "RecordTaskHeartbeat",
    "RecordTaskHeartbeats",
    "RespondQueryTask",
    "RespondUpdateTask",
    "ReleaseWorkflowExecution",
    "ShutdownWorker",
];

const QUERY_METHODS: [&str; 8] = [
    "ListWorkflows",
    "SearchWorkflows",
    "GetExecutionJournal",
    "GetExecutionLogs",
    "GetTaskExecutions",
    "DescribeWorkflowExecution",
    "CountWorkflows",
    "GetWorkflowExecutionStats",
];

/// The scope a REST call needs, from its HTTP method and path. Reading needs
/// [`Scope::WorkflowsRead`] and changing [`Scope::WorkflowsWrite`], except
/// for the resources that have their own scope.
pub fn for_rest(method: &http::Method, path: &str) -> Scope {
    let path = path.trim_end_matches('/');
    // A router nested under /api/v1 may hand its middleware the path without it.
    let resource = path
        .strip_prefix("/api/v1")
        .or_else(|| path.strip_prefix("/v1"))
        .unwrap_or(path)
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or("");
    let reads = matches!(*method, http::Method::GET | http::Method::HEAD);
    match resource {
        "api-keys" => Scope::ApiKeysManage,
        "namespaces" if !reads => Scope::NamespacesManage,
        _ if reads => Scope::WorkflowsRead,
        _ => Scope::WorkflowsWrite,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scope_reads_and_writes_as_its_name() {
        for scope in Scope::ALL {
            assert_eq!(scope.as_str().parse::<Scope>(), Ok(scope));
        }
        assert!("workflows:delete".parse::<Scope>().is_err());
    }

    #[test]
    fn writing_workflows_includes_reading_them_and_nothing_else() {
        let (write, _) = Scopes::parse(["workflows:write"]);
        assert!(write.allows(Scope::WorkflowsRead));
        assert!(write.allows(Scope::WorkflowsWrite));
        assert!(!write.allows(Scope::WorkersRun));
        let (read, _) = Scopes::parse(["workflows:read"]);
        assert!(!read.allows(Scope::WorkflowsWrite));
    }

    #[test]
    fn a_star_is_every_scope_and_an_unknown_name_none() {
        let (all, unknown) = Scopes::parse(["*"]);
        assert_eq!(all, Scopes::all());
        assert!(unknown.is_empty());
        let (some, unknown) = Scopes::parse(["workers:run", "tea:make", " "]);
        assert!(some.allows(Scope::WorkersRun));
        assert_eq!(some.names(), ["workers:run"]);
        assert_eq!(unknown, ["tea:make"]);
    }

    #[test]
    fn a_credential_may_issue_only_what_it_holds() {
        let (member, _) = Scopes::parse(["workflows:write", "workers:run"]);
        let (worker, _) = Scopes::parse(["workers:run"]);
        let (reader, _) = Scopes::parse(["workflows:read"]);
        assert!(member.covers(&worker));
        assert!(member.covers(&reader));
        assert!(!worker.covers(&member));
        assert!(!member.covers(&Scopes::all()));
        assert!(Scopes::all().covers(&member));
    }

    #[test]
    fn an_unknown_grpc_method_needs_a_scope_nothing_has() {
        assert_eq!(for_grpc_method("/orcher.v1.WorkflowService/Nope"), None);
        assert_eq!(for_grpc_method("/grpc.health.v1.Health/Check"), None);
        assert_eq!(for_grpc_method("nonsense"), None);
        assert_eq!(
            for_grpc_method("/orcher.v1.WorkflowService/StartWorkflow"),
            Some(Scope::WorkflowsWrite)
        );
        assert_eq!(
            for_grpc_method("/orcher.v1.ExecutionService/PollTaskExecutions"),
            Some(Scope::WorkersRun)
        );
    }

    /// Every method of every service the engine serves needs a scope: one
    /// added to the protocol without one here is refused, and this says so
    /// before a release does.
    #[test]
    fn every_method_the_engine_serves_needs_a_scope() {
        let metadata = std::process::Command::new(env!("CARGO"))
            .args(["metadata", "--format-version", "1"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .expect("cargo metadata");
        let metadata: serde_json::Value =
            serde_json::from_slice(&metadata.stdout).expect("metadata is JSON");
        let manifest = metadata["packages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["name"] == "orcher-proto")
            .and_then(|p| p["manifest_path"].as_str())
            .expect("orcher-proto in the dependency graph");
        let proto_dir = std::path::Path::new(manifest).with_file_name("proto");
        let mut methods = 0;
        for entry in std::fs::read_dir(&proto_dir).unwrap() {
            let text = std::fs::read_to_string(entry.unwrap().path()).unwrap();
            let mut service = None;
            for line in text.lines().map(str::trim) {
                if let Some(rest) = line.strip_prefix("service ") {
                    service = rest.split_whitespace().next().map(str::to_string);
                } else if let (Some(rest), Some(service)) = (line.strip_prefix("rpc "), &service) {
                    let method = rest.split(['(', ' ']).next().unwrap();
                    let path = format!("/orcher.v1.{service}/{method}");
                    assert!(for_grpc_method(&path).is_some(), "{path} has no scope");
                    methods += 1;
                }
            }
        }
        assert!(
            methods >= 55,
            "found only {methods} methods in {proto_dir:?}"
        );
    }

    #[test]
    fn a_rest_call_needs_the_scope_of_what_it_does() {
        use http::Method;
        // As a router nested under /api/v1 sees it.
        assert_eq!(
            for_rest(&Method::POST, "/namespaces"),
            Scope::NamespacesManage
        );
        assert_eq!(for_rest(&Method::POST, "/api-keys"), Scope::ApiKeysManage);
        assert_eq!(for_rest(&Method::GET, "/executions"), Scope::WorkflowsRead);
        assert_eq!(
            for_rest(&Method::GET, "/api/v1/executions"),
            Scope::WorkflowsRead
        );
        assert_eq!(
            for_rest(&Method::POST, "/api/v1/workflows/x/cancel"),
            Scope::WorkflowsWrite
        );
        assert_eq!(
            for_rest(&Method::GET, "/api/v1/namespaces"),
            Scope::WorkflowsRead
        );
        assert_eq!(
            for_rest(&Method::POST, "/api/v1/namespaces"),
            Scope::NamespacesManage
        );
        assert_eq!(
            for_rest(&Method::GET, "/api/v1/api-keys"),
            Scope::ApiKeysManage
        );
        assert_eq!(
            for_rest(&Method::DELETE, "/api/v1/api-keys/k1/"),
            Scope::ApiKeysManage
        );
    }
}
