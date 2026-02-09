use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use memorose_common::Event;
use std::fmt;

/// The application data request type which the `Raft` node can receive.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientRequest {
    /// Ingest a new event into the system.
    IngestEvent(Event),
    /// Update or add an edge in the knowledge graph.
    UpdateGraph(memorose_common::GraphEdge),
    // Future: etc.
}

/// The application data response type which the `Raft` node returns.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientResponse {
    pub success: bool,
    // Future: return IDs, error messages, etc.
}

/// The implementation of the `RaftTypeConfig` trait for Memorose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct MemoroseTypeConfig;

impl fmt::Display for MemoroseTypeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoroseTypeConfig")
    }
}

impl openraft::RaftTypeConfig for MemoroseTypeConfig {
    type D = ClientRequest;
    type R = ClientResponse;
    type NodeId = u64;
    type Node = BasicNode;
    type Entry = openraft::Entry<MemoroseTypeConfig>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}
