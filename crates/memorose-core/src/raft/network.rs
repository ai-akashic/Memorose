use tonic::transport::Channel;
use tonic::Request;
use openraft::network::RPCOption;
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use openraft::error::{RPCError, RaftError, InstallSnapshotError};
use openraft::BasicNode;
use openraft::{RaftNetwork, RaftNetworkFactory};

use super::types::MemoroseTypeConfig;

// Include the generated gRPC code
pub mod raft_proto {
    tonic::include_proto!("raft");
}

use raft_proto::raft_service_client::RaftServiceClient;
use raft_proto::RaftRequest;

pub struct MemoroseNetworkConnection {
    endpoint: String,
    client: Option<RaftServiceClient<Channel>>,
}

impl MemoroseNetworkConnection {
    pub fn new(endpoint: String) -> Self {
        Self { endpoint, client: None }
    }

    async fn get_client(&mut self) -> Result<&mut RaftServiceClient<Channel>, tonic::Status> {
        if self.client.is_none() {
            let channel = Channel::from_shared(self.endpoint.clone())
                .map_err(|e| tonic::Status::internal(format!("Invalid endpoint: {}", e)))?
                .connect()
                .await
                .map_err(|e| tonic::Status::unavailable(format!("Connect failed: {}", e)))?;
            self.client = Some(RaftServiceClient::new(channel));
        }
        Ok(self.client.as_mut().unwrap())
    }
}

impl RaftNetwork<MemoroseTypeConfig> for MemoroseNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MemoroseTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let data = serde_json::to_vec(&rpc).map_err(to_rpc_err)?;
        let request = Request::new(RaftRequest { data });

        let client = self.get_client().await.map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;
        let response = client.append_entries(request).await
            .map_err(|e| { self.client = None; RPCError::Network(openraft::error::NetworkError::new(&e)) })?;

        let res_data = response.into_inner().data;
        let res: AppendEntriesResponse<u64> = serde_json::from_slice(&res_data).map_err(to_rpc_err)?;
        Ok(res)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MemoroseTypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>> {
        let data = serde_json::to_vec(&rpc).map_err(to_rpc_err_snapshot)?;
        let request = Request::new(RaftRequest { data });

        let client = self.get_client().await.map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;
        let response = client.install_snapshot(request).await
            .map_err(|e| { self.client = None; RPCError::Network(openraft::error::NetworkError::new(&e)) })?;

        let res_data = response.into_inner().data;
        let res: InstallSnapshotResponse<u64> = serde_json::from_slice(&res_data).map_err(to_rpc_err_snapshot)?;
        Ok(res)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let data = serde_json::to_vec(&rpc).map_err(to_rpc_err)?;
        let request = Request::new(RaftRequest { data });

        let client = self.get_client().await.map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;
        let response = client.vote(request).await
            .map_err(|e| { self.client = None; RPCError::Network(openraft::error::NetworkError::new(&e)) })?;

        let res_data = response.into_inner().data;
        let res: VoteResponse<u64> = serde_json::from_slice(&res_data).map_err(to_rpc_err)?;
        Ok(res)
    }
}

fn to_rpc_err<E: std::error::Error + 'static>(e: E) -> RPCError<u64, BasicNode, RaftError<u64>> {
    RPCError::Network(openraft::error::NetworkError::new(&e))
}

fn to_rpc_err_snapshot<E: std::error::Error + 'static>(e: E) -> RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>> {
    RPCError::Network(openraft::error::NetworkError::new(&e))
}

use raft_proto::raft_service_server::{RaftService, RaftServiceServer};
use raft_proto::RaftResponse;

pub struct MemoroseRaftServer {
    raft: super::MemoroseRaft,
}

impl MemoroseRaftServer {
    pub fn new(raft: super::MemoroseRaft) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl RaftService for MemoroseRaftServer {
    async fn append_entries(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftResponse>, tonic::Status> {
        let req: AppendEntriesRequest<MemoroseTypeConfig> = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
            
        let res = self.raft.append_entries(req).await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
            
        let data = serde_json::to_vec(&res).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftResponse { data }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftResponse>, tonic::Status> {
        let req: InstallSnapshotRequest<MemoroseTypeConfig> = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
            
        let res = self.raft.install_snapshot(req).await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
            
        let data = serde_json::to_vec(&res).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftResponse { data }))
    }

    async fn vote(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftResponse>, tonic::Status> {
        let req: VoteRequest<u64> = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
            
        let res = self.raft.vote(req).await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
            
        let data = serde_json::to_vec(&res).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftResponse { data }))
    }
}

pub async fn run_raft_server(addr: std::net::SocketAddr, raft: super::MemoroseRaft) -> Result<(), tonic::transport::Error> {
    let service = MemoroseRaftServer::new(raft);
    tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(service))
        .serve(addr)
        .await
}

#[derive(Clone, Default)]
pub struct MemoroseNetworkFactory {}

impl RaftNetworkFactory<MemoroseTypeConfig> for MemoroseNetworkFactory {
    type Network = MemoroseNetworkConnection;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        let addr = format!("http://{}", node.addr);
        MemoroseNetworkConnection::new(addr)
    }
}
