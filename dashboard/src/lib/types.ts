export interface LoginResponse {
  token: string;
  expires_in: number;
}

export interface ShardStatus {
  shard_id: number;
  raft_node_id: number;
  raft_state: "Leader" | "Follower" | "Candidate";
  current_leader: number | null;
  current_term: number;
  last_log_index: number;
  last_applied: number;
  replication_lag: number;
  voters: number[];
  learners: number[];
}

// Single-shard backward-compatible format
export interface ClusterStatusSingle {
  node_id: number;
  raft_node_id: number;
  shard_id: number;
  raft_state: "Leader" | "Follower" | "Candidate";
  current_leader: number | null;
  current_term: number;
  last_log_index: number;
  last_applied: number;
  replication_lag: number;
  voters: number[];
  learners: number[];
  snapshot_policy_logs: number;
  config: {
    heartbeat_interval_ms: number;
    election_timeout_min_ms: number;
    election_timeout_max_ms: number;
  };
}

// Multi-shard format
export interface ClusterStatusSharded {
  physical_node_id: number;
  shard_count: number;
  shards: ShardStatus[];
  config: {
    heartbeat_interval_ms: number;
    election_timeout_min_ms: number;
    election_timeout_max_ms: number;
  };
}

// Union type - the API returns one or the other
export type ClusterStatus = ClusterStatusSingle | ClusterStatusSharded;

export function isShardedCluster(status: ClusterStatus): status is ClusterStatusSharded {
  return "shards" in status;
}

export interface Stats {
  total_events: number;
  pending_events: number;
  total_memory_units: number;
  total_edges: number;
  memory_by_level: {
    l1: number;
    l2: number;
  };
  uptime_seconds: number;
}

export interface MemoryItem {
  id: string;
  user_id: string;
  agent_id?: string | null;
  app_id: string;
  memory_type?: "factual" | "procedural";
  content: string;
  level: number;
  importance: number;
  keywords: string[];
  access_count: number;
  last_accessed_at: string;
  transaction_time: string;
  reference_count: number;
  has_assets: boolean;
  item_type?: "memory" | "event";
}

export interface MemoryListResponse {
  items: MemoryItem[];
  total: number;
  page: number;
  limit: number;
}

export interface MemoryUnit {
  id: string;
  user_id: string;
  agent_id: string | null;
  app_id: string;
  stream_id: string;
  memory_type: "factual" | "procedural";
  content: string;
  keywords: string[];
  importance: number;
  level: number;
  transaction_time: string;
  valid_time: string | null;
  last_accessed_at: string;
  access_count: number;
  references: string[];
  assets: Array<{
    storage_key: string;
    original_name: string;
    asset_type: string;
  }>;
}

export interface GraphNode {
  id: string;
  label: string;
  level: number;
  importance: number;
  user_id?: string;
}

export interface GraphEdge {
  source: string;
  target: string;
  relation: string;
  weight: number;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
  stats: {
    node_count: number;
    edge_count: number;
    relation_distribution: Record<string, number>;
  };
}

export interface SearchResult {
  unit: MemoryUnit;
  score: number;
}

export interface SearchResponse {
  results: SearchResult[];
  query_time_ms: number;
}

export interface AppConfig {
  raft: Record<string, unknown>;
  worker: Record<string, unknown>;
  llm: Record<string, unknown>;
  storage: Record<string, unknown>;
}

export interface VersionInfo {
  version: string;
  build_time: string;
  features: string[];
}

export interface AgentSummary {
  agent_id: string;
  total_memories: number;
  l1_count: number;
  l2_count: number;
  total_events: number;
  last_activity: number | null;
}

export interface AgentListResponse {
  agents: AgentSummary[];
  total_count: number;
}

export interface L3Task {
  task_id: string;
  org_id?: string | null;
  user_id: string;
  agent_id?: string | null;
  app_id: string;
  parent_id?: string | null;
  title: string;
  description: string;
  status: 'Pending' | 'InProgress' | { Blocked: string } | 'Completed' | { Failed: string } | 'Cancelled';
  progress: number;
  dependencies: string[];
  context_refs: string[];
  created_at: string;
  updated_at: string;
  result_summary?: string | null;
}

export interface L3TaskTree {
  task: L3Task;
  children: L3TaskTree[];
}

export interface GoalTree {
  goal: MemoryUnit;
  tasks: L3TaskTree[];
}

export interface AppStats {
  app_id: string;
  overview: {
    total_events: number;
    total_users: number;
    total_memories: number;
    l1_count: number;
    l2_count: number;
    memory_pipeline_status: string;
    avg_memories_per_user: number;
  };
  users: Array<{
    user_id: string;
    event_count: number;
    memory_count: number;
    last_activity: number | null;
  }>;
  recent_activity: Array<{
    timestamp: number;
    user_id: string;
    event_type: string;
    stream_id: string;
  }>;
  performance: {
    total_storage_bytes: number;
    avg_event_size_bytes: number;
    l1_generation_rate: number;
    l2_generation_rate: number;
  };
}

export interface AppSummary {
  app_id: string;
  total_events: number;
  total_users: number;
  total_memories: number;
}

export interface RetrieveRequest {
  query: string;
  limit?: number;
  min_score?: number;
  graph_depth?: number;
  valid_time_start?: string;
  valid_time_end?: string;
  as_of?: string;
}

export type RetrieveResponse = SearchResponse;

export interface AddEdgeRequest {
  source_id: string;
  target_id: string;
  relation: string;
  weight?: number;
}

export interface PendingCountResponse {
  pending: number;
}

export interface ClusterInitResponse {
  status: string;
  node_id?: number;
}

export interface ClusterJoinRequest {
  node_id: number;
  address: string;
}

export interface ClusterJoinResponse {
  status: string;
}

export type ReadyTask = L3Task;
