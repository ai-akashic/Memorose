export interface LoginResponse {
  token: string;
  expires_in: number;
}

export interface ShardStatus {
  shard_id: number;
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
  memory_by_scope: {
    local: number;
    shared: number;
  };
  memory_by_domain: {
    agent: number;
    user: number;
    organization: number;
  };
  memory_by_level: {
    l1: number;
    l2: number;
  };
  memory_by_level_and_scope: {
    local: {
      l1: number;
      l2: number;
    };
    shared: {
      l1: number;
      l2: number;
    };
  };
  uptime_seconds: number;
}

export interface MemoryItem {
  id: string;
  user_id: string;
  agent_id?: string | null;
  memory_type?: "factual" | "procedural";
  content: string;
  level: number;
  importance: number;
  keywords: string[];
  access_count: number;
  reference_count: number;
  item_type?: "memory" | "event";
}

export interface MemoryListResponse {
  items: MemoryItem[];
  total: number;
  page: number;
  limit: number;
}

export interface DashboardMemoryDetail {
  id: string;
  org_id?: string | null;
  user_id: string;
  content: string;
  keywords: string[];
  importance: number;
  level: number;
  transaction_time: string;
  organization_knowledge?: OrganizationKnowledgeDetail;
}

export interface SearchMemoryUnit {
  id: string;
  memory_type: "factual" | "procedural";
  content: string;
  keywords: string[];
  level: number;
}

export interface OrganizationKnowledgeUnit {
  id: string;
  content: string;
  keywords: string[];
  transaction_time: string;
}

export interface OrganizationContribution {
  source_id: string;
  contributor_user_id: string;
  status: "candidate" | "active" | "revoked";
  source_memory_type?: "factual" | "procedural" | null;
  source_level?: number | null;
  source_keywords: string[];
  source_content_preview?: string | null;
  candidate_at?: string | null;
  activated_at?: string | null;
  approval_mode?: "auto" | null;
  approved_by?: string | null;
  revoked_at?: string | null;
}

export interface OrganizationKnowledgeMembership {
  source_id: string;
  contributor_user_id: string;
  source_memory_type?: "factual" | "procedural" | null;
  source_level?: number | null;
  source_keywords: string[];
  source_content_preview?: string | null;
  activated_at?: string | null;
  approval_mode?: "auto" | null;
  approved_by?: string | null;
  updated_at: string;
}

export interface OrganizationKnowledgeMembershipSummary {
  contributors: Array<{
    contributor_user_id: string;
    membership_count: number;
    source_ids: string[];
    source_memory_types: string[];
  }>;
  source_types: Array<{
    source_memory_type: string;
    membership_count: number;
    contributor_user_ids: string[];
  }>;
}

export interface OrganizationKnowledgeMembershipState {
  membership_count: number;
  summary: OrganizationKnowledgeMembershipSummary;
  memberships: OrganizationKnowledgeMembership[];
}

export interface OrganizationKnowledgeHistorySummary {
  contributors: Array<{
    contributor_user_id: string;
    contribution_count: number;
    candidate_contribution_count: number;
    active_contribution_count: number;
    revoked_contribution_count: number;
    source_ids: string[];
    source_memory_types: string[];
  }>;
  source_types: Array<{
    source_memory_type: string;
    contribution_count: number;
    candidate_contribution_count: number;
    active_contribution_count: number;
    revoked_contribution_count: number;
    contributor_user_ids: string[];
  }>;
}

export interface OrganizationKnowledgeHistory {
    contribution_count: number;
    candidate_contribution_count: number;
    active_contribution_count: number;
    revoked_contribution_count: number;
    summary: OrganizationKnowledgeHistorySummary;
    contributions: OrganizationContribution[];
}

export interface OrganizationKnowledgeDetail {
  membership: OrganizationKnowledgeMembershipState;
  history: OrganizationKnowledgeHistory;
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
  unit: SearchMemoryUnit;
  score: number;
}

export interface SearchResponse {
  results: SearchResult[];
  query_time_ms: number;
}

export interface RuntimeConfig {
  raft: Record<string, unknown>;
  worker: Record<string, unknown>;
  llm: Record<string, unknown>;
  storage: Record<string, unknown>;
}

export interface Organization {
  org_id: string;
  name: string;
  created_at: string;
}

export interface OrganizationListResponse {
  organizations: Organization[];
  total_count: number;
}

export interface ApiKeySummary {
  key_id: string;
  org_id: string;
  name: string;
  key_prefix: string;
  created_at: string;
  revoked_at?: string | null;
  active: boolean;
}

export interface ApiKeyListResponse {
  api_keys: ApiKeySummary[];
  total_count: number;
}

export interface CreatedApiKey {
  key_id: string;
  org_id: string;
  name: string;
  key_prefix: string;
  key: string;
  created_at: string;
}

export interface OrganizationKnowledgeItem {
  unit: OrganizationKnowledgeUnit;
  knowledge: OrganizationKnowledgeDetail;
}

export interface OrganizationKnowledgeListItem {
  unit: OrganizationKnowledgeUnit;
  contribution_count: number;
  membership_count: number;
  contributor_user_ids: string[];
  top_contributor_user_id?: string | null;
  source_memory_types: string[];
  primary_source_memory_type?: string | null;
  published_at: string;
}

export interface OrganizationKnowledgeListResponse {
  items: OrganizationKnowledgeListItem[];
  total_count: number;
  summary: {
    knowledge_count: number;
    contribution_count: number;
    membership_count: number;
    contributor_count: number;
  };
}

export interface OrganizationKnowledgeMetrics {
  org_id: string;
  knowledge_count: number;
  contribution_count: number;
  membership_count: number;
  candidate_contribution_count: number;
  revoked_contribution_count: number;
  contributor_count: number;
  auto_approved_total: number;
  auto_publish_total: number;
  rebuild_total: number;
  revoke_total: number;
  merged_publication_total: number;
  source_type_distribution: Array<{
    key: string;
    value: number;
  }>;
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
  goal: {
    id: string;
    content: string;
    transaction_time: string;
  };
  tasks: L3TaskTree[];
}


export interface RetrieveRequest {
  query: string;
  limit?: number;
  min_score?: number;
  graph_depth?: number;
  start_time?: string;
  end_time?: string;
  as_of?: string;
  org_id?: string;
  agent_id?: string;
}

export interface RetrieveResponse extends SearchResponse {
  stream_id: string;
  query: string;
}

export interface PendingCountResponse {
  pending: number;
}

export type ReadyTask = L3Task;
