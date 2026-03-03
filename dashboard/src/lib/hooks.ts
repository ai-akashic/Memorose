import useSWR from "swr";
import { api } from "./api";
import type { ClusterStatus, Stats, MemoryListResponse, GraphData, AgentListResponse, PendingCountResponse } from "./types";

export function useClusterStatus() {
  return useSWR<ClusterStatus>("cluster-status", () => api.clusterStatus(), {
    refreshInterval: 5000,
  });
}

export function useStats(user_id?: string) {
  return useSWR<Stats>(`stats-${user_id ?? "_all"}`, () => api.stats(user_id), {
    refreshInterval: 5000,
  });
}

export function useMemories(params: {
  level?: number;
  page?: number;
  limit?: number;
  sort?: string;
  user_id?: string;
  agent_id?: string;
}) {
  const key = `memories-${JSON.stringify(params)}`;
  return useSWR<MemoryListResponse>(key, () => api.memories(params), {
    refreshInterval: 30000,
  });
}

export function useGraph(limit?: number, user_id?: string) {
  const key = `graph-${limit}-${user_id ?? "_all"}`;
  return useSWR<GraphData>(key, () => api.graph(limit, user_id));
}

export function useAgents() {
  return useSWR<AgentListResponse>("agents-list", () => api.agents(), {
    refreshInterval: 30000,
  });
}

export function useAgentStats(agentId: string | undefined) {
  return useSWR(
    agentId ? `agent-stats-${agentId}` : null,
    () => agentId ? api.agentStats(agentId) : null,
    { refreshInterval: 30000 }
  );
}

export function useTaskTree(user_id: string | undefined) {
  const key = user_id ? `tasks-tree-${user_id}` : null;
  return useSWR(key, () => api.getTaskTree(user_id!));
}

export function useAppStats(appId: string | undefined) {
  return useSWR(
    appId ? `app-stats-${appId}` : null,
    () => appId ? api.appStats(appId) : null,
    { refreshInterval: 30000 }
  );
}

export function useReadyTasks(user_id: string | undefined) {
  return useSWR(
    user_id ? `ready-tasks-${user_id}` : null,
    () => user_id ? api.getReadyTasks(user_id) : null,
    { refreshInterval: 5000 }
  );
}

export function usePendingCount() {
  return useSWR<PendingCountResponse>("pending-count", () => api.pendingCount(), {
    refreshInterval: 5000,
  });
}
