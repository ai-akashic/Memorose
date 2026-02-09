import useSWR from "swr";
import { api } from "./api";
import type { ClusterStatus, Stats, MemoryListResponse, GraphData } from "./types";

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
