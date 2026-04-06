import { useEffect, useState } from "react";
import useSWR from "swr";
import { api } from "./api";
import type {
  ApiKeyListResponse,
  AgentListResponse,
  OrganizationListResponse,
  OrganizationKnowledgeItem,
  OrganizationKnowledgeListResponse,
  OrganizationKnowledgeMetrics,
  ClusterStatus,
  GraphData,
  MemoryListResponse,
  PendingCountResponse,
  Stats,
} from "./types";

export function useClusterStatus() {
  return useSWR<ClusterStatus>("cluster-status", () => api.clusterStatus(), {
    refreshInterval: 5000,
  });
}

export function useStats(user_id?: string, org_id?: string, history_hours?: number) {
  return useSWR<Stats>(
    `stats-${user_id ?? "_all"}-${org_id ?? "_all"}-${history_hours ?? 24}`,
    () => api.stats(user_id, org_id, history_hours),
    {
    refreshInterval: 5000,
    }
  );
}

export function useMemories(params: {
  level?: number;
  page?: number;
  limit?: number;
  sort?: string;
  org_id?: string;
  user_id?: string;
  agent_id?: string;
}) {
  const key = `memories-${JSON.stringify(params)}`;
  return useSWR<MemoryListResponse>(key, () => api.memories(params), {
    refreshInterval: 30000,
  });
}

export function useGraph(limit?: number, user_id?: string, org_id?: string) {
  const key = `graph-${limit}-${user_id ?? "_all"}-${org_id ?? "_all"}`;
  return useSWR<GraphData>(key, () => api.graph(limit, user_id, org_id));
}

export function useAgents() {
  return useSWR<AgentListResponse>("agents-list", () => api.agents(), {
    refreshInterval: 30000,
  });
}

export function useOrganizations() {
  return useSWR<OrganizationListResponse>("organizations-list", () => api.listOrganizations(), {
    refreshInterval: 30000,
  });
}

export function useApiKeys() {
  return useSWR<ApiKeyListResponse>("api-keys-list", () => api.listApiKeys(), {
    refreshInterval: 30000,
  });
}

export function useOrganizationKnowledge(
  orgId: string | undefined,
  params?: {
    q?: string;
    contributor?: string;
    source_type?: string;
    sort?: string;
  }
) {
  return useSWR<OrganizationKnowledgeListResponse>(
    orgId ? `organization-knowledge-${orgId}-${JSON.stringify(params ?? {})}` : null,
    () => api.listOrganizationKnowledge(orgId!, params),
    {
      refreshInterval: 30000,
    }
  );
}

export function useOrganizationKnowledgeDetail(
  orgId: string | undefined,
  knowledgeId: string | undefined
) {
  return useSWR<OrganizationKnowledgeItem>(
    orgId && knowledgeId
      ? `organization-knowledge-detail-${orgId}-${knowledgeId}`
      : null,
    () => api.getOrganizationKnowledge(orgId!, knowledgeId!),
    {
      refreshInterval: 30000,
    }
  );
}

export function useOrganizationKnowledgeMetrics(orgId: string | undefined) {
  return useSWR<OrganizationKnowledgeMetrics>(
    orgId ? `organization-knowledge-metrics-${orgId}` : null,
    () => api.getOrganizationKnowledgeMetrics(orgId!),
    {
      refreshInterval: 30000,
    }
  );
}

export function useTaskTree(user_id: string | undefined) {
  const key = user_id ? `tasks-tree-${user_id}` : null;
  return useSWR(key, () => api.getTaskTree(user_id!));
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

export function useStoredString(key: string, fallback = "") {
  const [value, setValue] = useState(() => {
    if (typeof window === "undefined") {
      return fallback;
    }
    return window.localStorage.getItem(key) ?? fallback;
  });

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const trimmed = value.trim();
    if (trimmed) {
      window.localStorage.setItem(key, trimmed);
    } else {
      window.localStorage.removeItem(key);
    }
  }, [key, value]);

  return [value, setValue] as const;
}
