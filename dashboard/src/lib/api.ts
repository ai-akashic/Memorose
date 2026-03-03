import { getToken, clearToken } from "./auth";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || (typeof window !== "undefined"
  ? `${window.location.protocol}//${window.location.hostname}:${window.location.port}`
  : "");

async function fetchAPI<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string> || {}),
  };

  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(`${API_BASE}/v1/dashboard${path}`, {
    ...options,
    headers,
  });

  if (res.status === 401) {
    clearToken();
    if (typeof window !== "undefined") {
      window.location.href = "/dashboard/login/";
    }
    throw new Error("Unauthorized");
  }

  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }

  return res.json();
}

// For endpoints outside /v1/dashboard (users, cluster, status, etc.)
async function fetchRaw<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string> || {}),
  };

  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers,
  });

  if (res.status === 401) {
    clearToken();
    if (typeof window !== "undefined") {
      window.location.href = "/dashboard/login/";
    }
    throw new Error("Unauthorized");
  }

  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }

  return res.json();
}

export const api = {
  login: (username: string, password: string) =>
    fetchAPI<{ token: string; expires_in: number; must_change_password: boolean }>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),

  changePassword: (current_password: string, new_password: string) =>
    fetchAPI<{ status: string }>("/auth/password", {
      method: "POST",
      body: JSON.stringify({ current_password, new_password }),
    }),

  clusterStatus: () => fetchAPI<import("./types").ClusterStatus>("/cluster/status"),
  stats: (user_id?: string) => {
    const qs = new URLSearchParams();
    if (user_id) qs.set("user_id", user_id);
    const qstr = qs.toString();
    return fetchAPI<import("./types").Stats>(`/stats${qstr ? `?${qstr}` : ""}`);
  },
  config: () => fetchAPI<import("./types").AppConfig>("/config"),
  version: () => fetchAPI<import("./types").VersionInfo>("/version"),

  memories: (params: {
    level?: number;
    page?: number;
    limit?: number;
    sort?: string;
    user_id?: string;
    agent_id?: string;
  }) => {
    const qs = new URLSearchParams();
    if (params.level !== undefined) qs.set("level", String(params.level));
    if (params.page) qs.set("page", String(params.page));
    if (params.limit) qs.set("limit", String(params.limit));
    if (params.sort) qs.set("sort", params.sort);
    if (params.user_id) qs.set("user_id", params.user_id);
    if (params.agent_id) qs.set("agent_id", params.agent_id);
    return fetchAPI<import("./types").MemoryListResponse>(`/memories?${qs}`);
  },

  memory: (id: string) => {
    return fetchAPI<import("./types").MemoryUnit>(`/memories/${id}`);
  },

  graph: (limit?: number, user_id?: string) => {
    const qs = new URLSearchParams();
    if (limit) qs.set("limit", String(limit));
    if (user_id) qs.set("user_id", user_id);
    return fetchAPI<import("./types").GraphData>(`/graph?${qs}`);
  },

  search: (params: {
    query: string;
    mode?: string;
    limit?: number;
    enable_arbitration?: boolean;
    user_id?: string;
    app_id?: string;
    agent_id?: string;
  }) =>
    fetchAPI<import("./types").SearchResponse>("/search", {
      method: "POST",
      body: JSON.stringify(params),
    }),

  ingestEvent: (params: {
    user_id: string;
    app_id: string;
    stream_id: string;
    content: {
      type: string;
      data: string;
    };
  }) => {
    const { user_id, app_id, stream_id, content } = params;
    return fetch(`${API_BASE}/v1/users/${user_id}/apps/${app_id}/streams/${stream_id}/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ content }),
    });
  },

  agents: () =>
    fetchAPI<import("./types").AgentListResponse>("/agents"),

  agentStats: (agentId: string) =>
    fetchAPI<import("./types").AgentSummary & Record<string, unknown>>(`/agents/${encodeURIComponent(agentId)}/stats`),

  // Fixed: was using fetchAPI which adds /v1/dashboard prefix incorrectly
  getTaskTree: (user_id: string) =>
    fetchRaw<import("./types").GoalTree[]>(`/v1/users/${user_id}/tasks/tree`),

  // App stats
  appStats: (app_id: string) =>
    fetchAPI<import("./types").AppStats>(`/apps/${encodeURIComponent(app_id)}/stats`),

  // Task endpoints
  getReadyTasks: (user_id: string) =>
    fetchRaw<import("./types").ReadyTask[]>(`/v1/users/${user_id}/tasks/ready`),

  updateTaskStatus: (user_id: string, task_id: string, body: { status: string; result_summary?: string }) =>
    fetchRaw<{ status: string }>(`/v1/users/${user_id}/tasks/${task_id}/status`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),

  // Retrieve endpoint
  retrieve: (user_id: string, app_id: string, stream_id: string, body: import("./types").RetrieveRequest) =>
    fetchRaw<import("./types").RetrieveResponse>(
      `/v1/users/${user_id}/apps/${app_id}/streams/${stream_id}/retrieve`,
      { method: "POST", body: JSON.stringify(body) }
    ),

  // Graph edge
  addEdge: (user_id: string, body: import("./types").AddEdgeRequest) =>
    fetchRaw<{ status: string }>(`/v1/users/${user_id}/graph/edges`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  // Status
  pendingCount: () =>
    fetchRaw<import("./types").PendingCountResponse>("/v1/status/pending"),

  // Cluster management
  initializeCluster: () =>
    fetchRaw<import("./types").ClusterInitResponse>("/v1/cluster/initialize", { method: "POST" }),

  joinCluster: (body: import("./types").ClusterJoinRequest) =>
    fetchRaw<import("./types").ClusterJoinResponse>("/v1/cluster/join", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  leaveCluster: (node_id: number) =>
    fetchRaw<{ status: string }>(`/v1/cluster/nodes/${node_id}`, { method: "DELETE" }),
};
