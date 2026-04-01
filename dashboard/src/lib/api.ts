import { getToken, clearToken } from "./auth";

function normalizeApiBase(rawBase?: string): string {
  const fallbackOrigin = typeof window !== "undefined" ? window.location.origin : "";
  const candidate = (rawBase || fallbackOrigin).trim().replace(/\/+$/, "");

  if (!candidate) {
    return "";
  }

  const stripKnownSuffixes = (value: string) =>
    value
      .replace(/\/v1\/dashboard$/i, "")
      .replace(/\/dashboard$/i, "")
      .replace(/\/v1$/i, "");

  try {
    const parsed = new URL(candidate);
    const normalizedPath = stripKnownSuffixes(parsed.pathname);
    return `${parsed.origin}${normalizedPath === "/" ? "" : normalizedPath}`;
  } catch {
    return stripKnownSuffixes(candidate);
  }
}

const API_BASE = normalizeApiBase(process.env.NEXT_PUBLIC_API_URL);

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
  stats: (user_id?: string, org_id?: string) => {
    const qs = new URLSearchParams();
    if (user_id) qs.set("user_id", user_id);
    if (org_id) qs.set("org_id", org_id);
    const qstr = qs.toString();
    return fetchAPI<import("./types").Stats>(`/stats${qstr ? `?${qstr}` : ""}`);
  },
  runtimeConfig: () => fetchAPI<import("./types").RuntimeConfig>("/config"),

  memories: (params: {
    level?: number;
    page?: number;
    limit?: number;
    sort?: string;
    org_id?: string;
    user_id?: string;
    agent_id?: string;
  }) => {
    const qs = new URLSearchParams();
    if (params.level !== undefined) qs.set("level", String(params.level));
    if (params.page) qs.set("page", String(params.page));
    if (params.limit) qs.set("limit", String(params.limit));
    if (params.sort) qs.set("sort", params.sort);
    if (params.org_id) qs.set("org_id", params.org_id);
    if (params.user_id) qs.set("user_id", params.user_id);
    if (params.agent_id) qs.set("agent_id", params.agent_id);
    return fetchAPI<import("./types").MemoryListResponse>(`/memories?${qs}`);
  },

  memory: (id: string) => {
    return fetchAPI<import("./types").DashboardMemoryDetail>(`/memories/${id}`);
  },

  graph: (limit?: number, user_id?: string, org_id?: string) => {
    const qs = new URLSearchParams();
    if (limit) qs.set("limit", String(limit));
    if (user_id) qs.set("user_id", user_id);
    if (org_id) qs.set("org_id", org_id);
    return fetchAPI<import("./types").GraphData>(`/graph?${qs}`);
  },

  search: (params: {
    query: string;
    mode?: string;
    limit?: number;
    enable_arbitration?: boolean;
    user_id: string;
    org_id?: string;
    agent_id?: string;
  }) =>
    fetchAPI<import("./types").SearchResponse>("/search", {
      method: "POST",
      body: JSON.stringify(params),
    }),

  ingestEvent: (params: {
    user_id: string;
    stream_id: string;
    content: {
      type: string;
      data: string;
    };
  }) => {
    const { user_id, stream_id, content } = params;
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    const token = getToken();
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
    return fetch(`${API_BASE}/v1/users/${user_id}/streams/${stream_id}/events`, {
      method: "POST",
      headers,
      body: JSON.stringify({ content: content.data, content_type: content.type }),
    });
  },

  agents: () =>
    fetchAPI<import("./types").AgentListResponse>("/agents"),

  listOrganizations: () =>
    fetchAPI<import("./types").OrganizationListResponse>("/organizations"),

  createOrganization: (body: { org_id: string; name?: string }) =>
    fetchAPI<import("./types").Organization>("/organizations", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  listApiKeys: () =>
    fetchAPI<import("./types").ApiKeyListResponse>("/api-keys"),

  createApiKey: (body: { org_id: string; name?: string }) =>
    fetchAPI<import("./types").CreatedApiKey>("/api-keys", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  revokeApiKey: (keyId: string) =>
    fetchAPI<import("./types").ApiKeySummary>(`/api-keys/${encodeURIComponent(keyId)}`, {
      method: "DELETE",
    }),

  listOrganizationKnowledge: (orgId: string, params?: {
    q?: string;
    contributor?: string;
    source_type?: string;
    sort?: string;
  }) => {
    const qs = new URLSearchParams();
    if (params?.q) qs.set("q", params.q);
    if (params?.contributor) qs.set("contributor", params.contributor);
    if (params?.source_type) qs.set("source_type", params.source_type);
    if (params?.sort) qs.set("sort", params.sort);
    const suffix = qs.toString();
    return fetchAPI<import("./types").OrganizationKnowledgeListResponse>(
      `/organizations/${encodeURIComponent(orgId)}/knowledge${suffix ? `?${suffix}` : ""}`
    );
  },

  getOrganizationKnowledge: (orgId: string, id: string) =>
    fetchAPI<import("./types").OrganizationKnowledgeItem>(
      `/organizations/${encodeURIComponent(orgId)}/knowledge/${encodeURIComponent(id)}`
    ),

  getOrganizationKnowledgeMetrics: (orgId: string) =>
    fetchAPI<import("./types").OrganizationKnowledgeMetrics>(
      `/organizations/${encodeURIComponent(orgId)}/knowledge/metrics`
    ),

  // Fixed: was using fetchAPI which adds /v1/dashboard prefix incorrectly
  getTaskTree: (user_id: string) =>
    fetchRaw<import("./types").GoalTree[]>(`/v1/users/${user_id}/tasks/tree`),

  // Task endpoints
  getReadyTasks: (user_id: string) =>
    fetchRaw<import("./types").ReadyTask[]>(`/v1/users/${user_id}/tasks/ready`),

  updateTaskStatus: (user_id: string, task_id: string, body: { status: string; result_summary?: string }) =>
    fetchRaw<{ status: string }>(`/v1/users/${user_id}/tasks/${task_id}/status`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),

  // Retrieve endpoint
  retrieve: (user_id: string, stream_id: string, body: import("./types").RetrieveRequest) =>
    fetchRaw<import("./types").RetrieveResponse>(
      `/v1/users/${user_id}/streams/${stream_id}/retrieve`,
      { method: "POST", body: JSON.stringify(body) }
    ),

  // Status
  pendingCount: () =>
    fetchRaw<import("./types").PendingCountResponse>("/v1/status/pending"),

  leaveCluster: (node_id: number) =>
    fetchRaw<{ status: string }>(`/v1/cluster/nodes/${node_id}`, { method: "DELETE" }),
};
