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
  }) => {
    const qs = new URLSearchParams();
    if (params.level !== undefined) qs.set("level", String(params.level));
    if (params.page) qs.set("page", String(params.page));
    if (params.limit) qs.set("limit", String(params.limit));
    if (params.sort) qs.set("sort", params.sort);
    if (params.user_id) qs.set("user_id", params.user_id);
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
};
