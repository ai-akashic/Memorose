"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import {
  Activity,
  AlertTriangle,
  Building2,
  CheckCircle2,
  Clock3,
  Copy,
  Database,
  FileText,
  KeyRound,
  Layers,
  Loader2,
  Package,
  Server,
  Share2,
  Shield,
  TrendingUp,
  User,
  Users,
  Zap,
} from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { api } from "@/lib/api";
import { useAppStats, useMemorySharing, useStoredString } from "@/lib/hooks";
import { formatNumber } from "@/lib/utils";
import type { AppApiKey, AppStats, ShareBackfillStatus, SharePolicy } from "@/lib/types";

const DEFAULT_POLICY: SharePolicy = {
  contribute: false,
  consume: false,
  include_history: false,
  targets: [],
};

function StatCard({
  label,
  value,
  sub,
  icon: Icon,
  color = "text-primary",
}: {
  label: string;
  value: string | number;
  sub?: string;
  icon: React.ElementType;
  color?: string;
}) {
  return (
    <Card className="border-border/70">
      <CardContent className="pt-4 pb-3">
        <div className="mb-2 flex items-center justify-between">
          <span className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {label}
          </span>
          <Icon className={`h-3.5 w-3.5 ${color} opacity-60`} />
        </div>
        <div className="text-xl font-bold tracking-tight">
          {typeof value === "number" ? formatNumber(value) : value}
        </div>
        {sub ? <div className="mt-0.5 text-[11px] text-muted-foreground">{sub}</div> : null}
      </CardContent>
    </Card>
  );
}

function PolicyControls({
  title,
  description,
  policy,
  backfill,
  saving,
  onChange,
  onSave,
}: {
  title: string;
  description: string;
  policy: SharePolicy;
  backfill?: ShareBackfillStatus | null;
  saving?: boolean;
  onChange: (next: SharePolicy) => void;
  onSave: () => void;
}) {
  const statusTone =
    backfill?.status === "done"
      ? "text-success border-success/20 bg-success/5"
      : backfill?.status === "failed"
        ? "text-destructive border-destructive/20 bg-destructive/5"
        : "text-warning border-warning/20 bg-warning/5";

  return (
    <Card className="border-border/70">
      <CardHeader className="space-y-2">
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="text-sm">{title}</CardTitle>
          {backfill ? (
            <div className={`rounded-full border px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.18em] ${statusTone}`}>
              {backfill.status}
            </div>
          ) : null}
        </div>
        <p className="text-sm text-muted-foreground">{description}</p>
      </CardHeader>
      <CardContent className="space-y-4">
        {[
          {
            checked: policy.consume,
            label: "Consume shared memory",
            description: "Read this shared layer after local agent/user memory during retrieval.",
            onChange: (checked: boolean) => onChange({ ...policy, consume: checked }),
            disabled: false,
          },
          {
            checked: policy.contribute,
            label: "Contribute local memory",
            description: "Project local memory upward into this shared layer.",
            onChange: (checked: boolean) => onChange({ ...policy, contribute: checked }),
            disabled: false,
          },
          {
            checked: policy.include_history,
            label: "Include historical memory",
            description: "Backfill existing local memory when contribution is enabled.",
            onChange: (checked: boolean) => onChange({ ...policy, include_history: checked }),
            disabled: !policy.contribute,
          },
        ].map((item) => (
          <label key={item.label} className="flex items-start gap-3 rounded-xl border border-border/70 px-3 py-3">
            <input
              type="checkbox"
              checked={item.checked}
              disabled={item.disabled}
              onChange={(event) => item.onChange(event.target.checked)}
              className="mt-1 h-4 w-4 rounded border-border"
            />
            <div>
              <p className="text-sm font-medium">{item.label}</p>
              <p className="text-sm text-muted-foreground">{item.description}</p>
            </div>
          </label>
        ))}

        {backfill ? (
          <div className="rounded-xl border border-border/70 bg-background/50 px-3 py-3 text-sm">
            <div className="flex items-center gap-2 text-muted-foreground">
              {backfill.status === "done" ? (
                <CheckCircle2 className="h-4 w-4 text-success" />
              ) : backfill.status === "failed" ? (
                <AlertTriangle className="h-4 w-4 text-destructive" />
              ) : (
                <Clock3 className="h-4 w-4 text-warning" />
              )}
              <span className="font-medium">Backfill status</span>
            </div>
            <div className="mt-2 space-y-1 font-mono text-[11px] text-muted-foreground">
              {backfill.scheduled_at ? <p>scheduled: {backfill.scheduled_at}</p> : null}
              {backfill.finished_at ? <p>finished: {backfill.finished_at}</p> : null}
              {typeof backfill.projected === "number" ? <p>projected: {backfill.projected}</p> : null}
              {backfill.error ? <p className="text-destructive">error: {backfill.error}</p> : null}
            </div>
          </div>
        ) : null}

        <Button disabled={saving} onClick={onSave} className="w-full">
          {saving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Shield className="mr-2 h-4 w-4" />}
          Save {title}
        </Button>
      </CardContent>
    </Card>
  );
}

function SharingSettings({ appId, orgId }: { appId: string; orgId: string }) {
  const [userIdInput, setUserIdInput] = useStoredString(`memorose-sharing-user:${appId}`);
  const userId = userIdInput.trim();
  const { data, isLoading, error, mutate } = useMemorySharing(userId || undefined, appId, orgId);
  const [appPolicy, setAppPolicy] = useState<SharePolicy>(DEFAULT_POLICY);
  const [orgPolicy, setOrgPolicy] = useState<SharePolicy>(DEFAULT_POLICY);
  const [savingApp, setSavingApp] = useState(false);
  const [savingOrg, setSavingOrg] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [messageTone, setMessageTone] = useState<"success" | "error">("success");

  useEffect(() => {
    if (!data) return;
    setAppPolicy(data.app ?? DEFAULT_POLICY);
    setOrgPolicy(data.organization ?? DEFAULT_POLICY);
  }, [data]);

  async function saveAppPolicy() {
    if (!userId) {
      setMessageTone("error");
      setMessage("user_id is required to configure sharing.");
      return;
    }

    setSavingApp(true);
    setMessage(null);
    try {
      await api.updateMemorySharing(userId, appId, { app: appPolicy, org_id: orgId });
      await mutate();
      setMessageTone("success");
      setMessage("App sharing policy updated.");
    } catch (saveError) {
      setMessageTone("error");
      setMessage(saveError instanceof Error ? saveError.message : "Failed to update app sharing.");
    } finally {
      setSavingApp(false);
    }
  }

  async function saveOrgPolicy() {
    if (!userId) {
      setMessageTone("error");
      setMessage("user_id is required to configure sharing.");
      return;
    }

    setSavingOrg(true);
    setMessage(null);
    try {
      await api.updateMemorySharing(userId, appId, { org_id: orgId, organization: orgPolicy });
      await mutate();
      setMessageTone("success");
      setMessage("Organization sharing policy updated.");
    } catch (saveError) {
      setMessageTone("error");
      setMessage(saveError instanceof Error ? saveError.message : "Failed to update organization sharing.");
    } finally {
      setSavingOrg(false);
    }
  }

  return (
    <div className="space-y-4">
      <Card className="border-border/70">
        <CardHeader>
          <CardTitle className="text-sm">Memory Sharing</CardTitle>
          <p className="text-sm text-muted-foreground">
            Sharing is configured per user. This app is pinned to organization{" "}
            <span className="font-mono">{orgId}</span>, so organization sharing always targets that
            same boundary.
          </p>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <div className="space-y-1.5">
              <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                <User className="h-3.5 w-3.5" />
                User Scope
              </div>
              <Input
                value={userIdInput}
                onChange={(event) => setUserIdInput(event.target.value)}
                placeholder="Enter user_id"
                className="font-mono"
              />
            </div>

            <div className="space-y-1.5">
              <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                <Building2 className="h-3.5 w-3.5" />
                Organization Scope
              </div>
              <div className="rounded-xl border border-border/70 bg-background/50 px-3 py-2.5 font-mono text-sm">
                {orgId}
              </div>
            </div>
          </div>

          {message ? (
            <div
              className={`rounded-xl border px-3 py-2 text-sm ${
                messageTone === "success"
                  ? "border-success/20 bg-success/5 text-success"
                  : "border-destructive/20 bg-destructive/5 text-destructive"
              }`}
            >
              {message}
            </div>
          ) : null}

          {!userId ? (
            <div className="rounded-xl border border-dashed border-border px-4 py-6 text-center text-sm text-muted-foreground">
              Enter a user_id to load and edit sharing policy.
            </div>
          ) : isLoading ? (
            <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
              <Skeleton className="h-80" />
              <Skeleton className="h-80" />
            </div>
          ) : error ? (
            <div className="rounded-xl border border-destructive/20 bg-destructive/5 px-4 py-6 text-sm text-destructive">
              {error.message}
            </div>
          ) : (
            <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
              <PolicyControls
                title="App Sharing"
                description="Controls whether this user contributes to or consumes shared memory inside this app."
                policy={appPolicy}
                backfill={data?.app_backfill}
                saving={savingApp}
                onChange={setAppPolicy}
                onSave={saveAppPolicy}
              />
              <PolicyControls
                title="Organization Sharing"
                description="Controls whether this user contributes to or consumes shared memory across apps in the same organization."
                policy={orgPolicy}
                backfill={data?.organization_backfill}
                saving={savingOrg}
                onChange={setOrgPolicy}
                onSave={saveOrgPolicy}
              />
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function ApiKeysTab({ appId, orgId }: { appId: string; orgId: string }) {
  const [apiKeys, setApiKeys] = useState<AppApiKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [copied, setCopied] = useState(false);
  const [keyName, setKeyName] = useState("");
  const [revealedKey, setRevealedKey] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function loadKeys(targetAppId = appId) {
    try {
      const response = await api.listApiKeys(targetAppId);
      setApiKeys(response.api_keys);
      setError(null);
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Failed to load API keys");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setLoading(true);
    void (async () => {
      try {
        const response = await api.listApiKeys(appId);
        setApiKeys(response.api_keys);
        setError(null);
      } catch (loadError) {
        setError(loadError instanceof Error ? loadError.message : "Failed to load API keys");
      } finally {
        setLoading(false);
      }
    })();
  }, [appId]);

  async function handleCreateKey() {
    setCreating(true);
    setError(null);
    setRevealedKey(null);

    try {
      const response = await api.createApiKey(appId, { name: keyName.trim() || undefined });
      setKeyName("");
      setRevealedKey(response.raw_key);
      await loadKeys();
    } catch (createError) {
      setError(createError instanceof Error ? createError.message : "Failed to create API key");
    } finally {
      setCreating(false);
    }
  }

  async function handleRevoke(keyId: string) {
    try {
      await api.revokeApiKey(appId, keyId);
      await loadKeys();
    } catch (revokeError) {
      setError(revokeError instanceof Error ? revokeError.message : "Failed to revoke API key");
    }
  }

  async function handleCopy() {
    if (!revealedKey) return;
    await navigator.clipboard.writeText(revealedKey);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1500);
  }

  return (
    <div className="space-y-4">
      <Card className="border-border/70">
        <CardHeader className="space-y-2">
          <CardTitle className="text-sm">App API Keys</CardTitle>
          <p className="text-sm text-muted-foreground">
            Keys are scoped to <span className="font-mono">{appId}</span> in organization{" "}
            <span className="font-mono">{orgId}</span>. Create the app first, then mint keys for
            client access.
          </p>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_auto]">
            <Input
              value={keyName}
              onChange={(event) => setKeyName(event.target.value)}
              placeholder="Primary production key"
            />
            <Button onClick={handleCreateKey} disabled={creating}>
              {creating ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <KeyRound className="mr-2 h-4 w-4" />}
              Create API Key
            </Button>
          </div>

          {revealedKey ? (
            <div className="rounded-2xl border border-primary/20 bg-primary/5 p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold">Copy this key now</p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    It is only shown once after creation.
                  </p>
                </div>
                <Button variant="outline" onClick={handleCopy}>
                  <Copy className="mr-2 h-4 w-4" />
                  {copied ? "Copied" : "Copy"}
                </Button>
              </div>
              <div className="mt-3 overflow-x-auto rounded-xl border border-border/70 bg-background/80 px-3 py-3 font-mono text-sm">
                {revealedKey}
              </div>
            </div>
          ) : null}

          {error ? (
            <div className="rounded-xl border border-destructive/20 bg-destructive/5 px-3 py-2 text-sm text-destructive">
              {error}
            </div>
          ) : null}

          {loading ? (
            <Skeleton className="h-48 rounded-2xl" />
          ) : apiKeys.length === 0 ? (
            <div className="rounded-xl border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
              No API keys yet.
            </div>
          ) : (
            <div className="space-y-3">
              {apiKeys.map((apiKey) => (
                <div
                  key={apiKey.key_id}
                  className="flex flex-col gap-3 rounded-2xl border border-border/70 bg-background/40 p-4 md:flex-row md:items-center md:justify-between"
                >
                  <div className="min-w-0">
                    <p className="text-sm font-semibold">{apiKey.name}</p>
                    <div className="mt-1 flex flex-wrap gap-3 text-[11px] text-muted-foreground">
                      <span className="font-mono">{apiKey.key_prefix}...</span>
                      <span>created {new Date(apiKey.created_at).toLocaleString()}</span>
                      {apiKey.revoked_at ? (
                        <span className="text-destructive">
                          revoked {new Date(apiKey.revoked_at).toLocaleString()}
                        </span>
                      ) : (
                        <span className="text-success">active</span>
                      )}
                    </div>
                  </div>
                  <Button
                    variant="outline"
                    onClick={() => handleRevoke(apiKey.key_id)}
                    disabled={Boolean(apiKey.revoked_at)}
                  >
                    Revoke
                  </Button>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function OverviewTab({ stats }: { stats: AppStats }) {
  const pipelineColor =
    stats.overview.memory_pipeline_status === "healthy"
      ? "text-success"
      : stats.overview.memory_pipeline_status === "generating_l2"
        ? "text-warning"
        : "text-muted-foreground";

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        <StatCard label="Total Events" value={stats.overview.total_events} icon={Activity} />
        <StatCard label="Total Users" value={stats.overview.total_users} icon={Users} />
        <StatCard
          label="Local Memories"
          value={stats.overview.local_memories}
          sub={`${stats.overview.agent_memories} agent / ${stats.overview.user_memories} user`}
          icon={Database}
        />
        <StatCard
          label="Shared Memories"
          value={stats.overview.shared_memories}
          sub={`${stats.overview.shared_app_memories} app / ${stats.overview.shared_org_memories} org`}
          icon={Share2}
          color="text-warning"
        />
        <StatCard
          label="Total Memories"
          value={stats.overview.total_memories}
          sub={`${stats.overview.l1_count} L1 / ${stats.overview.l2_count} L2`}
          icon={Layers}
        />
        <StatCard
          label="Avg Local / User"
          value={stats.overview.avg_local_memories_per_user.toFixed(1)}
          icon={TrendingUp}
        />
      </div>

      <Card className="border-border/70">
        <CardHeader>
          <CardTitle className="text-sm">Memory Pipeline Status</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <div className={`h-2 w-2 rounded-full ${pipelineColor === "text-success" ? "bg-green-500" : pipelineColor === "text-warning" ? "bg-yellow-500" : "bg-gray-500"}`} />
            <span className={`font-medium ${pipelineColor}`}>
              {stats.overview.memory_pipeline_status.replace(/_/g, " ").toUpperCase()}
            </span>
          </div>
          <p className="mt-2 text-sm text-muted-foreground">
            {stats.overview.memory_pipeline_status === "healthy"
              ? "All memory generation stages are working correctly."
              : stats.overview.memory_pipeline_status === "generating_l2"
                ? "L1 memories exist and L2 generation is in progress."
                : "Waiting for new events to generate memory."}
          </p>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Scope Breakdown</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <Row label="Local" value={stats.overview.memory_by_scope.local} />
            <Row label="Shared" value={stats.overview.memory_by_scope.shared} />
          </CardContent>
        </Card>

        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Domain Breakdown</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <Row label="Agent" value={stats.overview.memory_by_domain.agent} />
            <Row label="User" value={stats.overview.memory_by_domain.user} />
            <Row label="App" value={stats.overview.memory_by_domain.app} />
            <Row label="Organization" value={stats.overview.memory_by_domain.organization} />
          </CardContent>
        </Card>

        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Level Breakdown</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <Row label="Local L1" value={stats.overview.memory_by_level_and_scope.local.l1} />
            <Row label="Local L2" value={stats.overview.memory_by_level_and_scope.local.l2} />
            <Row label="Shared L1" value={stats.overview.memory_by_level_and_scope.shared.l1} />
            <Row label="Shared L2" value={stats.overview.memory_by_level_and_scope.shared.l2} />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function UsersTab({ stats }: { stats: AppStats }) {
  if (stats.users.length === 0) {
    return (
      <Card className="border-border/70">
        <CardContent className="pt-6">
          <div className="py-8 text-center">
            <Users className="mx-auto mb-4 h-12 w-12 text-muted-foreground" />
            <p className="text-muted-foreground">No user activity yet</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="border-border/70">
      <CardHeader>
        <CardTitle className="text-sm">User Activity</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-2">
          <div className="grid grid-cols-4 gap-4 border-b pb-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            <div>User ID</div>
            <div className="text-right">Events</div>
            <div className="text-right">Memories</div>
            <div className="text-right">Last Activity</div>
          </div>
          {stats.users.slice(0, 20).map((user) => (
            <div key={user.user_id} className="grid grid-cols-4 gap-4 border-b py-2 text-sm last:border-0">
              <div className="truncate font-mono">{user.user_id}</div>
              <div className="text-right font-mono">{formatNumber(user.event_count)}</div>
              <div className="text-right font-mono">{formatNumber(user.memory_count)}</div>
              <div className="text-right text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                {user.last_activity ? new Date(user.last_activity * 1000).toLocaleString() : "N/A"}
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function ActivityTab({ stats }: { stats: AppStats }) {
  if (stats.recent_activity.length === 0) {
    return (
      <Card className="border-border/70">
        <CardContent className="pt-6">
          <div className="py-8 text-center">
            <Activity className="mx-auto mb-4 h-12 w-12 text-muted-foreground" />
            <p className="text-muted-foreground">No recent activity</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  const eventTypeCounts = stats.recent_activity.reduce((acc, activity) => {
    acc[activity.event_type] = (acc[activity.event_type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  return (
    <div className="space-y-4">
      <Card className="border-border/70">
        <CardHeader>
          <CardTitle className="text-sm">Event Type Distribution</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
            {Object.entries(eventTypeCounts).map(([type, count]) => (
              <div key={type} className="flex flex-col">
                <span className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                  {type}
                </span>
                <span className="text-lg font-semibold">{formatNumber(count)}</span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card className="border-border/70">
        <CardHeader>
          <CardTitle className="text-sm">Recent Activity Log</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            <div className="grid grid-cols-4 gap-4 border-b pb-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
              <div>Timestamp</div>
              <div>User</div>
              <div>Type</div>
              <div>Stream</div>
            </div>
            {stats.recent_activity.slice(0, 50).map((activity, index) => (
              <div key={`${activity.stream_id}-${index}`} className="grid grid-cols-4 gap-4 border-b py-2 text-sm last:border-0">
                <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                  {new Date(activity.timestamp * 1000).toLocaleString()}
                </div>
                <div className="truncate font-mono text-xs">{activity.user_id}</div>
                <div>
                  <span className="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium text-primary bg-primary/10">
                    {activity.event_type}
                  </span>
                </div>
                <div className="truncate font-mono text-xs">{activity.stream_id}</div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function PerformanceTab({ stats }: { stats: AppStats }) {
  const formatBytes = (bytes: number) => {
    if (bytes === 0) return "0 B";
    const sizes = ["B", "KB", "MB", "GB"];
    const index = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, index)).toFixed(2)} ${sizes[index]}`;
  };

  const formatPercentage = (value: number) => `${(value * 100).toFixed(1)}%`;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard label="Total Storage" value={formatBytes(stats.performance.total_storage_bytes)} icon={Server} />
        <StatCard label="Event Storage" value={formatBytes(stats.performance.event_storage_bytes)} icon={Activity} />
        <StatCard label="Memory Storage" value={formatBytes(stats.performance.memory_storage_bytes)} icon={Database} />
        <StatCard label="Avg Event Size" value={formatBytes(stats.performance.avg_event_size_bytes)} icon={FileText} />
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <StatCard label="L1 Generation Rate" value={formatPercentage(stats.performance.l1_generation_rate)} sub="Events -> local L1 memories" icon={Zap} />
        <StatCard label="L2 Generation Rate" value={formatPercentage(stats.performance.l2_generation_rate)} sub="Local L1 -> local L2 memories" icon={TrendingUp} />
      </div>
    </div>
  );
}

function Row({ label, value }: { label: string; value: number }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-mono">{formatNumber(value)}</span>
    </div>
  );
}

export default function AppDetailClient() {
  const params = useParams();
  const appId = params.app_id as string;
  const { data: stats, isLoading, error } = useAppStats(appId);

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-96" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-4">
        <h1 className="text-2xl font-bold">Application Details</h1>
        <Card className="border-border/70">
          <CardContent className="pt-6">
            <div className="text-destructive">Error: {error.message}</div>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (!stats) {
    return null;
  }

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Link href="/apps/" className="hover:text-foreground">
              Apps
            </Link>
            <span>/</span>
            <span className="font-mono">{stats.app_id}</span>
          </div>
          <div className="mt-2 flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2">
              <Package className="h-5 w-5 text-primary" />
              <h1 className="text-2xl font-bold">{stats.name}</h1>
            </div>
            <div className="inline-flex items-center gap-2 rounded-full border border-border/70 bg-background/60 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
              <Building2 className="h-3.5 w-3.5" />
              {stats.org_id}
            </div>
          </div>
        </div>
      </div>

      <Tabs defaultValue="overview" className="w-full">
        <TabsList className="grid w-full max-w-4xl grid-cols-6">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="sharing">Sharing</TabsTrigger>
          <TabsTrigger value="keys">API Keys</TabsTrigger>
          <TabsTrigger value="users">Users</TabsTrigger>
          <TabsTrigger value="activity">Activity</TabsTrigger>
          <TabsTrigger value="performance">Performance</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-4">
          <OverviewTab stats={stats} />
        </TabsContent>

        <TabsContent value="sharing" className="mt-4">
          <SharingSettings appId={appId} orgId={stats.org_id} />
        </TabsContent>

        <TabsContent value="keys" className="mt-4">
          <ApiKeysTab appId={appId} orgId={stats.org_id} />
        </TabsContent>

        <TabsContent value="users" className="mt-4">
          <UsersTab stats={stats} />
        </TabsContent>

        <TabsContent value="activity" className="mt-4">
          <ActivityTab stats={stats} />
        </TabsContent>

        <TabsContent value="performance" className="mt-4">
          <PerformanceTab stats={stats} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
