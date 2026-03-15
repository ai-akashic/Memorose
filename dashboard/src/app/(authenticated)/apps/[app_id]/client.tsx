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
import { useTranslations } from "next-intl";

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
  const t = useTranslations("AppDetail");
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
            label: t("sharing.consumeLabel"),
            description: t("sharing.consumeDescription"),
            onChange: (checked: boolean) => onChange({ ...policy, consume: checked }),
            disabled: false,
          },
          {
            checked: policy.contribute,
            label: t("sharing.contributeLabel"),
            description: t("sharing.contributeDescription"),
            onChange: (checked: boolean) => onChange({ ...policy, contribute: checked }),
            disabled: false,
          },
          {
            checked: policy.include_history,
            label: t("sharing.historyLabel"),
            description: t("sharing.historyDescription"),
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
              <span className="font-medium">{t("sharing.backfillStatus")}</span>
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
          {t("sharing.saveButton", { title })}
        </Button>
      </CardContent>
    </Card>
  );
}

function SharingSettings({ appId, orgId }: { appId: string; orgId: string }) {
  const t = useTranslations("AppDetail");
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
      setMessage(t("sharing.userRequired"));
      return;
    }

    setSavingApp(true);
    setMessage(null);
    try {
      await api.updateMemorySharing(userId, appId, { app: appPolicy, org_id: orgId });
      await mutate();
      setMessageTone("success");
      setMessage(t("sharing.appPolicyUpdated"));
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
      setMessage(t("sharing.userRequired"));
      return;
    }

    setSavingOrg(true);
    setMessage(null);
    try {
      await api.updateMemorySharing(userId, appId, { org_id: orgId, organization: orgPolicy });
      await mutate();
      setMessageTone("success");
      setMessage(t("sharing.orgPolicyUpdated"));
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
          <CardTitle className="text-sm">{t("sharing.title")}</CardTitle>
          <p className="text-sm text-muted-foreground">
            {t("sharing.description", { orgId })}
          </p>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <div className="space-y-1.5">
              <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                <User className="h-3.5 w-3.5" />
                {t("sharing.userScope")}
              </div>
              <Input
                value={userIdInput}
                onChange={(event) => setUserIdInput(event.target.value)}
                placeholder={t("sharing.enterUserId")}
                className="font-mono"
              />
            </div>

            <div className="space-y-1.5">
              <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                <Building2 className="h-3.5 w-3.5" />
                {t("sharing.orgScope")}
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
              {t("sharing.enterUserIdPrompt")}
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
                title={t("sharing.appSharingTitle")}
                description={t("sharing.appSharingDescription")}
                policy={appPolicy}
                backfill={data?.app_backfill}
                saving={savingApp}
                onChange={setAppPolicy}
                onSave={saveAppPolicy}
              />
              <PolicyControls
                title={t("sharing.orgSharingTitle")}
                description={t("sharing.orgSharingDescription")}
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
  const t = useTranslations("AppDetail");
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
          <CardTitle className="text-sm">{t("apiKeys.title")}</CardTitle>
          <p className="text-sm text-muted-foreground">
            {t("apiKeys.description", { appId, orgId })}
          </p>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_auto]">
            <Input
              value={keyName}
              onChange={(event) => setKeyName(event.target.value)}
              placeholder={t("apiKeys.keyNamePlaceholder")}
            />
            <Button onClick={handleCreateKey} disabled={creating}>
              {creating ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <KeyRound className="mr-2 h-4 w-4" />}
              {t("apiKeys.createButton")}
            </Button>
          </div>

          {revealedKey ? (
            <div className="rounded-2xl border border-primary/20 bg-primary/5 p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold">{t("apiKeys.copyNowTitle")}</p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {t("apiKeys.copyNowDescription")}
                  </p>
                </div>
                <Button variant="outline" onClick={handleCopy}>
                  <Copy className="mr-2 h-4 w-4" />
                  {copied ? t("apiKeys.copied") : t("apiKeys.copy")}
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
              {t("apiKeys.empty")}
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
                      <span>{t("apiKeys.created")} {new Date(apiKey.created_at).toLocaleString()}</span>
                      {apiKey.revoked_at ? (
                        <span className="text-destructive">
                          {t("apiKeys.revoked")} {new Date(apiKey.revoked_at).toLocaleString()}
                        </span>
                      ) : (
                        <span className="text-success">{t("apiKeys.active")}</span>
                      )}
                    </div>
                  </div>
                  <Button
                    variant="outline"
                    onClick={() => handleRevoke(apiKey.key_id)}
                    disabled={Boolean(apiKey.revoked_at)}
                  >
                    {t("apiKeys.revoke")}
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
  const t = useTranslations("AppDetail");
  const pipelineColor =
    stats.overview.memory_pipeline_status === "healthy"
      ? "text-success"
      : stats.overview.memory_pipeline_status === "generating_l2"
        ? "text-warning"
        : "text-muted-foreground";

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        <StatCard label={t("overview.totalEvents")} value={stats.overview.total_events} icon={Activity} />
        <StatCard label={t("overview.totalUsers")} value={stats.overview.total_users} icon={Users} />
        <StatCard
          label={t("overview.localMemories")}
          value={stats.overview.local_memories}
          sub={`${stats.overview.agent_memories} agent / ${stats.overview.user_memories} user`}
          icon={Database}
        />
        <StatCard
          label={t("overview.sharedMemories")}
          value={stats.overview.shared_memories}
          sub={`${stats.overview.shared_app_memories} app / ${stats.overview.shared_org_memories} org`}
          icon={Share2}
          color="text-warning"
        />
        <StatCard
          label={t("overview.totalMemories")}
          value={stats.overview.total_memories}
          sub={`${stats.overview.l1_count} L1 / ${stats.overview.l2_count} L2`}
          icon={Layers}
        />
        <StatCard
          label={t("overview.avgPerUser")}
          value={stats.overview.avg_local_memories_per_user.toFixed(1)}
          icon={TrendingUp}
        />
      </div>

      <Card className="border-border/70">
        <CardHeader>
          <CardTitle className="text-sm">{t("overview.pipelineTitle")}</CardTitle>
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
              ? t("overview.pipelineHealthy")
              : stats.overview.memory_pipeline_status === "generating_l2"
                ? t("overview.pipelineGenerating")
                : t("overview.pipelineWaiting")}
          </p>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">{t("overview.scopeBreakdown")}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <Row label={t("overview.local")} value={stats.overview.memory_by_scope.local} />
            <Row label={t("overview.shared")} value={stats.overview.memory_by_scope.shared} />
          </CardContent>
        </Card>

        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">{t("overview.domainBreakdown")}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <Row label={t("overview.agent")} value={stats.overview.memory_by_domain.agent} />
            <Row label={t("overview.user")} value={stats.overview.memory_by_domain.user} />
            <Row label={t("overview.app")} value={stats.overview.memory_by_domain.app} />
            <Row label={t("overview.organization")} value={stats.overview.memory_by_domain.organization} />
          </CardContent>
        </Card>

        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">{t("overview.levelBreakdown")}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <Row label={t("overview.localL1")} value={stats.overview.memory_by_level_and_scope.local.l1} />
            <Row label={t("overview.localL2")} value={stats.overview.memory_by_level_and_scope.local.l2} />
            <Row label={t("overview.sharedL1")} value={stats.overview.memory_by_level_and_scope.shared.l1} />
            <Row label={t("overview.sharedL2")} value={stats.overview.memory_by_level_and_scope.shared.l2} />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function UsersTab({ stats }: { stats: AppStats }) {
  const t = useTranslations("AppDetail");
  if (stats.users.length === 0) {
    return (
      <Card className="border-border/70">
        <CardContent className="pt-6">
          <div className="py-8 text-center">
            <Users className="mx-auto mb-4 h-12 w-12 text-muted-foreground" />
            <p className="text-muted-foreground">{t("users.empty")}</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="border-border/70">
      <CardHeader>
        <CardTitle className="text-sm">{t("users.title")}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-2">
          <div className="grid grid-cols-4 gap-4 border-b pb-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            <div>{t("users.userId")}</div>
            <div className="text-right">{t("users.events")}</div>
            <div className="text-right">{t("users.memories")}</div>
            <div className="text-right">{t("users.lastActivity")}</div>
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
  const t = useTranslations("AppDetail");
  if (stats.recent_activity.length === 0) {
    return (
      <Card className="border-border/70">
        <CardContent className="pt-6">
          <div className="py-8 text-center">
            <Activity className="mx-auto mb-4 h-12 w-12 text-muted-foreground" />
            <p className="text-muted-foreground">{t("activity.empty")}</p>
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
          <CardTitle className="text-sm">{t("activity.distributionTitle")}</CardTitle>
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
          <CardTitle className="text-sm">{t("activity.logTitle")}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            <div className="grid grid-cols-4 gap-4 border-b pb-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
              <div>{t("activity.timestamp")}</div>
              <div>{t("activity.user")}</div>
              <div>{t("activity.type")}</div>
              <div>{t("activity.stream")}</div>
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
  const t = useTranslations("AppDetail");
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
        <StatCard label={t("performance.totalStorage")} value={formatBytes(stats.performance.total_storage_bytes)} icon={Server} />
        <StatCard label={t("performance.eventStorage")} value={formatBytes(stats.performance.event_storage_bytes)} icon={Activity} />
        <StatCard label={t("performance.memoryStorage")} value={formatBytes(stats.performance.memory_storage_bytes)} icon={Database} />
        <StatCard label={t("performance.avgEventSize")} value={formatBytes(stats.performance.avg_event_size_bytes)} icon={FileText} />
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <StatCard label={t("performance.l1Rate")} value={formatPercentage(stats.performance.l1_generation_rate)} sub={t("performance.l1RateSub")} icon={Zap} />
        <StatCard label={t("performance.l2Rate")} value={formatPercentage(stats.performance.l2_generation_rate)} sub={t("performance.l2RateSub")} icon={TrendingUp} />
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
  const t = useTranslations("AppDetail");
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
        <h1 className="text-2xl font-bold">{t("error")}</h1>
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
              {t("breadcrumbApps")}
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
          <TabsTrigger value="overview">{t("tabs.overview")}</TabsTrigger>
          <TabsTrigger value="sharing">{t("tabs.sharing")}</TabsTrigger>
          <TabsTrigger value="keys">{t("tabs.keys")}</TabsTrigger>
          <TabsTrigger value="users">{t("tabs.users")}</TabsTrigger>
          <TabsTrigger value="activity">{t("tabs.activity")}</TabsTrigger>
          <TabsTrigger value="performance">{t("tabs.performance")}</TabsTrigger>
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
