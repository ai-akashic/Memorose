"use client";

import Link from "next/link";
import { useState } from "react";
import { Activity, Building2, Database, Layers, Loader2, Package, Plus, Share2, Users } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { api } from "@/lib/api";
import { useApps } from "@/lib/hooks";
import { useOrgScope } from "@/lib/org-scope";
import { formatNumber } from "@/lib/utils";
import { useTranslations } from "next-intl";

function AppMetric({
  label,
  value,
  sub,
  tone = "text-foreground/70",
}: {
  label: string;
  value: number;
  sub?: string;
  tone?: string;
}) {
  return (
    <div className="rounded-xl border border-border/60 bg-background/50 px-3 py-3">
      <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
        {label}
      </div>
      <div className={`mt-1 font-mono text-base ${tone}`}>{formatNumber(value)}</div>
      {sub ? <div className="mt-1 text-[11px] text-muted-foreground">{sub}</div> : null}
    </div>
  );
}

export default function AppsPage() {
  const t = useTranslations("Apps");
  const { orgId } = useOrgScope();
  const scopedOrgId = orgId.trim();
  const { data, isLoading, error, mutate } = useApps(scopedOrgId);
  const [appId, setAppId] = useState("");
  const [appName, setAppName] = useState("");
  const [creating, setCreating] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [messageTone, setMessageTone] = useState<"success" | "error">("success");

  async function handleCreateApp() {
    const normalizedAppId = appId.trim();
    if (!normalizedAppId) {
      setMessageTone("error");
      setMessage("app_id is required");
      return;
    }

    setCreating(true);
    setMessage(null);

    try {
      await api.createApp({
        app_id: normalizedAppId,
        org_id: scopedOrgId,
        name: appName.trim() || undefined,
      });
      setAppId("");
      setAppName("");
      setMessageTone("success");
      setMessage(`Application ${normalizedAppId} created in ${scopedOrgId}.`);
      await mutate();
    } catch (createError) {
      setMessageTone("error");
      setMessage(createError instanceof Error ? createError.message : "Failed to create application");
    } finally {
      setCreating(false);
    }
  }

  if (isLoading) {
    return (
      <div className="space-y-6">
        <div className="space-y-2">
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-4 w-80" />
        </div>
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,18rem)_minmax(0,1fr)]">
          <Skeleton className="h-64 rounded-3xl" />
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            {Array.from({ length: 4 }).map((_, index) => (
              <Skeleton key={index} className="h-64 rounded-3xl" />
            ))}
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <Card className="glass-card">
        <CardContent className="pt-6 text-sm text-destructive">{error.message}</CardContent>
      </Card>
    );
  }

  const apps = data?.apps ?? [];

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            <Building2 className="h-3.5 w-3.5" />
            {t("sectionLabel")}
          </div>
          <h1 className="mt-2 text-2xl font-bold tracking-tight text-foreground">{scopedOrgId}</h1>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            {t("description")}
          </p>
        </div>

        <div className="rounded-2xl border border-border/70 bg-card px-4 py-3">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("registeredApps")}
          </div>
          <div className="mt-1 font-mono text-2xl font-bold">{formatNumber(data?.total_count ?? 0)}</div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,20rem)_minmax(0,1fr)]">
        <Card className="glass-card border-border/70">
          <CardHeader className="space-y-2">
            <CardTitle className="text-sm">{t("create.title")}</CardTitle>
            <p className="text-sm text-muted-foreground">
              {t("create.description")}
            </p>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-1.5">
              <label className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                {t("create.orgIdLabel")}
              </label>
              <div className="rounded-xl border border-border/70 bg-background/60 px-3 py-2.5 font-mono text-sm">
                {scopedOrgId}
              </div>
            </div>

            <div className="space-y-1.5">
              <label className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                {t("create.appIdLabel")}
              </label>
              <Input
                value={appId}
                onChange={(event) => setAppId(event.target.value)}
                placeholder="support-console"
                className="font-mono"
              />
            </div>

            <div className="space-y-1.5">
              <label className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                {t("create.displayName")}
              </label>
              <Input
                value={appName}
                onChange={(event) => setAppName(event.target.value)}
                placeholder="Support Console"
              />
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

            <Button onClick={handleCreateApp} disabled={creating} className="w-full">
              {creating ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Plus className="mr-2 h-4 w-4" />}
              {t("create.button")}
            </Button>
          </CardContent>
        </Card>

        {apps.length === 0 ? (
          <Card className="glass-card border-border/70">
            <CardContent className="flex min-h-[20rem] flex-col items-center justify-center py-16 text-center">
              <Package className="mb-4 h-10 w-10 text-muted-foreground" />
              <p className="text-sm font-medium text-foreground">{t("empty.title")}</p>
              <p className="mt-1 max-w-sm text-sm text-muted-foreground">
                {t("empty.description", { orgId: scopedOrgId })}
              </p>
            </CardContent>
          </Card>
        ) : (
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            {apps.map((app) => (
              <Link key={app.app_id} href={`/apps/${encodeURIComponent(app.app_id)}/`}>
                <Card className="glass-card h-full border-border/70 transition-colors hover:border-primary/30 hover:bg-card/90">
                  <CardHeader className="space-y-4">
                    <div className="flex items-start justify-between gap-4">
                      <div className="min-w-0">
                        <CardTitle className="flex items-center gap-2 text-base">
                          <Package className="h-4 w-4 text-primary" />
                          <span className="truncate">{app.name}</span>
                        </CardTitle>
                        <div className="mt-2 space-y-1">
                          <p className="font-mono text-[11px] text-muted-foreground">{app.app_id}</p>
                          <p className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                            {t("card.lastActivity")}{" "}
                            <span className="font-mono normal-case tracking-normal text-foreground/70">
                              {app.last_activity
                                ? new Date(app.last_activity * 1000).toLocaleString()
                                : t("card.noTraffic")}
                            </span>
                          </p>
                        </div>
                      </div>
                      <div className="rounded-xl border border-border/70 bg-background/60 px-3 py-2 text-right">
                        <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                          {t("card.totalMemories")}
                        </div>
                        <div className="font-mono text-lg font-bold">{formatNumber(app.total_memories)}</div>
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 gap-3">
                      <AppMetric label={t("card.events")} value={app.total_events} tone="text-primary/80" />
                      <AppMetric label={t("card.users")} value={app.total_users} tone="text-foreground/80" />
                      <AppMetric label={t("card.local")} value={app.local_memories} tone="text-success" />
                      <AppMetric
                        label={t("card.shared")}
                        value={app.shared_app_memories + app.shared_org_memories}
                        tone="text-warning"
                      />
                    </div>

                    <div className="grid grid-cols-2 gap-3 border-t border-border/70 pt-4">
                      <div className="space-y-2">
                        <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                          <Layers className="h-3.5 w-3.5" />
                          {t("card.localDomains")}
                        </div>
                        <div className="space-y-1 text-sm">
                          <div className="flex items-center justify-between">
                            <span className="flex items-center gap-2 text-muted-foreground">
                              <Activity className="h-3.5 w-3.5" />
                              {t("card.agent")}
                            </span>
                            <span className="font-mono">{formatNumber(app.agent_memories)}</span>
                          </div>
                          <div className="flex items-center justify-between">
                            <span className="flex items-center gap-2 text-muted-foreground">
                              <Users className="h-3.5 w-3.5" />
                              {t("card.user")}
                            </span>
                            <span className="font-mono">{formatNumber(app.user_memories)}</span>
                          </div>
                        </div>
                      </div>

                      <div className="space-y-2">
                        <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                          <Share2 className="h-3.5 w-3.5" />
                          {t("card.sharedDomains")}
                        </div>
                        <div className="space-y-1 text-sm">
                          <div className="flex items-center justify-between">
                            <span className="flex items-center gap-2 text-muted-foreground">
                              <Package className="h-3.5 w-3.5" />
                              {t("card.app")}
                            </span>
                            <span className="font-mono">{formatNumber(app.shared_app_memories)}</span>
                          </div>
                          <div className="flex items-center justify-between">
                            <span className="flex items-center gap-2 text-muted-foreground">
                              <Database className="h-3.5 w-3.5" />
                              {t("card.org")}
                            </span>
                            <span className="font-mono">{formatNumber(app.shared_org_memories)}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
