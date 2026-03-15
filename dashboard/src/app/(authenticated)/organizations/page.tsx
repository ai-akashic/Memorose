"use client";

import Link from "next/link";
import { useState } from "react";
import { ArrowRight, Building2, FolderPlus, Loader2, Plus, Package } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { api } from "@/lib/api";
import { useApps, useOrganizations } from "@/lib/hooks";
import { useOrgScope } from "@/lib/org-scope";
import { formatNumber } from "@/lib/utils";
import { useTranslations } from "next-intl";

export default function OrganizationsPage() {
  const t = useTranslations("Organizations");
  const { orgId, setOrgId } = useOrgScope();
  const scopedOrgId = orgId.trim();
  const { data: orgData, isLoading: loadingOrganizations, mutate: mutateOrganizations } = useOrganizations();
  const { data: appData, isLoading: loadingApps, mutate: mutateApps } = useApps(scopedOrgId);
  const [newOrgId, setNewOrgId] = useState("");
  const [newOrgName, setNewOrgName] = useState("");
  const [newAppId, setNewAppId] = useState("");
  const [newAppName, setNewAppName] = useState("");
  const [creatingOrg, setCreatingOrg] = useState(false);
  const [creatingApp, setCreatingApp] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [messageTone, setMessageTone] = useState<"success" | "error">("success");

  const organizations = orgData?.organizations ?? [];
  const apps = appData?.apps ?? [];

  async function handleCreateOrg() {
    const normalizedOrgId = newOrgId.trim();
    if (!normalizedOrgId) {
      setMessageTone("error");
      setMessage("org_id is required");
      return;
    }

    setCreatingOrg(true);
    setMessage(null);
    try {
      const organization = await api.createOrganization({
        org_id: normalizedOrgId,
        name: newOrgName.trim() || undefined,
      });
      setOrgId(organization.org_id);
      setNewOrgId("");
      setNewOrgName("");
      setMessageTone("success");
      setMessage(`Organization ${organization.org_id} created.`);
      await mutateOrganizations();
    } catch (error) {
      setMessageTone("error");
      setMessage(error instanceof Error ? error.message : "Failed to create organization");
    } finally {
      setCreatingOrg(false);
    }
  }

  async function handleCreateApp() {
    const normalizedAppId = newAppId.trim();
    if (!normalizedAppId) {
      setMessageTone("error");
      setMessage("app_id is required");
      return;
    }

    setCreatingApp(true);
    setMessage(null);
    try {
      await api.createApp({
        app_id: normalizedAppId,
        org_id: scopedOrgId,
        name: newAppName.trim() || undefined,
      });
      setNewAppId("");
      setNewAppName("");
      setMessageTone("success");
      setMessage(`Application ${normalizedAppId} created in ${scopedOrgId}.`);
      await mutateApps();
    } catch (error) {
      setMessageTone("error");
      setMessage(error instanceof Error ? error.message : "Failed to create application");
    } finally {
      setCreatingApp(false);
    }
  }

  if (loadingOrganizations) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[18rem_minmax(0,1fr)]">
          <Skeleton className="h-[26rem] rounded-3xl" />
          <Skeleton className="h-[26rem] rounded-3xl" />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            <Building2 className="h-3.5 w-3.5" />
            {t("sectionLabel")}
          </div>
          <h1 className="mt-2 text-2xl font-bold tracking-tight">{t("title")}</h1>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            {t("description")}
          </p>
        </div>

        <div className="rounded-2xl border border-border/70 bg-card px-4 py-3">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("activeOrg")}
          </div>
          <div className="mt-1 font-mono text-2xl font-bold">{scopedOrgId}</div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[20rem_minmax(0,1fr)]">
        <Card className="glass-card border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">{t("panel.title")}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="space-y-2">
              {organizations.map((organization) => {
                const selected = organization.org_id === scopedOrgId;
                return (
                  <button
                    key={organization.org_id}
                    type="button"
                    onClick={() => setOrgId(organization.org_id)}
                    className={`w-full rounded-2xl border px-3 py-3 text-left transition-colors ${
                      selected
                        ? "border-primary/30 bg-primary/5"
                        : "border-border/70 bg-background/50 hover:bg-muted/60"
                    }`}
                  >
                    <div className="truncate text-sm font-semibold">{organization.name}</div>
                    <div className="mt-1 font-mono text-[11px] text-muted-foreground">
                      {organization.org_id}
                    </div>
                  </button>
                );
              })}
            </div>

            <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
              <div className="mb-3 flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                <Plus className="h-3.5 w-3.5" />
                {t("createOrg.title")}
              </div>
              <div className="space-y-3">
                <Input
                  value={newOrgId}
                  onChange={(event) => setNewOrgId(event.target.value)}
                  placeholder={t("createOrg.orgIdPlaceholder")}
                  className="font-mono"
                />
                <Input
                  value={newOrgName}
                  onChange={(event) => setNewOrgName(event.target.value)}
                  placeholder={t("createOrg.displayName")}
                />
                <Button onClick={handleCreateOrg} disabled={creatingOrg} className="w-full">
                  {creatingOrg ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Building2 className="mr-2 h-4 w-4" />}
                  {t("createOrg.button")}
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>

        <div className="space-y-4">
          <Card className="glass-card border-border/70">
            <CardHeader>
              <CardTitle className="text-sm">{t("apps.createTitle", { orgId: scopedOrgId })}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-3 md:grid-cols-2">
                <Input
                  value={newAppId}
                  onChange={(event) => setNewAppId(event.target.value)}
                  placeholder={t("apps.appIdPlaceholder")}
                  className="font-mono"
                />
                <Input
                  value={newAppName}
                  onChange={(event) => setNewAppName(event.target.value)}
                  placeholder={t("apps.displayName")}
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

              <Button onClick={handleCreateApp} disabled={creatingApp}>
                {creatingApp ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <FolderPlus className="mr-2 h-4 w-4" />}
                {t("apps.createButton")}
              </Button>
            </CardContent>
          </Card>

          <Card className="glass-card border-border/70">
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle className="text-sm">{t("apps.title", { orgId: scopedOrgId })}</CardTitle>
                <p className="mt-1 text-sm text-muted-foreground">
                  {t("apps.description")}
                </p>
              </div>
              <div className="rounded-xl border border-border/70 bg-background/60 px-3 py-2 text-right">
                <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                  {t("apps.count")}
                </div>
                <div className="font-mono text-lg font-bold">{formatNumber(appData?.total_count ?? 0)}</div>
              </div>
            </CardHeader>
            <CardContent>
              {loadingApps ? (
                <Skeleton className="h-40 rounded-2xl" />
              ) : apps.length === 0 ? (
                <div className="rounded-2xl border border-dashed border-border px-4 py-10 text-center text-sm text-muted-foreground">
                  {t("apps.empty")}
                </div>
              ) : (
                <div className="space-y-3">
                  {apps.map((app) => (
                    <Link
                      key={app.app_id}
                      href={`/apps/${encodeURIComponent(app.app_id)}/`}
                      className="flex items-center justify-between rounded-2xl border border-border/70 bg-background/50 px-4 py-4 transition-colors hover:bg-muted/60"
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-2 text-sm font-semibold">
                          <Package className="h-4 w-4 text-primary" />
                          <span className="truncate">{app.name}</span>
                        </div>
                        <div className="mt-1 font-mono text-[11px] text-muted-foreground">
                          {app.app_id}
                        </div>
                      </div>
                      <div className="flex items-center gap-3">
                        <div className="text-right text-[11px] text-muted-foreground">
                          <div>{formatNumber(app.total_memories)} {t("apps.memories")}</div>
                          <div>{formatNumber(app.total_events)} {t("apps.events")}</div>
                        </div>
                        <ArrowRight className="h-4 w-4 text-muted-foreground" />
                      </div>
                    </Link>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
