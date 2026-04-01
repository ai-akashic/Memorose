"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { Building2, Loader2, Plus, Search, X } from "lucide-react";
import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { OrganizationKnowledgeDetail } from "@/components/organization-knowledge-detail";
import { api } from "@/lib/api";
import {
  useOrganizationKnowledge,
  useOrganizationKnowledgeDetail,
  useOrganizationKnowledgeMetrics,
  useOrganizations,
} from "@/lib/hooks";
import { useOrgScope } from "@/lib/org-scope";
import { formatNumber, truncate } from "@/lib/utils";
import { useTranslations } from "next-intl";
import { DashboardHero, DashboardStatRail } from "@/components/dashboard-chrome";

function formatKnowledgeTimestamp(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export default function OrganizationsPage() {
  const t = useTranslations("Organizations");
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { orgId, setOrgId } = useOrgScope();
  const scopedOrgId = orgId.trim();
  const selectedKnowledgeId = searchParams.get("knowledge")?.trim() || "";
  const queryText = searchParams.get("q")?.trim() || "";
  const contributorFilter = searchParams.get("contributor")?.trim() || "";
  const sourceTypeFilter = searchParams.get("source_type")?.trim() || "";
  const sort = searchParams.get("sort")?.trim() || "published_desc";
  const { data: orgData, isLoading: loadingOrganizations, mutate: mutateOrganizations } = useOrganizations();
  const {
    data: knowledgeData,
    isLoading: loadingKnowledge,
  } = useOrganizationKnowledge(scopedOrgId || undefined, {
    q: queryText || undefined,
    contributor: contributorFilter || undefined,
    source_type: sourceTypeFilter || undefined,
    sort,
  });
  const { data: selectedKnowledge, isLoading: loadingKnowledgeDetail } =
    useOrganizationKnowledgeDetail(scopedOrgId || undefined, selectedKnowledgeId || undefined);
  const { data: automationMetrics } = useOrganizationKnowledgeMetrics(scopedOrgId || undefined);
  const [draftQueryText, setDraftQueryText] = useState(queryText);
  const [newOrgId, setNewOrgId] = useState("");
  const [newOrgName, setNewOrgName] = useState("");
  const [creatingOrg, setCreatingOrg] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [messageTone, setMessageTone] = useState<"success" | "error">("success");

  const organizations = orgData?.organizations ?? [];
  const knowledgeItems = useMemo(() => knowledgeData?.items ?? [], [knowledgeData]);
  const contributorOptions = useMemo(
    () =>
      Array.from(
        new Set(knowledgeItems.flatMap((item) => item.contributor_user_ids))
      ).sort(),
    [knowledgeItems]
  );
  const sourceTypeOptions = useMemo(
    () =>
      Array.from(
        new Set(knowledgeItems.flatMap((item) => item.source_memory_types))
      ).sort(),
    [knowledgeItems]
  );

  const updateQueryParams = useCallback((
    updates: Record<string, string | null>,
    options?: { preserveKnowledge?: boolean }
  ) => {
    const next = new URLSearchParams(searchParams.toString());
    for (const [key, value] of Object.entries(updates)) {
      if (value && value.trim()) {
        next.set(key, value);
      } else {
        next.delete(key);
      }
    }
    if (!options?.preserveKnowledge && !("knowledge" in updates)) {
      next.delete("knowledge");
    }
    const query = next.toString();
    router.replace(query ? `${pathname}?${query}` : pathname);
  }, [pathname, router, searchParams]);

  useEffect(() => {
    setDraftQueryText(queryText);
  }, [queryText]);

  useEffect(() => {
    if (!knowledgeItems.length) {
      if (selectedKnowledgeId) {
        updateQueryParams({ knowledge: null }, { preserveKnowledge: true });
      }
      return;
    }

    if (!selectedKnowledgeId || !knowledgeItems.some((item) => item.unit.id === selectedKnowledgeId)) {
      updateQueryParams(
        { knowledge: knowledgeItems[0].unit.id },
        { preserveKnowledge: true }
      );
    }
  }, [knowledgeItems, selectedKnowledgeId, updateQueryParams]);

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
      updateQueryParams({ knowledge: null }, { preserveKnowledge: true });
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
      <DashboardHero
        icon={Building2}
        kicker={t("sectionLabel")}
        title={t("title")}
        description={t("description")}
        actions={
          <div className="dashboard-stat-pill min-w-[11rem]">
            <span className="dashboard-stat-label">{t("activeOrg")}</span>
            <span className="dashboard-stat-value font-mono text-primary">{scopedOrgId || "—"}</span>
          </div>
        }
      >
        <DashboardStatRail
          items={[
            { label: t("panel.title"), value: organizations.length, tone: "primary" },
            { label: t("knowledge.title"), value: knowledgeData?.total_count ?? 0, tone: "success" },
            { label: t("knowledge.metricPublished"), value: automationMetrics?.auto_publish_total ?? 0, tone: "warning" },
          ]}
        />
      </DashboardHero>

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
                    onClick={() => {
                      setOrgId(organization.org_id);
                      updateQueryParams({ knowledge: null }, { preserveKnowledge: true });
                    }}
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
              <CardTitle className="text-sm">{t("activeOrg")}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
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

              <div className="grid gap-4 md:grid-cols-2">
                <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("panel.title")}
                  </div>
                  <div className="mt-2 font-mono text-xl font-bold">{scopedOrgId}</div>
                </div>
                <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("sectionLabel")}
                  </div>
                  <div className="mt-2 font-mono text-xl font-bold">
                    {formatNumber(orgData?.total_count ?? 0)}
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
                <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                  {t("knowledge.policyTitle")}
                </div>
                <div className="mt-3 grid gap-3 md:grid-cols-3">
                  <div>
                    <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                      {t("knowledge.policyProjection")}
                    </div>
                    <div className="mt-1 text-sm">{t("knowledge.policyProjectionValue")}</div>
                  </div>
                  <div>
                    <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                      {t("knowledge.policyApproval")}
                    </div>
                    <div className="mt-1 text-sm">{t("knowledge.policyApprovalValue")}</div>
                  </div>
                  <div>
                    <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                      {t("knowledge.policyStorage")}
                    </div>
                    <div className="mt-1 text-sm">{t("knowledge.policyStorageValue")}</div>
                  </div>
                </div>
              </div>

              <div className="grid gap-3 md:grid-cols-5">
                <div className="rounded-xl border border-border/70 bg-background/50 p-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.metricAutoApproved")}
                  </div>
                  <div className="mt-2 font-mono text-lg">
                    {formatNumber(automationMetrics?.auto_approved_total ?? 0)}
                  </div>
                </div>
                <div className="rounded-xl border border-border/70 bg-background/50 p-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.metricPublished")}
                  </div>
                  <div className="mt-2 font-mono text-lg">
                    {formatNumber(automationMetrics?.auto_publish_total ?? 0)}
                  </div>
                </div>
                <div className="rounded-xl border border-border/70 bg-background/50 p-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.metricRebuilds")}
                  </div>
                  <div className="mt-2 font-mono text-lg">
                    {formatNumber(automationMetrics?.rebuild_total ?? 0)}
                  </div>
                </div>
                <div className="rounded-xl border border-border/70 bg-background/50 p-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.metricRevokes")}
                  </div>
                  <div className="mt-2 font-mono text-lg">
                    {formatNumber(automationMetrics?.revoke_total ?? 0)}
                  </div>
                </div>
                <div className="rounded-xl border border-border/70 bg-background/50 p-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.metricMerged")}
                  </div>
                  <div className="mt-2 font-mono text-lg">
                    {formatNumber(automationMetrics?.merged_publication_total ?? 0)}
                  </div>
                </div>
              </div>

              {automationMetrics?.source_type_distribution.length ? (
                <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.sourceDistribution")}
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {automationMetrics.source_type_distribution.map((item) => (
                      <span
                        key={item.key}
                        className="rounded-full border border-border bg-card px-2.5 py-1 font-mono text-[11px] uppercase tracking-widest text-muted-foreground"
                      >
                        {item.key} · {formatNumber(item.value)}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
            </CardContent>
          </Card>

          <Card className="glass-card border-border/70">
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle className="text-sm">{t("knowledge.title")}</CardTitle>
                <p className="mt-1 text-sm text-muted-foreground">
                  {t("knowledge.description", { orgId: scopedOrgId || "—" })}
                </p>
              </div>
              <div className="rounded-xl border border-border/70 bg-background/60 px-3 py-2 text-right">
                <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                  {t("knowledge.title")}
                </div>
                <div className="font-mono text-lg font-bold">{formatNumber(knowledgeData?.total_count ?? 0)}</div>
              </div>
            </CardHeader>
            <CardContent>
              <div className="mb-4 grid gap-3 lg:grid-cols-[minmax(0,1.2fr)_12rem_12rem_12rem_auto]">
                <div className="flex gap-2">
                  <Input
                    value={draftQueryText}
                    onChange={(event) => setDraftQueryText(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        updateQueryParams({ q: draftQueryText || null });
                      }
                    }}
                    placeholder={t("knowledge.searchPlaceholder")}
                    className="font-mono"
                  />
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => updateQueryParams({ q: draftQueryText || null })}
                  >
                    <Search className="mr-2 h-4 w-4" />
                    {t("knowledge.search")}
                  </Button>
                </div>

                <Select
                  value={contributorFilter || "__all__"}
                  onValueChange={(value) =>
                    updateQueryParams({ contributor: value === "__all__" ? null : value })
                  }
                >
                  <SelectTrigger>
                    <SelectValue placeholder={t("knowledge.allContributors")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="__all__">{t("knowledge.allContributors")}</SelectItem>
                    {contributorOptions.map((userId) => (
                      <SelectItem key={userId} value={userId}>
                        {userId}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>

                <Select
                  value={sourceTypeFilter || "__all__"}
                  onValueChange={(value) =>
                    updateQueryParams({ source_type: value === "__all__" ? null : value })
                  }
                >
                  <SelectTrigger>
                    <SelectValue placeholder={t("knowledge.allSourceTypes")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="__all__">{t("knowledge.allSourceTypes")}</SelectItem>
                    {sourceTypeOptions.map((sourceType) => (
                      <SelectItem key={sourceType} value={sourceType}>
                        {sourceType}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>

                <Select
                  value={sort}
                  onValueChange={(value) => updateQueryParams({ sort: value })}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={t("knowledge.sortPublished")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="published_desc">{t("knowledge.sortPublished")}</SelectItem>
                    <SelectItem value="contributions_desc">{t("knowledge.sortContributions")}</SelectItem>
                    <SelectItem value="active_desc">{t("knowledge.sortActive")}</SelectItem>
                    <SelectItem value="topic_asc">{t("knowledge.sortTopic")}</SelectItem>
                  </SelectContent>
                </Select>

                <Button
                  type="button"
                  variant="ghost"
                  onClick={() =>
                    updateQueryParams({
                      q: null,
                      contributor: null,
                      source_type: null,
                      sort: "published_desc",
                    })
                  }
                >
                  <X className="mr-2 h-4 w-4" />
                  {t("knowledge.clearFilters")}
                </Button>
              </div>

              <div className="mb-4 grid gap-3 md:grid-cols-4">
                <div className="rounded-xl border border-border/70 bg-background/50 p-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.metricKnowledge")}
                  </div>
                  <div className="mt-2 font-mono text-lg">
                    {formatNumber(knowledgeData?.summary.knowledge_count ?? 0)}
                  </div>
                </div>
                <div className="rounded-xl border border-border/70 bg-background/50 p-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.metricContributions")}
                  </div>
                  <div className="mt-2 font-mono text-lg">
                    {formatNumber(knowledgeData?.summary.contribution_count ?? 0)}
                  </div>
                </div>
                <div className="rounded-xl border border-border/70 bg-background/50 p-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.metricActive")}
                  </div>
                  <div className="mt-2 font-mono text-lg">
                    {formatNumber(knowledgeData?.summary.membership_count ?? 0)}
                  </div>
                </div>
                <div className="rounded-xl border border-border/70 bg-background/50 p-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.metricContributors")}
                  </div>
                  <div className="mt-2 font-mono text-lg">
                    {formatNumber(knowledgeData?.summary.contributor_count ?? 0)}
                  </div>
                </div>
              </div>

              {loadingKnowledge ? (
                <div className="space-y-3">
                  <Skeleton className="h-24 rounded-2xl" />
                  <Skeleton className="h-24 rounded-2xl" />
                </div>
              ) : knowledgeItems.length === 0 ? (
                <div className="rounded-2xl border border-dashed border-border px-4 py-10 text-center text-sm text-muted-foreground">
                  {t("knowledge.empty")}
                </div>
              ) : (
                <div className="grid gap-4 xl:grid-cols-[minmax(0,1.05fr)_minmax(20rem,0.95fr)]">
                  <div className="space-y-3">
                  {knowledgeItems.map((item) => {
                    const selected = item.unit.id === selectedKnowledgeId;
                    return (
                      <div
                        key={item.unit.id}
                        role="button"
                        tabIndex={0}
                        onClick={() =>
                          updateQueryParams(
                            { knowledge: item.unit.id },
                            { preserveKnowledge: true }
                          )
                        }
                        onKeyDown={(event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            updateQueryParams(
                              { knowledge: item.unit.id },
                              { preserveKnowledge: true }
                            );
                          }
                        }}
                        className={`w-full cursor-pointer rounded-2xl border px-4 py-4 text-left transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-primary/40 ${
                          selected
                            ? "border-primary/30 bg-primary/5"
                            : "border-border/70 bg-background/50 hover:bg-muted/60"
                        }`}
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="flex items-center gap-2 text-sm font-semibold">
                              <Building2 className="h-4 w-4 text-primary" />
                              <span className="truncate">
                                {item.unit.keywords[0] || truncate(item.unit.content, 48)}
                              </span>
                            </div>
                            <div className="mt-2 text-sm text-muted-foreground">
                              {truncate(item.unit.content, 140)}
                            </div>
                          </div>
                          <Badge variant="outline" className="shrink-0 font-mono text-[11px]">
                            {item.membership_count}/{item.contribution_count}
                          </Badge>
                        </div>

                        <div className="mt-3 flex flex-wrap gap-2">
                          {item.unit.keywords.slice(0, 4).map((keyword) => (
                            <span
                              key={keyword}
                              className="rounded-full border border-border bg-card px-2 py-0.5 text-[11px] uppercase tracking-widest text-muted-foreground"
                            >
                              {keyword}
                            </span>
                          ))}
                        </div>

                        <div className="mt-4 grid gap-3 md:grid-cols-3">
                          <div>
                            <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                              {t("knowledge.activeMemberships")}
                            </div>
                            <div className="mt-1 font-mono text-sm">
                              {item.membership_count}
                            </div>
                          </div>
                          <div>
                            <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                              {t("knowledge.totalContributions")}
                            </div>
                            <div className="mt-1 font-mono text-sm">
                              {item.contribution_count}
                            </div>
                          </div>
                          <div>
                            <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                              {t("knowledge.updatedAt")}
                            </div>
                            <div className="mt-1 text-sm">
                              {formatKnowledgeTimestamp(item.published_at)}
                            </div>
                          </div>
                        </div>
                        {item.contributor_user_ids.length > 0 ? (
                          <div className="mt-4">
                            <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                              {t("knowledge.contributors")}
                            </div>
                            <div className="mt-2 flex flex-wrap gap-2">
                              {item.contributor_user_ids.slice(0, 4).map((userId) => (
                                <span
                                  key={userId}
                                  className="rounded-full border border-border bg-card px-2 py-0.5 font-mono text-[11px] text-muted-foreground"
                                >
                                  {userId}
                                </span>
                              ))}
                            </div>
                          </div>
                        ) : null}
                        <div className="mt-4 grid gap-3 md:grid-cols-2">
                          <div>
                            <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                              {t("knowledge.topContributor")}
                            </div>
                            <div className="mt-1 font-mono text-sm">
                              {item.top_contributor_user_id ?? "—"}
                            </div>
                          </div>
                          <div>
                            <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                              {t("knowledge.primarySourceType")}
                            </div>
                            <div className="mt-1 font-mono text-sm uppercase">
                              {item.primary_source_memory_type ?? "—"}
                            </div>
                          </div>
                        </div>
                        <div className="mt-4">
                          <Link
                            href={`/dashboard/organizations/${encodeURIComponent(scopedOrgId)}/knowledge/${encodeURIComponent(item.unit.id)}`}
                            onClick={(event) => event.stopPropagation()}
                            className="text-xs font-medium uppercase tracking-[0.18em] text-primary hover:underline"
                          >
                            {t("knowledge.openDetail")}
                          </Link>
                        </div>
                      </div>
                    );
                  })}
                  </div>

                  <div className="rounded-2xl border border-border/70 bg-background/40 p-4">
                    <OrganizationKnowledgeDetail
                      orgId={scopedOrgId}
                      knowledge={selectedKnowledge}
                      isLoading={loadingKnowledgeDetail}
                      showOpenDetailLink
                    />
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
