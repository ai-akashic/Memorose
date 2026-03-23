"use client";

import Link from "next/link";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { truncate } from "@/lib/utils";
import type { OrganizationKnowledgeItem } from "@/lib/types";
import { useTranslations } from "next-intl";

function formatKnowledgeTimestamp(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export function OrganizationKnowledgeDetail({
  orgId,
  knowledge,
  isLoading = false,
  showOpenDetailLink = false,
  showHeader = true,
  showPayload = true,
  showKeywords = true,
}: {
  orgId: string;
  knowledge?: OrganizationKnowledgeItem;
  isLoading?: boolean;
  showOpenDetailLink?: boolean;
  showHeader?: boolean;
  showPayload?: boolean;
  showKeywords?: boolean;
}) {
  const t = useTranslations("Organizations");

  if (isLoading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-8 w-40" />
        <Skeleton className="h-24 rounded-2xl" />
        <Skeleton className="h-32 rounded-2xl" />
      </div>
    );
  }

  if (!knowledge) {
    return (
      <div className="rounded-2xl border border-dashed border-border px-4 py-10 text-center text-sm text-muted-foreground">
        {t("knowledge.select")}
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {showHeader ? (
        <div>
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.detailTitle")}
          </div>
          <div className="mt-2 flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-lg font-semibold">
                {knowledge.unit.keywords[0] || truncate(knowledge.unit.content, 64)}
              </div>
              <div className="mt-1 font-mono text-[11px] text-muted-foreground">
                {knowledge.unit.id}
              </div>
            </div>
            <Badge variant="outline" className="font-mono text-[11px]">
              {knowledge.knowledge.membership.membership_count}/
              {knowledge.knowledge.history.contribution_count}
            </Badge>
          </div>
        </div>
      ) : null}

      {showOpenDetailLink ? (
        <div>
          <Link
            href={`/dashboard/organizations/${encodeURIComponent(orgId)}/knowledge/${encodeURIComponent(knowledge.unit.id)}`}
            className="text-xs font-medium uppercase tracking-[0.18em] text-primary hover:underline"
          >
            {t("knowledge.openDetail")}
          </Link>
        </div>
      ) : null}

      {showPayload ? (
        <div className="rounded-2xl border border-border/70 bg-card p-4">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.payload")}
          </div>
          <div className="mt-2 whitespace-pre-wrap text-sm leading-6 text-foreground/90">
            {knowledge.unit.content}
          </div>
        </div>
      ) : null}

      <div className="grid gap-3 md:grid-cols-2">
        <div className="rounded-xl border border-border/70 bg-card p-3">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.membershipCount")}
          </div>
          <div className="mt-2 font-mono text-lg">
            {knowledge.knowledge.membership.membership_count}
          </div>
        </div>
        <div className="rounded-xl border border-border/70 bg-card p-3">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.totalContributions")}
          </div>
          <div className="mt-2 font-mono text-lg">
            {knowledge.knowledge.history.contribution_count}
          </div>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-3">
        <div className="rounded-xl border border-border/70 bg-card p-3">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.activeContributions")}
          </div>
          <div className="mt-2 font-mono text-lg">
            {knowledge.knowledge.history.active_contribution_count}
          </div>
        </div>
        <div className="rounded-xl border border-border/70 bg-card p-3">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.candidateContributions")}
          </div>
          <div className="mt-2 font-mono text-lg">
            {knowledge.knowledge.history.candidate_contribution_count}
          </div>
        </div>
        <div className="rounded-xl border border-border/70 bg-card p-3">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.revokedContributions")}
          </div>
          <div className="mt-2 font-mono text-lg">
            {knowledge.knowledge.history.revoked_contribution_count}
          </div>
        </div>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <div className="rounded-2xl border border-border/70 bg-card p-4">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.membershipContributorGroups")}
          </div>
          <div className="mt-3 space-y-3">
            {knowledge.knowledge.membership.summary.contributors.map((summary) => (
              <div
                key={summary.contributor_user_id}
                className="rounded-xl border border-border/70 bg-background/50 p-3"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="font-mono text-sm">{summary.contributor_user_id}</div>
                  <Badge variant="outline" className="font-mono text-[11px]">
                    {summary.membership_count}
                  </Badge>
                </div>
                {summary.source_memory_types.length > 0 ? (
                  <div className="mt-2 flex flex-wrap gap-2">
                    {summary.source_memory_types.map((sourceType) => (
                      <span
                        key={sourceType}
                        className="rounded-full border border-border bg-card px-2 py-0.5 text-[11px] uppercase tracking-widest text-muted-foreground"
                      >
                        {sourceType}
                      </span>
                    ))}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        </div>

        <div className="rounded-2xl border border-border/70 bg-card p-4">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.membershipSourceTypeGroups")}
          </div>
          <div className="mt-3 space-y-3">
            {knowledge.knowledge.membership.summary.source_types.map((summary) => (
              <div
                key={summary.source_memory_type}
                className="rounded-xl border border-border/70 bg-background/50 p-3"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="font-mono text-sm uppercase">
                    {summary.source_memory_type}
                  </div>
                  <Badge variant="outline" className="font-mono text-[11px]">
                    {summary.membership_count}
                  </Badge>
                </div>
                {summary.contributor_user_ids.length > 0 ? (
                  <div className="mt-2 flex flex-wrap gap-2">
                    {summary.contributor_user_ids.map((userId) => (
                      <span
                        key={userId}
                        className="rounded-full border border-border bg-card px-2 py-0.5 font-mono text-[11px] text-muted-foreground"
                      >
                        {userId}
                      </span>
                    ))}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      </div>

      <div>
        <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
          {t("knowledge.membershipTitle")}
        </div>
        <div className="mt-3 space-y-3">
          {knowledge.knowledge.membership.memberships.map((membership) => (
            <div
              key={membership.source_id}
              className="rounded-xl border border-border/70 bg-card p-3"
            >
              <div className="flex items-center justify-between gap-3">
                <div className="min-w-0">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.source")}
                  </div>
                  <div className="mt-1 break-all font-mono text-[11px] text-foreground/80">
                    {membership.source_id}
                  </div>
                </div>
                <Badge variant="outline" className="uppercase text-[11px]">
                  active
                </Badge>
              </div>
              <div className="mt-3 grid gap-3 md:grid-cols-2">
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.contributor")}
                  </div>
                  <div className="mt-1 font-mono text-[11px] text-foreground/80">
                    {membership.contributor_user_id}
                  </div>
                </div>
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.approval")}
                  </div>
                  <div className="mt-1 font-mono text-[11px] text-foreground/80">
                    {membership.approval_mode ?? "—"}
                  </div>
                </div>
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.sourceType")}
                  </div>
                  <div className="mt-1 font-mono text-[11px] text-foreground/80">
                    {membership.source_memory_type ?? "—"}
                    {membership.source_level ? ` · L${membership.source_level}` : ""}
                  </div>
                </div>
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.activatedAt")}
                  </div>
                  <div className="mt-1 text-[11px] text-foreground/80">
                    {membership.activated_at
                      ? formatKnowledgeTimestamp(membership.activated_at)
                      : "—"}
                  </div>
                </div>
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.updatedAtLabel")}
                  </div>
                  <div className="mt-1 text-[11px] text-foreground/80">
                    {formatKnowledgeTimestamp(membership.updated_at)}
                  </div>
                </div>
              </div>
              {membership.source_content_preview ? (
                <div className="mt-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.sourcePreview")}
                  </div>
                  <div className="mt-1 text-sm leading-6 text-foreground/85">
                    {membership.source_content_preview}
                  </div>
                </div>
              ) : null}
              {membership.source_keywords.length > 0 ? (
                <div className="mt-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.sourceKeywords")}
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {membership.source_keywords.slice(0, 6).map((keyword) => (
                      <span
                        key={keyword}
                        className="rounded-full border border-border bg-background px-2 py-0.5 text-[11px] uppercase tracking-widest text-muted-foreground"
                      >
                        {keyword}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
              {membership.approved_by ? (
                <div className="mt-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.approvedBy")}
                  </div>
                  <div className="mt-1 font-mono text-[11px] text-foreground/80">
                    {membership.approved_by}
                  </div>
                </div>
              ) : null}
            </div>
          ))}
        </div>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <div className="rounded-2xl border border-border/70 bg-card p-4">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.historyContributorGroups")}
          </div>
          <div className="mt-3 space-y-3">
            {knowledge.knowledge.history.summary.contributors.map((summary) => (
              <div
                key={summary.contributor_user_id}
                className="rounded-xl border border-border/70 bg-background/50 p-3"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="font-mono text-sm">{summary.contributor_user_id}</div>
                  <Badge variant="outline" className="font-mono text-[11px]">
                    {summary.active_contribution_count}/{summary.contribution_count}
                  </Badge>
                </div>
                {summary.source_memory_types.length > 0 ? (
                  <div className="mt-2 flex flex-wrap gap-2">
                    {summary.source_memory_types.map((sourceType) => (
                      <span
                        key={sourceType}
                        className="rounded-full border border-border bg-card px-2 py-0.5 text-[11px] uppercase tracking-widest text-muted-foreground"
                      >
                        {sourceType}
                      </span>
                    ))}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        </div>

        <div className="rounded-2xl border border-border/70 bg-card p-4">
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.historySourceTypeGroups")}
          </div>
          <div className="mt-3 space-y-3">
            {knowledge.knowledge.history.summary.source_types.map((summary) => (
              <div
                key={summary.source_memory_type}
                className="rounded-xl border border-border/70 bg-background/50 p-3"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="font-mono text-sm uppercase">
                    {summary.source_memory_type}
                  </div>
                  <Badge variant="outline" className="font-mono text-[11px]">
                    {summary.active_contribution_count}/{summary.contribution_count}
                  </Badge>
                </div>
                {summary.contributor_user_ids.length > 0 ? (
                  <div className="mt-2 flex flex-wrap gap-2">
                    {summary.contributor_user_ids.map((userId) => (
                      <span
                        key={userId}
                        className="rounded-full border border-border bg-card px-2 py-0.5 font-mono text-[11px] text-muted-foreground"
                      >
                        {userId}
                      </span>
                    ))}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      </div>

      {showKeywords && knowledge.unit.keywords.length > 0 ? (
        <div>
          <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("knowledge.keywords")}
          </div>
          <div className="mt-2 flex flex-wrap gap-2">
            {knowledge.unit.keywords.map((keyword) => (
              <span
                key={keyword}
                className="rounded-full border border-border bg-card px-2 py-0.5 text-[11px] uppercase tracking-widest text-muted-foreground"
              >
                {keyword}
              </span>
            ))}
          </div>
        </div>
      ) : null}

      <div>
        <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
          {t("knowledge.historyTitle")}
        </div>
        <div className="mt-3 space-y-3">
          {knowledge.knowledge.history.contributions.map((contribution) => (
            <div
              key={contribution.source_id}
              className="rounded-xl border border-border/70 bg-card p-3"
            >
              <div className="flex items-center justify-between gap-3">
                <div className="min-w-0">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.source")}
                  </div>
                  <div className="mt-1 break-all font-mono text-[11px] text-foreground/80">
                    {contribution.source_id}
                  </div>
                </div>
                <Badge variant="outline" className="uppercase text-[11px]">
                  {contribution.status}
                </Badge>
              </div>
              <div className="mt-3 grid gap-3 md:grid-cols-2">
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.contributor")}
                  </div>
                  <div className="mt-1 font-mono text-[11px] text-foreground/80">
                    {contribution.contributor_user_id}
                  </div>
                </div>
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.approval")}
                  </div>
                  <div className="mt-1 font-mono text-[11px] text-foreground/80">
                    {contribution.approval_mode ?? "—"}
                  </div>
                </div>
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.sourceType")}
                  </div>
                  <div className="mt-1 font-mono text-[11px] text-foreground/80">
                    {contribution.source_memory_type ?? "—"}
                    {contribution.source_level ? ` · L${contribution.source_level}` : ""}
                  </div>
                </div>
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.candidateAt")}
                  </div>
                  <div className="mt-1 text-[11px] text-foreground/80">
                    {formatKnowledgeTimestamp(
                      contribution.candidate_at ?? knowledge.unit.transaction_time
                    )}
                  </div>
                </div>
                <div>
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.activatedAt")}
                  </div>
                  <div className="mt-1 text-[11px] text-foreground/80">
                    {contribution.activated_at
                      ? formatKnowledgeTimestamp(contribution.activated_at)
                      : "—"}
                  </div>
                </div>
              </div>
              {contribution.source_content_preview ? (
                <div className="mt-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.sourcePreview")}
                  </div>
                  <div className="mt-1 text-sm leading-6 text-foreground/85">
                    {contribution.source_content_preview}
                  </div>
                </div>
              ) : null}
              {contribution.source_keywords.length > 0 ? (
                <div className="mt-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.sourceKeywords")}
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {contribution.source_keywords.slice(0, 6).map((keyword) => (
                      <span
                        key={keyword}
                        className="rounded-full border border-border bg-background px-2 py-0.5 text-[11px] uppercase tracking-widest text-muted-foreground"
                      >
                        {keyword}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
              {contribution.approved_by ? (
                <div className="mt-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                    {t("knowledge.approvedBy")}
                  </div>
                  <div className="mt-1 font-mono text-[11px] text-foreground/80">
                    {contribution.approved_by}
                  </div>
                </div>
              ) : null}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
