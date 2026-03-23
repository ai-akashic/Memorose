"use client";

import Link from "next/link";
import { useEffect } from "react";
import { ArrowLeft, Building2 } from "lucide-react";
import { useParams } from "next/navigation";
import { useTranslations } from "next-intl";
import { OrganizationKnowledgeDetail } from "@/components/organization-knowledge-detail";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useOrganizationKnowledgeDetail } from "@/lib/hooks";
import { useOrgScope } from "@/lib/org-scope";
import { truncate } from "@/lib/utils";

export default function OrganizationKnowledgeDetailPage() {
  const t = useTranslations("Organizations");
  const params = useParams<{ orgId: string; knowledgeId: string }>();
  const { setOrgId } = useOrgScope();
  const orgId = params.orgId;
  const knowledgeId = params.knowledgeId;
  const { data: knowledge, isLoading } = useOrganizationKnowledgeDetail(orgId, knowledgeId);

  useEffect(() => {
    if (orgId) {
      setOrgId(orgId);
    }
  }, [orgId, setOrgId]);

  const title = knowledge?.unit.keywords[0] || (knowledge ? truncate(knowledge.unit.content, 64) : t("knowledge.detailTitle"));

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            <Building2 className="h-3.5 w-3.5" />
            {t("knowledge.detailTitle")}
          </div>
          <h1 className="mt-2 text-2xl font-bold tracking-tight">{title}</h1>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            {t("knowledge.detailDescription", { orgId })}
          </p>
        </div>

        <Button asChild variant="outline">
          <Link
            href={`/dashboard/organizations?knowledge=${encodeURIComponent(knowledgeId)}`}
          >
            <ArrowLeft className="mr-2 h-4 w-4" />
            {t("knowledge.backToList")}
          </Link>
        </Button>
      </div>

      <Card className="glass-card border-border/70">
        <CardHeader>
          <CardTitle className="text-sm">{t("knowledge.detailTitle")}</CardTitle>
        </CardHeader>
        <CardContent>
          {knowledge || isLoading ? (
            <OrganizationKnowledgeDetail
              orgId={orgId}
              knowledge={knowledge}
              isLoading={isLoading}
            />
          ) : (
            <div className="rounded-2xl border border-dashed border-border px-4 py-10 text-center text-sm text-muted-foreground">
              {t("knowledge.notFound")}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
