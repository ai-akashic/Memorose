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
import { DashboardHero } from "@/components/dashboard-chrome";

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
      <DashboardHero
        icon={Building2}
        kicker={t("knowledge.detailTitle")}
        title={title}
        description={t("knowledge.detailDescription", { orgId })}
        actions={
          <Button asChild variant="outline">
            <Link
              href={`/dashboard/organizations?knowledge=${encodeURIComponent(knowledgeId)}`}
            >
              <ArrowLeft className="mr-2 h-4 w-4" />
              {t("knowledge.backToList")}
            </Link>
          </Button>
        }
      />

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
