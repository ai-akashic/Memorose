"use client";

import { useStats, useClusterStatus } from "@/lib/hooks";
import { formatNumber, formatBytes } from "@/lib/utils";
import {
  Activity,
  Clock3,
  Database,
  GitBranch,
  Layers,
  Search,
  Share2,
} from "lucide-react";
import { StatCard } from "@/components/stat-card";
import { RuntimeModeBanner } from "@/components/runtime-mode-banner";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from "recharts";
import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import { useOrgScope } from "@/lib/org-scope";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import type { GraphData } from "@/lib/types";
import { motion } from "framer-motion";
import { useTranslations } from "next-intl";
import { DashboardHero } from "@/components/dashboard-chrome";
import { StatusDot } from "@/components/status-dot";

const CHART_TOOLTIP_STYLE = {
  background: "rgba(0, 0, 0, 0.85)",
  backdropFilter: "blur(16px)",
  border: "1px solid rgba(255,255,255,0.08)",
  borderRadius: "12px",
  boxShadow: "0 8px 32px rgba(0,0,0,0.3)",
};

const CHART_TOOLTIP_ITEM_STYLE = {
  color: "hsl(0 0% 90%)",
  fontSize: "10px",
  fontWeight: "bold" as const,
  textTransform: "uppercase" as const,
};

function RelationDistribution({ graphData, className = "" }: { graphData: GraphData | null, className?: string }) {
  const t = useTranslations("Metrics");
  if (!graphData) return null;

  const COLORS = ["hsl(10 95% 67%)", "hsl(34 88% 62%)", "hsl(212 78% 63%)", "hsl(154 51% 52%)", "hsl(286 58% 68%)", "hsl(38 92% 56%)"];

  const data = Object.entries(graphData.stats.relation_distribution).map(([name, value]) => ({
    name,
    value,
  }));

  return (
    <Card className={`glass-card flex flex-col border-white/[0.04] ${className}`}>
      <CardHeader className="pb-2 flex-shrink-0">
        <CardTitle className="label-xs">{t("pathways")}</CardTitle>
      </CardHeader>
      <CardContent className="flex-1 flex flex-col justify-center items-center p-4">
        {data.length === 0 ? (
          <p className="label-xs">{t("pathwaysEmpty")}</p>
        ) : (
          <div className="h-full w-full min-h-[160px] flex-1">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={data}
                  cx="50%"
                  cy="50%"
                  innerRadius="55%"
                  outerRadius="85%"
                  paddingAngle={6}
                  dataKey="value"
                  stroke="none"
                  labelLine={false}
                >
                  {data.map((_, i) => (
                    <Cell key={i} fill={COLORS[i % COLORS.length]} className="outline-none opacity-60 hover:opacity-100 transition-opacity" style={{ filter: `drop-shadow(0 0 8px ${COLORS[i % COLORS.length]}40)` }} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={CHART_TOOLTIP_STYLE}
                  itemStyle={CHART_TOOLTIP_ITEM_STYLE}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function ImportanceHistogram({
  orgId,
  className = "",
}: {
  orgId?: string;
  className?: string;
}) {
  const t = useTranslations("Metrics");
  const [histData, setHistData] = useState<{ range: string; count: number }[]>([]);

  useEffect(() => {
    async function fetchData() {
      try {
        const res = await api.memories({ limit: 100, sort: "importance", org_id: orgId });
        const buckets = Array.from({ length: 10 }, (_, i) => ({
          range: `${(i / 10).toFixed(1)}`,
          count: 0,
        }));
        res.items.forEach((m) => {
          const idx = Math.min(Math.floor(m.importance * 10), 9);
          buckets[idx].count++;
        });
        setHistData(buckets);
      } catch {
        // ignore
      }
    }
    fetchData();
  }, [orgId]);

  return (
    <Card className={`glass-card flex flex-col border-white/[0.04] ${className}`}>
      <CardHeader className="pb-0 flex-shrink-0">
        <CardTitle className="label-xs">{t("density")}</CardTitle>
      </CardHeader>
      <CardContent className="flex-1 pt-6">
        <div className="h-full w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={histData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <defs>
                <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="hsl(10 95% 67%)" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="hsl(10 95% 67%)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis dataKey="range" hide />
              <YAxis hide />
              <Tooltip
                contentStyle={CHART_TOOLTIP_STYLE}
                itemStyle={{ ...CHART_TOOLTIP_ITEM_STYLE, fontFamily: "monospace" }}
                cursor={{ stroke: 'rgba(255,255,255,0.1)', strokeWidth: 1 }}
              />
              <Area
                type="monotone"
                dataKey="count"
                stroke="hsl(10 95% 67%)"
                strokeWidth={2}
                fillOpacity={1}
                fill="url(#colorCount)"
                activeDot={{ r: 4, strokeWidth: 0, fill: "hsl(10 95% 72%)" }}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}

function WorkerStatus({ config, className = "" }: { config: NonNullable<ReturnType<typeof useClusterStatus>["data"]>, className?: string }) {
  const t = useTranslations("Metrics");
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const snapshotLogs = "snapshot_policy_logs" in config ? (config as any).snapshot_policy_logs : "N/A";
  const worker = config.config.worker;
  return (
    <Card className={`glass-card flex flex-col border-white/[0.04] ${className}`}>
      <CardHeader className="pb-4 border-b border-border">
        <div className="flex items-center gap-2">
          <StatusDot status="healthy" size="sm" />
          <CardTitle className="label-xs">{t("node.title")}</CardTitle>
        </div>
      </CardHeader>
      <CardContent className="space-y-3 pt-4 px-5">
        {[
          { label: t("node.heartbeat"), value: `${config.config.heartbeat_interval_ms}ms` },
          { label: t("node.election"), value: `${config.config.election_timeout_min_ms}ms` },
          { label: t("node.snapshot"), value: `${snapshotLogs} ${t("node.logs")}` },
          { label: t("insight.interval"), value: `${worker.insight_interval_ms}ms` },
          { label: t("insight.minL1"), value: worker.insight_min_pending_l1 },
          { label: t("insight.minTokens"), value: worker.insight_min_pending_tokens },
          { label: t("insight.maxDelay"), value: `${Math.round(worker.insight_max_delay_ms / 3600000)}h` },
          { label: t("insight.batchTokens"), value: worker.insight_batch_target_tokens },
          { label: t("insight.batchL1"), value: worker.insight_max_l1_per_batch },
          { label: t("insight.batchCycles"), value: worker.insight_max_batches_per_cycle },
        ].map((item) => (
          <div key={item.label} className="flex flex-col gap-0.5">
            <span className="label-xs">{item.label}</span>
            <span className="font-mono text-[10px] text-foreground/70">{item.value}</span>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function BreakdownCard({
  title,
  rows,
  className = "",
}: {
  title: string;
  rows: Array<{ label: string; value: string | number; tone?: string }>;
  className?: string;
}) {
  return (
    <Card className={`glass-card border-white/[0.04] ${className}`}>
      <CardHeader className="pb-3">
        <CardTitle className="label-xs">
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {rows.map((row) => (
          <div key={row.label} className="flex items-center justify-between gap-4">
            <span className="text-sm text-muted-foreground">{row.label}</span>
            <span className={`font-mono text-sm ${row.tone ?? "text-foreground/80"}`}>
              {typeof row.value === "number" ? formatNumber(row.value) : row.value}
            </span>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

export default function MetricsPage() {
  const t = useTranslations("Metrics");
  const { orgId } = useOrgScope();
  const scopedOrgId = orgId.trim();
  const { data: stats, isLoading: statsLoading } = useStats(undefined, scopedOrgId || undefined);
  const { data: cluster } = useClusterStatus();
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const textIndexMetrics = stats?.text_index_metrics;
  const overlayLookups =
    (textIndexMetrics?.overlay_hit_total ?? 0) + (textIndexMetrics?.overlay_miss_total ?? 0);
  const overlayHitRate =
    overlayLookups > 0
      ? `${(((textIndexMetrics?.overlay_hit_total ?? 0) / overlayLookups) * 100).toFixed(1)}%`
      : "—";
  const avgCommitLatency =
    textIndexMetrics && textIndexMetrics.commit_total > 0
      ? `${(textIndexMetrics.commit_latency_total_ms / textIndexMetrics.commit_total).toFixed(1)}ms`
      : "—";

  useEffect(() => {
    async function loadGraph() {
      try {
        const data = await api.graph(500, undefined, scopedOrgId || undefined);
        setGraphData(data);
      } catch {
        // ignore
      }
    }
    loadGraph();
  }, [scopedOrgId]);

  if (statsLoading) {
    return (
      <div className="space-y-6 h-full p-4 relative">
        <h1 className="text-2xl font-bold tracking-tight">{t("title")}</h1>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 auto-rows-[160px]">
           {[1, 2, 3, 4, 5, 6, 7, 8].map((i) => (
             <Skeleton key={i} className="glass-card rounded-xl opacity-20" />
           ))}
        </div>
      </div>
    );
  }

  return (
    <div className="relative min-h-full pb-10">

      <div className="space-y-8">
        <DashboardHero
            icon={Activity}
            kicker={t("title")}
            title={t("title")}
            description={scopedOrgId ? t("subtitleOrg", { orgId: scopedOrgId }) : t("subtitle")}
          >
          </DashboardHero>

        <motion.div
          initial={{ opacity: 0, scale: 0.98 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.6, ease: [0.16, 1, 0.3, 1] }}
          className="space-y-3"
        >
          {cluster && <RuntimeModeBanner cluster={cluster} t={t} />}

          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6">
            <StatCard label={t("stats.ingested")} value={stats?.total_events ?? 0} icon={Activity} delay={0.1} compact />
            <StatCard label={t("stats.pending")} value={stats?.pending_events ?? 0} icon={Activity} color="text-warning" delay={0.15} compact />
            <StatCard label={t("stats.localMemory")} value={stats?.memory_by_scope.local ?? 0} icon={Database} color="text-success" delay={0.2} compact />
            <StatCard label={t("stats.sharedMemory")} value={stats?.memory_by_scope.shared ?? 0} icon={Share2} color="text-warning" delay={0.25} compact />
            <StatCard label={t("stats.totalMemory")} value={stats?.total_memory_units ?? 0} icon={Layers} color="text-primary" delay={0.3} compact />
            <StatCard label={t("stats.edges")} value={stats?.total_edges ?? 0} icon={GitBranch} color="text-primary" delay={0.35} compact />
          </div>

          <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
            <ImportanceHistogram orgId={scopedOrgId || undefined} className="min-h-[280px]" />
            <RelationDistribution graphData={graphData} className="min-h-[280px]" />
          </div>

          {textIndexMetrics && (
            <div className="space-y-3">
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-4">
                <StatCard
                  label={t("textIndex.dirtyDocs")}
                  value={textIndexMetrics.dirty_docs}
                  icon={Activity}
                  color="text-warning"
                  delay={0.18}
                  compact
                />
                <StatCard
                  label={t("textIndex.dirtyBytes")}
                  value={formatBytes(textIndexMetrics.dirty_bytes)}
                  icon={Database}
                  color="text-primary"
                  delay={0.2}
                  compact
                />
                <StatCard
                  label={t("textIndex.avgCommitLatency")}
                  value={avgCommitLatency}
                  icon={Clock3}
                  color="text-success"
                  delay={0.22}
                  compact
                />
                <StatCard
                  label={t("textIndex.overlayHitRate")}
                  value={overlayHitRate}
                  icon={Search}
                  color="text-primary"
                  delay={0.24}
                  compact
                />
              </div>

              <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
                <BreakdownCard
                  title={t("textIndex.writePathTitle")}
                  className="min-h-[220px]"
                  rows={[
                    {
                      label: t("textIndex.commitTotal"),
                      value: textIndexMetrics.commit_total,
                      tone: "text-primary",
                    },
                    {
                      label: t("textIndex.commitSeq"),
                      value: textIndexMetrics.commit_seq,
                      tone: "text-foreground/80",
                    },
                    {
                      label: t("textIndex.busySkips"),
                      value: textIndexMetrics.commit_skipped_busy_total,
                      tone: "text-warning",
                    },
                    {
                      label: t("textIndex.commitLatencyTotal"),
                      value: `${textIndexMetrics.commit_latency_total_ms}ms`,
                      tone: "text-success",
                    },
                  ]}
                />

                <BreakdownCard
                  title={t("textIndex.overlayTitle")}
                  className="min-h-[220px]"
                  rows={[
                    {
                      label: t("textIndex.overlayDocs"),
                      value: textIndexMetrics.overlay_docs,
                      tone: "text-primary",
                    },
                    {
                      label: t("textIndex.overlayBytes"),
                      value: formatBytes(textIndexMetrics.overlay_bytes),
                      tone: "text-foreground/80",
                    },
                    {
                      label: t("textIndex.overlayHits"),
                      value: textIndexMetrics.overlay_hit_total,
                      tone: "text-success",
                    },
                    {
                      label: t("textIndex.overlayMisses"),
                      value: textIndexMetrics.overlay_miss_total,
                      tone: "text-warning",
                    },
                    {
                      label: t("textIndex.overlayMerges"),
                      value: textIndexMetrics.overlay_merge_total,
                      tone: "text-accent",
                    },
                    {
                      label: t("textIndex.overlayEvictions"),
                      value: textIndexMetrics.overlay_evicted_total,
                      tone: "text-muted-foreground",
                    },
                  ]}
                />
              </div>
            </div>
          )}

          <div className="grid grid-cols-1 gap-3 xl:grid-cols-3">
            {cluster ? (
              <WorkerStatus config={cluster} className="min-h-[220px]" />
            ) : (
              <div className="glass-card min-h-[220px] rounded-xl opacity-10 animate-pulse" />
            )}

            <BreakdownCard
              title={t("domainMix.title")}
              className="min-h-[220px]"
              rows={[
                { label: t("domainMix.agent"), value: stats?.memory_by_domain.agent ?? 0, tone: "text-primary" },
                { label: t("domainMix.user"), value: stats?.memory_by_domain.user ?? 0, tone: "text-success" },
                {
                  label: t("domainMix.organization"),
                  value: stats?.memory_by_domain.organization ?? 0,
                  tone: "text-foreground/80",
                },
              ]}
            />

            <BreakdownCard
              title={t("levelByScope.title")}
              className="min-h-[220px]"
              rows={[
                { label: t("levelByScope.localL1"), value: stats?.memory_by_level_and_scope.local.l1 ?? 0, tone: "text-primary" },
                { label: t("levelByScope.localL2"), value: stats?.memory_by_level_and_scope.local.l2 ?? 0, tone: "text-success" },
                { label: t("levelByScope.localL3"), value: stats?.memory_by_level_and_scope.local.l3 ?? 0, tone: "text-accent" },
                { label: t("levelByScope.sharedL1"), value: stats?.memory_by_level_and_scope.shared.l1 ?? 0, tone: "text-warning" },
                {
                  label: t("levelByScope.sharedL2"),
                  value: stats?.memory_by_level_and_scope.shared.l2 ?? 0,
                  tone: "text-foreground/80",
                },
                {
                  label: t("levelByScope.sharedL3"),
                  value: stats?.memory_by_level_and_scope.shared.l3 ?? 0,
                  tone: "text-muted-foreground",
                },
              ]}
            />
          </div>
        </motion.div>
      </div>
    </div>
  );
}
