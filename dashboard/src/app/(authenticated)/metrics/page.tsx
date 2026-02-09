"use client";

import { useStats, useClusterStatus } from "@/lib/hooks";
import { useUserFilter } from "../layout";
import { formatNumber, formatDuration } from "@/lib/utils";
import {
  Activity,
  Database,
  GitBranch,
  ArrowRight,
  BarChart3,
} from "lucide-react";
import {
  BarChart,
  Bar,
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
import type { GraphData } from "@/lib/types";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

function StatCard({
  label,
  value,
  icon: Icon,
  color = "text-primary",
}: {
  label: string;
  value: string | number;
  icon: React.ElementType;
  color?: string;
}) {
  return (
    <Card>
      <CardContent className="pt-4 pb-3">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs text-muted-foreground">{label}</span>
          <Icon className={`w-3.5 h-3.5 ${color} opacity-60`} />
        </div>
        <div className="text-xl font-bold tracking-tight">{typeof value === "number" ? formatNumber(value) : value}</div>
      </CardContent>
    </Card>
  );
}

function MemoryFunnel({ stats }: { stats: NonNullable<ReturnType<typeof useStats>["data"]> }) {
  const steps = [
    { label: "Events", count: stats.total_events, bg: "bg-blue-500/10", text: "text-blue-400" },
    { label: "Pending (L0)", count: stats.pending_events, bg: "bg-warning/10", text: "text-warning" },
    { label: "L1 Units", count: stats.memory_by_level.l1, bg: "bg-primary/10", text: "text-primary" },
    { label: "L2 Insights", count: stats.memory_by_level.l2, bg: "bg-success/10", text: "text-success" },
  ];

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs flex items-center gap-1.5">
          <BarChart3 className="w-3.5 h-3.5 text-primary" />
          Memory Pipeline
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-between">
          {steps.map((step, i) => (
            <div key={step.label} className="flex items-center">
              <div className="text-center">
                <div className={`w-14 h-14 rounded-lg flex items-center justify-center ${step.bg}`}>
                  <span className={`text-sm font-bold ${step.text}`}>{formatNumber(step.count)}</span>
                </div>
                <p className="text-[11px] text-muted-foreground mt-1">{step.label}</p>
              </div>
              {i < steps.length - 1 && (
                <ArrowRight className="w-3.5 h-3.5 text-muted-foreground/50 mx-2" />
              )}
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function RelationDistribution({ graphData }: { graphData: GraphData | null }) {
  if (!graphData) return null;

  const COLORS = ["#387dff", "#22c55e", "#f59e0b", "#ef4444", "#a855f7", "#06b6d4", "#ec4899"];

  const data = Object.entries(graphData.stats.relation_distribution).map(([name, value]) => ({
    name,
    value,
  }));

  if (data.length === 0) {
    return (
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-xs">Relation Distribution</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground text-center py-8">No edges yet</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs">Relation Distribution</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={data}
                cx="50%"
                cy="50%"
                innerRadius={40}
                outerRadius={70}
                dataKey="value"
                label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(0)}%`}
              >
                {data.map((_, i) => (
                  <Cell key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{ background: "hsl(222 47% 11%)", border: "1px solid hsl(222 47% 18%)" }}
                labelStyle={{ color: "hsl(213 31% 91%)" }}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}

function ImportanceHistogram() {
  const [histData, setHistData] = useState<{ range: string; count: number }[]>([]);

  useEffect(() => {
    async function fetchData() {
      try {
        const res = await api.memories({ limit: 100, sort: "importance" });
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
  }, []);

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs">Importance Distribution</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={histData}>
              <XAxis dataKey="range" tick={{ fill: "hsl(215 20% 65%)", fontSize: 10 }} />
              <YAxis tick={{ fill: "hsl(215 20% 65%)", fontSize: 10 }} />
              <Tooltip
                contentStyle={{ background: "hsl(222 47% 11%)", border: "1px solid hsl(222 47% 18%)" }}
                labelStyle={{ color: "hsl(213 31% 91%)" }}
              />
              <Bar dataKey="count" fill="hsl(217 91% 60%)" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}

function WorkerStatus({ config }: { config: NonNullable<ReturnType<typeof useClusterStatus>["data"]> }) {
  const snapshotLogs = "snapshot_policy_logs" in config ? (config as any).snapshot_policy_logs : "N/A";
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs">Worker Configuration</CardTitle>
      </CardHeader>
      <CardContent className="space-y-2">
        <div className="flex justify-between text-xs">
          <span className="text-muted-foreground">Heartbeat Interval</span>
          <span className="font-mono">{config.config.heartbeat_interval_ms}ms</span>
        </div>
        <div className="flex justify-between text-xs">
          <span className="text-muted-foreground">Election Timeout</span>
          <span className="font-mono">{config.config.election_timeout_min_ms}-{config.config.election_timeout_max_ms}ms</span>
        </div>
        <div className="flex justify-between text-xs">
          <span className="text-muted-foreground">Snapshot Policy</span>
          <span className="font-mono">Every {snapshotLogs} logs</span>
        </div>
      </CardContent>
    </Card>
  );
}

export default function MetricsPage() {
  const { userId } = useUserFilter();
  const { data: stats, isLoading: statsLoading } = useStats(userId || undefined);
  const { data: cluster } = useClusterStatus();
  const [graphData, setGraphData] = useState<GraphData | null>(null);

  useEffect(() => {
    async function loadGraph() {
      try {
        const data = await api.graph(500, userId || undefined);
        setGraphData(data);
      } catch {
        // ignore
      }
    }
    loadGraph();
  }, [userId]);

  if (statsLoading) {
    return (
      <div className="space-y-6">
        <h1 className="text-lg font-semibold tracking-tight">Metrics & Monitoring</h1>
        <div className="grid grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <Card key={i}>
              <CardContent className="pt-4">
                <Skeleton className="h-4 w-24 mb-3" />
                <Skeleton className="h-8 w-16" />
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold tracking-tight">Metrics & Monitoring</h1>

      {/* KPI Row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Total Events" value={stats?.total_events ?? 0} icon={Activity} />
        <StatCard label="Pending" value={stats?.pending_events ?? 0} icon={Activity} color="text-warning" />
        <StatCard label="Memory Units" value={stats?.total_memory_units ?? 0} icon={Database} color="text-success" />
        <StatCard label="Graph Edges" value={stats?.total_edges ?? 0} icon={GitBranch} color="text-warning" />
      </div>

      {/* Pipeline Funnel */}
      {stats && <MemoryFunnel stats={stats} />}

      {/* Charts Row */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <ImportanceHistogram />
        <RelationDistribution graphData={graphData} />
      </div>

      {/* Worker Status */}
      {cluster && <WorkerStatus config={cluster} />}
    </div>
  );
}
