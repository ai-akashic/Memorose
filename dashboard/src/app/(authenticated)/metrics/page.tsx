"use client";

import { useStats, useClusterStatus } from "@/lib/hooks";
import { useUserFilter } from "../layout";
import { formatNumber } from "@/lib/utils";
import {
  Activity,
  Database,
  GitBranch,
  Layers,
} from "lucide-react";
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
import { RainbowWaterfall } from "@/components/RainbowWaterfall";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import type { GraphData } from "@/lib/types";
import { motion } from "framer-motion";

function NumberTicker({ value }: { value: number }) {
  const [displayValue, setDisplayValue] = useState(0);

  useEffect(() => {
    let startTimestamp: number | null = null;
    const duration = 1000;
    const startValue = displayValue;
    
    const step = (timestamp: number) => {
      if (!startTimestamp) startTimestamp = timestamp;
      const progress = Math.min((timestamp - startTimestamp) / duration, 1);
      
      const easeOutQuart = 1 - Math.pow(1 - progress, 4);
      const current = Math.floor(startValue + (value - startValue) * easeOutQuart);
      
      setDisplayValue(current);
      
      if (progress < 1) {
        window.requestAnimationFrame(step);
      }
    };
    window.requestAnimationFrame(step);
  }, [value]);

  return <span>{formatNumber(displayValue)}</span>;
}

function StatCard({
  label,
  value,
  icon: Icon,
  color = "text-primary",
  className = "",
}: {
  label: string;
  value: string | number;
  icon: React.ElementType;
  color?: string;
  className?: string;
}) {
  return (
    <Card className={`glass-card relative overflow-hidden group hover:bg-white/[0.04] transition-all duration-300 ${className}`}>
      <CardContent className="pt-4 pb-3 h-full flex flex-col justify-between relative z-10">
        <div className="flex items-center justify-between mb-2">
          <span className="text-[10px] text-muted-foreground/80 font-bold tracking-widest uppercase">{label}</span>
          <div className={`p-1.5 rounded-md bg-background/50 border border-white/5 ${color} shadow-sm group-hover:scale-110 transition-transform duration-300`}>
            <Icon className="w-3.5 h-3.5 opacity-80" />
          </div>
        </div>
        <div className="text-2xl font-bold tracking-tight text-foreground/90 font-mono">
          {typeof value === "number" ? <NumberTicker value={value} /> : value}
        </div>
      </CardContent>
      <div className="absolute inset-0 bg-gradient-to-tr from-transparent via-white/[0.02] to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
    </Card>
  );
}

function RelationDistribution({ graphData, className = "" }: { graphData: GraphData | null, className?: string }) {
  if (!graphData) return null;

  const COLORS = ["hsl(220 70% 50%)", "hsl(160 60% 45%)", "hsl(30 80% 55%)", "hsl(280 65% 60%)", "hsl(340 75% 55%)", "hsl(200 80% 60%)"];

  const data = Object.entries(graphData.stats.relation_distribution).map(([name, value]) => ({
    name,
    value,
  }));

  return (
    <Card className={`glass-card flex flex-col ${className}`}>
      <CardHeader className="pb-2 flex-shrink-0">
        <CardTitle className="text-[10px] uppercase tracking-widest font-bold text-muted-foreground">Neural Pathways</CardTitle>
      </CardHeader>
      <CardContent className="flex-1 flex flex-col justify-center items-center">
        {data.length === 0 ? (
          <p className="text-sm text-muted-foreground">No edges yet</p>
        ) : (
          <div className="h-full w-full min-h-[140px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={data}
                  cx="50%"
                  cy="50%"
                  innerRadius={75}
                  outerRadius={110}
                  paddingAngle={8}
                  dataKey="value"
                  stroke="none"
                  labelLine={false}
                >
                  {data.map((_, i) => (
                    <Cell key={i} fill={COLORS[i % COLORS.length]} className="outline-none" style={{ filter: `drop-shadow(0 0 8px ${COLORS[i % COLORS.length]}60)` }} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{ background: "rgba(10, 10, 15, 0.9)", backdropFilter: "blur(12px)", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "12px" }}
                  itemStyle={{ color: "hsl(0 0% 90%)", fontSize: "11px", fontWeight: "bold" }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function ImportanceHistogram({ className = "" }: { className?: string }) {
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
    <Card className={`glass-card flex flex-col ${className}`}>
      <CardHeader className="pb-0 flex-shrink-0">
        <CardTitle className="text-[10px] uppercase tracking-widest font-bold text-muted-foreground">Significance Density</CardTitle>
      </CardHeader>
      <CardContent className="flex-1 pt-6">
        <div className="h-full w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={histData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <defs>
                <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="hsl(217 91% 60%)" stopOpacity={0.5} />
                  <stop offset="95%" stopColor="hsl(217 91% 60%)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis dataKey="range" axisLine={false} tickLine={false} tick={{ fill: "hsl(215 20% 50%)", fontSize: 10, fontWeight: "bold" }} dy={10} />
              <YAxis axisLine={false} tickLine={false} tick={{ fill: "hsl(215 20% 50%)", fontSize: 10, fontWeight: "bold" }} />
              <Tooltip
                contentStyle={{ background: "rgba(10, 10, 15, 0.9)", backdropFilter: "blur(12px)", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "12px" }}
                itemStyle={{ color: "hsl(0 0% 90%)", fontSize: "12px", fontFamily: "monospace" }}
                cursor={{ stroke: 'rgba(255,255,255,0.2)', strokeWidth: 1 }}
              />
              <Area 
                type="monotone" 
                dataKey="count" 
                stroke="hsl(217 91% 60%)" 
                strokeWidth={4} 
                fillOpacity={1} 
                fill="url(#colorCount)" 
                activeDot={{ r: 6, strokeWidth: 0, fill: "hsl(217 91% 70%)", style: { filter: "drop-shadow(0 0 10px hsl(217 91% 60%))" } }}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}

function WorkerStatus({ config, className = "" }: { config: NonNullable<ReturnType<typeof useClusterStatus>["data"]>, className?: string }) {
  const snapshotLogs = "snapshot_policy_logs" in config ? (config as any).snapshot_policy_logs : "N/A";
  return (
    <Card className={`glass-card flex flex-col ${className}`}>
      <CardHeader className="pb-4 border-b border-white/[0.05]">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-success animate-pulse shadow-[0_0_10px_hsl(142,76%,36%)]" />
          <CardTitle className="text-[10px] uppercase tracking-widest font-bold text-muted-foreground">Node Config</CardTitle>
        </div>
      </CardHeader>
      <CardContent className="space-y-4 pt-4">
        <div className="flex justify-between items-center text-xs">
          <span className="text-muted-foreground font-medium">Heartbeat</span>
          <span className="font-mono bg-white/5 px-2 py-1 rounded text-foreground/90 border border-white/5">{config.config.heartbeat_interval_ms}ms</span>
        </div>
        <div className="flex justify-between items-center text-xs">
          <span className="text-muted-foreground font-medium">Election</span>
          <span className="font-mono bg-white/5 px-2 py-1 rounded text-foreground/90 border border-white/5">{config.config.election_timeout_min_ms}ms</span>
        </div>
        <div className="flex justify-between items-center text-xs">
          <span className="text-muted-foreground font-medium">Snapshot</span>
          <span className="font-mono bg-white/5 px-2 py-1 rounded text-foreground/90 border border-white/5">{snapshotLogs} logs</span>
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
      <div className="space-y-6 h-full p-4 relative">
        <h1 className="text-2xl font-bold tracking-tight">Telemetry</h1>
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
      <div className="absolute top-[-20%] right-[-10%] w-[800px] h-[800px] blob-bg opacity-30 pointer-events-none -z-10 mix-blend-screen" />
      
      <div className="space-y-8">
        <motion.div 
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          className="flex items-center justify-between"
        >
          <h1 className="text-3xl font-bold tracking-tighter bg-clip-text text-transparent bg-gradient-to-b from-white to-white/40">
            System Telemetry
          </h1>
        </motion.div>

        <motion.div 
          initial={{ opacity: 0, scale: 0.98 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.6, ease: [0.16, 1, 0.3, 1] }}
          className="grid grid-cols-1 md:grid-cols-4 gap-4 auto-rows-[160px]"
        >
          {/* Top Row KPIs */}
          <StatCard label="Ingested Events" value={stats?.total_events ?? 0} icon={Activity} />
          <StatCard label="Pending Stream" value={stats?.pending_events ?? 0} icon={Activity} color="text-warning" />
          <StatCard label="Memory Clusters" value={stats?.total_memory_units ?? 0} icon={Database} color="text-success" />
          <StatCard label="Neural Edges" value={stats?.total_edges ?? 0} icon={GitBranch} color="text-chart-2" />

          {/* Pipeline Flow - Large Block */}
          <Card className="glass-card md:col-span-2 md:row-span-2 p-6 flex flex-col relative group overflow-hidden">
             <div className="absolute inset-0 bg-gradient-to-b from-primary/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
             <div className="flex items-center gap-2 mb-6 relative z-10">
               <Layers className="w-3.5 h-3.5 text-primary opacity-70" />
               <span className="text-[10px] uppercase tracking-[0.2em] font-bold text-muted-foreground/80">Cognitive Flow</span>
             </div>
             <div className="flex-1 relative z-10">
               {stats && <RainbowWaterfall stats={stats} />}
             </div>
          </Card>

          {/* Importance Distribution - Large Center Block */}
          <ImportanceHistogram className="md:col-span-2 md:row-span-2" />

          {/* Relation Dist - Large Block */}
          <RelationDistribution graphData={graphData} className="md:col-span-2 md:row-span-2" />

          {/* Worker Status - Vertical Block */}
          {cluster ? (
            <WorkerStatus config={cluster} className="md:col-span-1 md:row-span-2" />
          ) : (
            <div className="md:col-span-1 md:row-span-2 glass-card rounded-xl opacity-20" />
          )}
          
          <div className="md:col-span-1 md:row-span-2 flex flex-col gap-4">
            <StatCard label="Uptime" value="99.9%" icon={Activity} color="text-success" className="h-full flex-1" />
            <StatCard label="Region" value="Global" icon={Database} className="h-full flex-1" />
          </div>
        </motion.div>
      </div>
    </div>
  );
}
