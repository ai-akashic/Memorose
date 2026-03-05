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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value]);

  return <span>{formatNumber(displayValue)}</span>;
}

function StatCard({
  label,
  value,
  icon: Icon,
  color = "text-primary",
  className = "",
  delay = 0,
}: {
  label: string;
  value: string | number;
  icon: React.ElementType;
  color?: string;
  className?: string;
  delay?: number;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 15 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay, ease: "easeOut" }}
      className={`h-full ${className}`}
    >
      <Card className="glass-card group relative overflow-hidden hover:bg-white/[0.04] transition-all duration-500 h-full border-white/[0.04] hover:border-white/10">
        <CardContent className="p-5 flex flex-col justify-between h-full relative z-10">
          <div className="flex items-center justify-between">
            <Icon className={`w-4 h-4 ${color} opacity-60 group-hover:opacity-100 transition-opacity`} />
            <span className="text-[10px] uppercase tracking-[0.2em] text-muted-foreground/40 font-bold group-hover:text-muted-foreground/70 transition-colors">
              {label}
            </span>
          </div>
          <div className="text-3xl font-bold tracking-tighter font-mono text-foreground/90 mt-4 group-hover:text-white transition-colors">
            {typeof value === "number" ? <NumberTicker value={value} /> : value}
          </div>
        </CardContent>
        <div className="absolute inset-0 bg-gradient-to-tr from-transparent via-white/[0.01] to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-700" />
      </Card>
    </motion.div>
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
    <Card className={`glass-card flex flex-col border-white/[0.04] ${className}`}>
      <CardHeader className="pb-2 flex-shrink-0">
        <CardTitle className="text-[10px] uppercase tracking-[0.2em] font-bold text-muted-foreground/40">Pathways</CardTitle>
      </CardHeader>
      <CardContent className="flex-1 flex flex-col justify-center items-center p-4">
        {data.length === 0 ? (
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground/30 font-bold">Empty</p>
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
                  contentStyle={{ background: "rgba(0, 0, 0, 0.8)", backdropFilter: "blur(12px)", border: "1px solid rgba(255,255,255,0.05)", borderRadius: "8px" }}
                  itemStyle={{ color: "hsl(0 0% 90%)", fontSize: "10px", fontWeight: "bold", textTransform: "uppercase" }}
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
    <Card className={`glass-card flex flex-col border-white/[0.04] ${className}`}>
      <CardHeader className="pb-0 flex-shrink-0">
        <CardTitle className="text-[10px] uppercase tracking-[0.2em] font-bold text-muted-foreground/40">Density</CardTitle>
      </CardHeader>
      <CardContent className="flex-1 pt-6">
        <div className="h-full w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={histData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <defs>
                <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="hsl(217 91% 60%)" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="hsl(217 91% 60%)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis dataKey="range" hide />
              <YAxis hide />
              <Tooltip
                contentStyle={{ background: "rgba(0, 0, 0, 0.8)", backdropFilter: "blur(12px)", border: "1px solid rgba(255,255,255,0.05)", borderRadius: "8px" }}
                itemStyle={{ color: "hsl(0 0% 90%)", fontSize: "10px", fontFamily: "monospace" }}
                cursor={{ stroke: 'rgba(255,255,255,0.1)', strokeWidth: 1 }}
              />
              <Area 
                type="monotone" 
                dataKey="count" 
                stroke="hsl(217 91% 60%)" 
                strokeWidth={2} 
                fillOpacity={1} 
                fill="url(#colorCount)" 
                activeDot={{ r: 4, strokeWidth: 0, fill: "hsl(217 91% 70%)" }}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}

function WorkerStatus({ config, className = "" }: { config: NonNullable<ReturnType<typeof useClusterStatus>["data"]>, className?: string }) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const snapshotLogs = "snapshot_policy_logs" in config ? (config as any).snapshot_policy_logs : "N/A";
  return (
    <Card className={`glass-card flex flex-col border-white/[0.04] ${className}`}>
      <CardHeader className="pb-4 border-b border-white/[0.03]">
        <div className="flex items-center gap-2">
          <div className="w-1.5 h-1.5 rounded-full bg-success animate-pulse shadow-[0_0_5px_hsl(142,76%,36%)]" />
          <CardTitle className="text-[10px] uppercase tracking-[0.2em] font-bold text-muted-foreground/40">Node</CardTitle>
        </div>
      </CardHeader>
      <CardContent className="space-y-3 pt-4 px-5">
        {[
          { label: "Heartbeat", value: `${config.config.heartbeat_interval_ms}ms` },
          { label: "Election", value: `${config.config.election_timeout_min_ms}ms` },
          { label: "Snapshot", value: `${snapshotLogs} logs` },
        ].map((item) => (
          <div key={item.label} className="flex flex-col gap-0.5">
            <span className="text-[9px] uppercase tracking-widest text-muted-foreground/30 font-bold">{item.label}</span>
            <span className="font-mono text-[11px] text-foreground/70">{item.value}</span>
          </div>
        ))}
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
          <div className="flex items-center gap-3">
            <div className="w-1 h-6 bg-primary/40 rounded-full" />
            <h1 className="text-sm font-bold tracking-[0.3em] uppercase text-muted-foreground/60">
              Telemetry <span className="text-muted-foreground/20 ml-2">v1.0.4</span>
            </h1>
          </div>
        </motion.div>

        <motion.div 
          initial={{ opacity: 0, scale: 0.98 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.6, ease: [0.16, 1, 0.3, 1] }}
          className="grid grid-cols-1 md:grid-cols-4 gap-3 auto-rows-[140px]"
        >
          {/* Top Row KPIs */}
          <StatCard label="Ingested" value={stats?.total_events ?? 0} icon={Activity} delay={0.1} />
          <StatCard label="Pending" value={stats?.pending_events ?? 0} icon={Activity} color="text-warning" delay={0.15} />
          <StatCard label="Clusters" value={stats?.total_memory_units ?? 0} icon={Database} color="text-success" delay={0.2} />
          <StatCard label="Edges" value={stats?.total_edges ?? 0} icon={GitBranch} color="text-primary" delay={0.25} />

          {/* Pipeline Flow - Large Block */}
          <Card className="glass-card md:col-span-2 md:row-span-2 p-6 flex flex-col relative group overflow-hidden border-white/[0.04] hover:border-white/10 transition-colors">
             <div className="absolute inset-0 bg-gradient-to-b from-primary/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-700" />
             <div className="flex items-center gap-2 mb-6 relative z-10">
               <Layers className="w-3.5 h-3.5 text-primary opacity-40 group-hover:opacity-70 transition-opacity" />
               <span className="text-[10px] uppercase tracking-[0.3em] font-bold text-muted-foreground/30 group-hover:text-muted-foreground/60 transition-colors">Cognitive Flow</span>
             </div>
             <div className="flex-1 w-full h-full relative z-10 min-h-0 flex items-center justify-center pt-2 pb-4">
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
            <div className="md:col-span-1 md:row-span-2 glass-card rounded-xl opacity-10 animate-pulse border-white/5" />
          )}
          
          <div className="md:col-span-1 md:row-span-2 flex flex-col gap-3">
            <StatCard label="Health" value="99.9%" icon={Activity} color="text-success" className="h-full flex-1" delay={0.3} />
            <StatCard label="Region" value="Global" icon={Database} className="h-full flex-1" delay={0.35} />
          </div>
        </motion.div>
      </div>
    </div>
  );
}
