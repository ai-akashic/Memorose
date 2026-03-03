"use client";

import { useAgents } from "@/lib/hooks";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Bot, Database, Activity, Zap } from "lucide-react";
import { formatNumber } from "@/lib/utils";
import type { AgentSummary } from "@/lib/types";
import { motion } from "framer-motion";

function formatRelativeTime(timestamp: number | null): string {
  if (!timestamp) return "Never";
  const diff = Date.now() - timestamp * 1000;
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "Just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function KpiCard({
  label,
  value,
  icon: Icon,
  color = "text-primary",
  glow = "shadow-primary/20",
}: {
  label: string;
  value: number | string;
  icon: React.ElementType;
  color?: string;
  glow?: string;
}) {
  return (
    <Card className="glass-card relative overflow-hidden group hover:bg-white/[0.04] transition-all duration-300">
      <CardContent className="pt-4 pb-3 relative z-10">
        <div className="flex items-center justify-between mb-3">
          <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground/70">{label}</span>
          <div className={`p-1.5 rounded-md bg-background/50 border border-white/5 ${color} shadow-sm ${glow} group-hover:scale-110 transition-transform duration-300`}>
            <Icon className="w-3.5 h-3.5 opacity-80" />
          </div>
        </div>
        <div className="text-2xl font-bold tracking-tight font-mono text-foreground/90">
          {typeof value === "number" ? formatNumber(value) : value}
        </div>
      </CardContent>
      <div className="absolute inset-0 bg-gradient-to-tr from-transparent via-white/[0.02] to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
    </Card>
  );
}

function AgentRow({ agent, index }: { agent: AgentSummary; index: number }) {
  const relTime = formatRelativeTime(agent.last_activity);
  const isRecent = agent.last_activity && Date.now() - agent.last_activity * 1000 < 3600000;

  return (
    <motion.tr
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.04, duration: 0.25 }}
      className="border-b border-white/5 hover:bg-white/[0.03] transition-colors group"
    >
      <TableCell>
        <div className="flex items-center gap-2.5">
          <div className="relative flex h-8 w-8 items-center justify-center rounded-lg bg-primary/10 border border-primary/10 group-hover:bg-primary/20 group-hover:border-primary/20 transition-colors shrink-0">
            <Bot className="w-3.5 h-3.5 text-primary" />
            {isRecent && (
              <span className="absolute -top-0.5 -right-0.5 w-2 h-2 rounded-full bg-success border border-background animate-pulse" />
            )}
          </div>
          <span className="font-mono text-xs text-foreground/90">{agent.agent_id}</span>
        </div>
      </TableCell>
      <TableCell className="text-center">
        <span className="font-mono text-sm font-semibold tabular-nums">{formatNumber(agent.total_memories)}</span>
      </TableCell>
      <TableCell className="text-center">
        <Badge variant="outline" className="text-xs font-mono bg-primary/5 text-primary border-primary/20 tabular-nums">
          {formatNumber(agent.l1_count)}
        </Badge>
      </TableCell>
      <TableCell className="text-center">
        <Badge variant="outline" className="text-xs font-mono bg-success/5 text-success border-success/20 tabular-nums">
          {formatNumber(agent.l2_count)}
        </Badge>
      </TableCell>
      <TableCell className="text-center">
        <span className="font-mono text-sm tabular-nums">{formatNumber(agent.total_events)}</span>
      </TableCell>
      <TableCell>
        <div className="flex flex-col">
          <span className={`text-xs font-medium ${isRecent ? "text-success" : "text-muted-foreground"}`}>{relTime}</span>
          {agent.last_activity && (
            <span className="text-[10px] text-muted-foreground/50 font-mono">
              {new Date(agent.last_activity * 1000).toLocaleDateString()}
            </span>
          )}
        </div>
      </TableCell>
    </motion.tr>
  );
}

export default function AgentsPage() {
  const { data, isLoading, error } = useAgents();

  const totalMemories = data?.agents.reduce((sum, a) => sum + a.total_memories, 0) ?? 0;
  const totalEvents = data?.agents.reduce((sum, a) => sum + a.total_events, 0) ?? 0;

  return (
    <div className="space-y-6 relative">
      <div className="absolute top-0 right-0 w-[500px] h-[300px] blob-bg opacity-20 pointer-events-none -z-10 mix-blend-screen" />

      <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
        <h1 className="text-3xl font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-b from-white to-white/60">
          Agents
        </h1>
        <p className="text-muted-foreground mt-1 text-sm">Memory metrics grouped by agent_id</p>
      </motion.div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {isLoading ? (
          [1, 2, 3].map((i) => <Skeleton key={i} className="h-[88px] glass-card rounded-xl opacity-30" />)
        ) : (
          <>
            <KpiCard label="Total Agents" value={data?.total_count ?? 0} icon={Bot} />
            <KpiCard label="Total Memories" value={totalMemories} icon={Database} color="text-success" glow="shadow-success/20" />
            <KpiCard label="Total Events" value={totalEvents} icon={Activity} color="text-warning" glow="shadow-warning/20" />
          </>
        )}
      </div>

      {/* Agent Table */}
      <div className="glass-card rounded-xl overflow-hidden border border-white/[0.06]">
        {/* Header */}
        <div className="px-5 py-3 border-b border-white/5 flex items-center gap-2">
          <Zap className="w-3.5 h-3.5 text-primary opacity-70" />
          <span className="text-[10px] uppercase tracking-widest font-bold text-muted-foreground/70">Agent Registry</span>
          {data && (
            <span className="ml-auto text-[10px] font-mono text-muted-foreground/40">{data.total_count} agents</span>
          )}
        </div>

        {error ? (
          <div className="p-6 text-sm text-destructive">
            Failed to load agents: {(error as Error).message}
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="border-white/5 hover:bg-transparent">
                <TableHead className="text-[10px] uppercase tracking-wider text-muted-foreground/60 font-semibold">Agent ID</TableHead>
                <TableHead className="text-[10px] uppercase tracking-wider text-muted-foreground/60 font-semibold text-center">Memories</TableHead>
                <TableHead className="text-[10px] uppercase tracking-wider text-muted-foreground/60 font-semibold text-center">L1</TableHead>
                <TableHead className="text-[10px] uppercase tracking-wider text-muted-foreground/60 font-semibold text-center">L2</TableHead>
                <TableHead className="text-[10px] uppercase tracking-wider text-muted-foreground/60 font-semibold text-center">Events</TableHead>
                <TableHead className="text-[10px] uppercase tracking-wider text-muted-foreground/60 font-semibold">Last Active</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <TableRow key={i} className="border-white/5">
                    <TableCell colSpan={6}>
                      <Skeleton className="h-8 w-full opacity-20" />
                    </TableCell>
                  </TableRow>
                ))
              ) : !data || data.agents.length === 0 ? (
                <TableRow className="border-white/5 hover:bg-transparent">
                  <TableCell colSpan={6} className="py-16">
                    <div className="flex flex-col items-center gap-3 text-center">
                      <div className="w-14 h-14 rounded-2xl bg-white/[0.03] border border-white/5 flex items-center justify-center">
                        <Bot className="w-7 h-7 opacity-20" />
                      </div>
                      <div>
                        <p className="text-sm text-muted-foreground font-medium">No agents found</p>
                        <p className="text-xs text-muted-foreground/60 mt-0.5">Agents appear once memories with an agent_id are ingested</p>
                      </div>
                    </div>
                  </TableCell>
                </TableRow>
              ) : (
                data.agents.map((agent, i) => (
                  <AgentRow key={agent.agent_id} agent={agent} index={i} />
                ))
              )}
            </TableBody>
          </Table>
        )}
      </div>
    </div>
  );
}
