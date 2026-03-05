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
import { Bot, Database, Activity } from "lucide-react";
import { formatNumber } from "@/lib/utils";
import type { AgentSummary } from "@/lib/types";
import { motion } from "framer-motion";

function formatRelativeTime(timestamp: number | null, now: number): string {
  if (!timestamp) return "Never";
  const diff = now - timestamp * 1000;
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
  delay = 0,
}: {
  label: string;
  value: number | string;
  icon: React.ElementType;
  color?: string;
  delay?: number;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 15 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay, ease: "easeOut" }}
      className="h-full"
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
            {typeof value === "number" ? formatNumber(value) : value}
          </div>
        </CardContent>
        <div className="absolute inset-0 bg-gradient-to-tr from-transparent via-white/[0.01] to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-700" />
      </Card>
    </motion.div>
  );
}

function AgentRow({ agent, index, now }: { agent: AgentSummary; index: number; now: number }) {
  const relTime = formatRelativeTime(agent.last_activity, now);
  const isRecent = agent.last_activity && now - agent.last_activity * 1000 < 3600000;

  return (
    <motion.tr
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.03, duration: 0.25 }}
      className="border-b border-white/[0.03] hover:bg-white/[0.03] transition-colors group"
    >
      <TableCell className="px-5">
        <div className="flex items-center gap-3">
          <div className="relative flex h-7 w-7 items-center justify-center rounded-lg bg-white/[0.02] border border-white/[0.04] group-hover:border-primary/20 transition-colors shrink-0">
            <Bot className="w-3.5 h-3.5 text-muted-foreground group-hover:text-primary transition-colors" />
            {isRecent && (
              <span className="absolute -top-0.5 -right-0.5 w-1 h-1 rounded-full bg-success shadow-[0_0_5px_rgba(34,197,94,0.8)] animate-pulse" />
            )}
          </div>
          <span className="font-mono text-xs text-foreground/80 group-hover:text-white transition-colors">{agent.agent_id}</span>
        </div>
      </TableCell>
      <TableCell className="text-center">
        <span className="font-mono text-[11px] text-foreground/60">{formatNumber(agent.total_memories)}</span>
      </TableCell>
      <TableCell className="text-center">
        <span className="font-mono text-[11px] text-primary/70">{formatNumber(agent.l1_count)}</span>
      </TableCell>
      <TableCell className="text-center">
        <span className="font-mono text-[11px] text-success/70">{formatNumber(agent.l2_count)}</span>
      </TableCell>
      <TableCell className="text-center">
        <span className="font-mono text-[11px] text-muted-foreground/50">{formatNumber(agent.total_events)}</span>
      </TableCell>
      <TableCell className="px-5">
        <span className={`font-mono text-[10px] uppercase tracking-wider ${isRecent ? "text-success/70" : "text-muted-foreground/30"}`}>{relTime}</span>
      </TableCell>
    </motion.tr>
  );
}

export default function AgentsPage() {
  const { data, isLoading, error } = useAgents();

  const totalMemories = data?.agents.reduce((sum, a) => sum + a.total_memories, 0) ?? 0;
  const totalEvents = data?.agents.reduce((sum, a) => sum + a.total_events, 0) ?? 0;

  return (
    <div className="space-y-8 relative pb-10">
      <div className="absolute top-0 right-0 w-[500px] h-[300px] blob-bg opacity-20 pointer-events-none -z-10 mix-blend-screen" />

      <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
        <div className="flex items-center gap-3">
          <div className="w-1 h-6 bg-primary/40 rounded-full" />
          <h1 className="text-sm font-bold tracking-[0.3em] uppercase text-muted-foreground/60">
            Agents
          </h1>
        </div>
      </motion.div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {isLoading ? (
          [1, 2, 3].map((i) => <Skeleton key={i} className="h-28 glass-card rounded-xl opacity-10 border-white/5" />)
        ) : (
          <>
            <KpiCard label="Count" value={data?.total_count ?? 0} icon={Bot} delay={0.1} />
            <KpiCard label="Total Memories" value={totalMemories} icon={Database} color="text-success" delay={0.15} />
            <KpiCard label="Total Events" value={totalEvents} icon={Activity} color="text-warning" delay={0.2} />
          </>
        )}
      </div>

      {/* Agent Table */}
      <div className="glass-card rounded-xl overflow-hidden border border-white/[0.06] shadow-2xl shadow-black/50">
        <TableHeader className="bg-white/[0.03]">
          <TableRow className="border-white/5 hover:bg-transparent">
            <TableHead className="text-[10px] uppercase tracking-[0.2em] text-muted-foreground/40 font-bold px-5">AGT</TableHead>
            <TableHead className="text-[10px] uppercase tracking-[0.2em] text-muted-foreground/40 font-bold text-center">MEM</TableHead>
            <TableHead className="text-[10px] uppercase tracking-[0.2em] text-muted-foreground/40 font-bold text-center">L1</TableHead>
            <TableHead className="text-[10px] uppercase tracking-[0.2em] text-muted-foreground/40 font-bold text-center">L2</TableHead>
            <TableHead className="text-[10px] uppercase tracking-[0.2em] text-muted-foreground/40 font-bold text-center">EVT</TableHead>
            <TableHead className="text-[10px] uppercase tracking-[0.2em] text-muted-foreground/40 font-bold px-5">ACT</TableHead>
          </TableRow>
        </TableHeader>

        {error ? (
          <div className="p-10 text-center">
             <span className="text-[10px] uppercase tracking-widest text-destructive font-bold">Sync Error</span>
          </div>
        ) : (
          <Table>
            <TableBody>
              {isLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <TableRow key={i} className="border-white/5">
                    <TableCell colSpan={6}>
                      <Skeleton className="h-8 w-full opacity-10" />
                    </TableCell>
                  </TableRow>
                ))
              ) : !data || data.agents.length === 0 ? (
                <TableRow className="border-white/5 hover:bg-transparent">
                  <TableCell colSpan={6} className="py-24">
                    <div className="flex flex-col items-center gap-3 text-center">
                      <div className="w-12 h-12 rounded-2xl bg-white/[0.02] border border-white/5 flex items-center justify-center">
                        <Bot className="w-5 h-5 opacity-10" />
                      </div>
                      <p className="text-[10px] uppercase tracking-widest text-muted-foreground/30 font-bold">Zero Registry</p>
                    </div>
                  </TableCell>
                </TableRow>
              ) : (
                data.agents.map((agent, i) => (
                  // eslint-disable-next-line react-hooks/purity
                  <AgentRow key={agent.agent_id} agent={agent} index={i} now={Date.now()} />
                ))
              )}
            </TableBody>
          </Table>
        )}
      </div>
    </div>
  );
}
