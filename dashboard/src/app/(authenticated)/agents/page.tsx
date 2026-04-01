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
import { useTranslations } from "next-intl";
import { useEffect, useState } from "react";
import { DashboardHero, DashboardStatRail } from "@/components/dashboard-chrome";

function formatRelativeTime(timestamp: number | null, now: number | null, t: ReturnType<typeof useTranslations>): string {
  if (!timestamp) return t("time.never");
  if (!now) return t("time.justNow");
  const diff = now - timestamp * 1000;
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return t("time.justNow");
  if (mins < 60) return t("time.minutesAgo", { count: mins });
  const hours = Math.floor(mins / 60);
  if (hours < 24) return t("time.hoursAgo", { count: hours });
  const days = Math.floor(hours / 24);
  return t("time.daysAgo", { count: days });
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
      <Card className="glass-card group relative overflow-hidden transition-all duration-500 h-full">
        <CardContent className="p-5 flex flex-col gap-4 h-full relative z-10">
          <div className="flex items-center gap-2">
            <Icon className={`w-4 h-4 ${color} opacity-60 group-hover:opacity-100 transition-opacity shrink-0`} />
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground truncate">
              {label}
            </span>
          </div>
          <div className="text-3xl font-bold tracking-tighter font-mono text-foreground/90 group-hover:text-white transition-colors">
            {typeof value === "number" ? formatNumber(value) : value}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  );
}

function AgentRow({ agent, index, now }: { agent: AgentSummary; index: number; now: number | null }) {
  const t = useTranslations("Agents");
  const relTime = formatRelativeTime(agent.last_activity, now, t);
  const isRecent = Boolean(agent.last_activity && now && now - agent.last_activity * 1000 < 3600000);

  return (
    <motion.tr
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.03, duration: 0.25 }}
      className="border-b border-border hover:bg-card transition-colors group"
    >
      <TableCell className="px-5">
        <div className="flex items-center gap-3">
          <div className="relative flex h-7 w-7 items-center justify-center rounded-lg bg-card border border-border group-hover:border-primary/20 transition-colors shrink-0">
            <Bot className="w-3.5 h-3.5 text-muted-foreground group-hover:text-primary transition-colors" />
            {isRecent && (
              <span className="absolute -top-0.5 -right-0.5 w-1 h-1 rounded-full bg-success animate-pulse" />
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
  const t = useTranslations("Agents");
  const { data, isLoading, error } = useAgents();
  const [now, setNow] = useState<number>(() => Date.now());

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 60_000);
    return () => window.clearInterval(timer);
  }, []);

  const totalMemories = data?.agents.reduce((sum, a) => sum + a.total_memories, 0) ?? 0;
  const totalEvents = data?.agents.reduce((sum, a) => sum + a.total_events, 0) ?? 0;

  return (
    <div className="space-y-8 relative pb-10">
      <div className="absolute top-0 right-0 w-[500px] h-[300px] blob-bg opacity-20 pointer-events-none -z-10 mix-blend-screen" />

      <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
        <DashboardHero
          icon={Bot}
          kicker={t("sectionLabel")}
          title={t("title")}
        >
          <DashboardStatRail
            items={[
              { label: t("kpi.count"), value: data?.total_count ?? 0, tone: "primary" },
              { label: t("kpi.memories"), value: totalMemories, tone: "success" },
              { label: t("kpi.events"), value: totalEvents, tone: "warning" },
            ]}
          />
        </DashboardHero>
      </motion.div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {isLoading ? (
          [1, 2, 3].map((i) => <Skeleton key={i} className="h-28 glass-card rounded-xl opacity-10" />)
        ) : (
          <>
            <KpiCard label={t("kpi.count")} value={data?.total_count ?? 0} icon={Bot} delay={0.1} />
            <KpiCard label={t("kpi.memories")} value={totalMemories} icon={Database} color="text-success" delay={0.15} />
            <KpiCard label={t("kpi.events")} value={totalEvents} icon={Activity} color="text-warning" delay={0.2} />
          </>
        )}
      </div>

      {/* Agent Table */}
      <div className="glass-card rounded-xl overflow-hidden">
        <Table>
          <TableHeader className="bg-card">
            <TableRow className="border-border hover:bg-transparent">
              <TableHead className="px-5 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.agent")}</TableHead>
              <TableHead className="text-center text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.memories")}</TableHead>
              <TableHead className="text-center text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.l1")}</TableHead>
              <TableHead className="text-center text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.l2")}</TableHead>
              <TableHead className="text-center text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.events")}</TableHead>
              <TableHead className="px-5 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.lastActive")}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {error ? (
              <TableRow className="border-border hover:bg-transparent">
                <TableCell colSpan={6} className="p-10 text-center">
                  <span className="text-destructive text-[11px] font-medium uppercase tracking-widest">{t("syncError")}</span>
                </TableCell>
              </TableRow>
            ) : isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i} className="border-border">
                  <TableCell colSpan={6}>
                    <Skeleton className="h-8 w-full opacity-10" />
                  </TableCell>
                </TableRow>
              ))
            ) : !data || data.agents.length === 0 ? (
              <TableRow className="border-border hover:bg-transparent">
                <TableCell colSpan={6} className="py-24">
                  <div className="flex flex-col items-center gap-3 text-center">
                    <div className="w-12 h-12 rounded-2xl bg-card border border-border flex items-center justify-center">
                      <Bot className="w-5 h-5 opacity-10" />
                    </div>
                    <p className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("empty")}</p>
                  </div>
                </TableCell>
              </TableRow>
            ) : (
              data.agents.map((agent, i) => (
                <AgentRow key={agent.agent_id} agent={agent} index={i} now={now} />
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
