"use client";

import { useState, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Package, Users, Database, Activity, TrendingUp, ArrowRight } from "lucide-react";
import { formatNumber } from "@/lib/utils";
import Link from "next/link";
import { getToken } from "@/lib/auth";
import { motion } from "framer-motion";

interface AppSummary {
  app_id: string;
  total_events: number;
  total_users: number;
  total_memories: number;
  l1_count: number;
  l2_count: number;
  last_activity: number | null;
}

interface AppsResponse {
  apps: AppSummary[];
  total_count: number;
}

function formatRelativeTime(timestamp: number | null): string {
  if (!timestamp) return "No activity";
  const diff = Date.now() - timestamp * 1000;
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "Just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function AppCard({ app, index }: { app: AppSummary; index: number }) {
  const relTime = formatRelativeTime(app.last_activity);
  const isRecent = app.last_activity && Date.now() - app.last_activity * 1000 < 3600000;
  const memTotal = app.total_memories;
  const l2Pct = memTotal > 0 ? (app.l2_count / memTotal) * 100 : 0;

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.06, duration: 0.3, ease: [0.16, 1, 0.3, 1] }}
    >
      <Link href={`/apps/${app.app_id}/`}>
        <Card className="glass-card group relative overflow-hidden hover:border-primary/30 hover:shadow-[0_0_30px_rgba(56,125,255,0.08)] transition-all duration-300 cursor-pointer h-full">
          {/* Hover gradient */}
          <div className="absolute inset-0 bg-gradient-to-br from-primary/[0.05] to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300 pointer-events-none" />

          <CardContent className="p-5 relative z-10">
            {/* Header */}
            <div className="flex items-start justify-between mb-5">
              <div className="flex items-center gap-3">
                <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-primary/10 border border-primary/10 group-hover:bg-primary/20 group-hover:border-primary/20 transition-all shrink-0">
                  <Package className="w-4 h-4 text-primary" />
                </div>
                <div>
                  <h3 className="text-sm font-semibold group-hover:text-primary transition-colors leading-tight">{app.app_id}</h3>
                  <div className="flex items-center gap-1.5 mt-0.5">
                    <div className={`w-1.5 h-1.5 rounded-full ${isRecent ? "bg-success animate-pulse" : "bg-muted-foreground/30"}`} />
                    <span className="text-[10px] text-muted-foreground/60">{relTime}</span>
                  </div>
                </div>
              </div>
              <ArrowRight className="w-4 h-4 text-muted-foreground/30 group-hover:text-primary/60 group-hover:translate-x-0.5 transition-all shrink-0 mt-0.5" />
            </div>

            {/* Stats grid */}
            <div className="grid grid-cols-3 gap-3 mb-4">
              {[
                { label: "Users", value: formatNumber(app.total_users), icon: Users, color: "text-primary" },
                { label: "Events", value: formatNumber(app.total_events), icon: Activity, color: "text-warning" },
                { label: "Memories", value: formatNumber(app.total_memories), icon: Database, color: "text-success" },
              ].map(({ label, value, icon: Icon, color }) => (
                <div key={label} className="bg-white/[0.03] rounded-lg px-3 py-2.5 border border-white/5">
                  <div className="flex items-center gap-1 mb-1">
                    <Icon className={`w-3 h-3 ${color} opacity-70`} />
                    <span className="text-[9px] uppercase tracking-wider text-muted-foreground/60 font-semibold">{label}</span>
                  </div>
                  <span className="text-sm font-bold font-mono tabular-nums">{value}</span>
                </div>
              ))}
            </div>

            {/* Memory pipeline bar */}
            <div className="space-y-1.5">
              <div className="flex items-center justify-between text-[10px]">
                <span className="flex items-center gap-1 text-muted-foreground/60">
                  <TrendingUp className="w-2.5 h-2.5" />
                  L2 elevation
                </span>
                <span className="font-mono text-muted-foreground/60">{l2Pct.toFixed(0)}%</span>
              </div>
              <div className="h-1 rounded-full bg-white/5 overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-primary/60 to-success/60 rounded-full transition-all duration-700"
                  style={{ width: `${Math.min(l2Pct, 100)}%` }}
                />
              </div>
              <div className="flex items-center justify-between text-[10px] text-muted-foreground/40 font-mono">
                <span>L1: {formatNumber(app.l1_count)}</span>
                <span>L2: {formatNumber(app.l2_count)}</span>
              </div>
            </div>
          </CardContent>
        </Card>
      </Link>
    </motion.div>
  );
}

export default function AppsPage() {
  const [apps, setApps] = useState<AppsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchApps() {
      try {
        const response = await fetch("/v1/dashboard/apps", {
          headers: { Authorization: `Bearer ${getToken()}` },
        });
        if (!response.ok) throw new Error(`Failed to fetch apps: ${response.status}`);
        setApps(await response.json());
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setLoading(false);
      }
    }
    fetchApps();
  }, []);

  return (
    <div className="space-y-6 relative">
      <div className="absolute top-0 right-0 w-[600px] h-[300px] blob-bg opacity-20 pointer-events-none -z-10 mix-blend-screen" />

      <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
        <h1 className="text-3xl font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-b from-white to-white/60">
          Applications
        </h1>
        <p className="text-muted-foreground mt-1 text-sm">
          {loading ? "Loading…" : error ? "Error loading apps" : `${apps?.total_count ?? 0} ${apps?.total_count === 1 ? "application" : "applications"} registered`}
        </p>
      </motion.div>

      {error && (
        <div className="glass-card rounded-xl border border-destructive/30 p-4 text-sm text-destructive flex items-center gap-2">
          <Activity className="w-4 h-4 shrink-0" />
          {error}
        </div>
      )}

      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[...Array(6)].map((_, i) => (
            <Skeleton key={i} className="h-52 glass-card rounded-xl opacity-20" />
          ))}
        </div>
      ) : !apps || apps.total_count === 0 ? (
        <div className="glass-card rounded-xl border border-dashed border-white/10 py-20 flex flex-col items-center text-center gap-4">
          <div className="w-16 h-16 rounded-2xl bg-white/[0.03] border border-white/5 flex items-center justify-center">
            <Package className="w-8 h-8 opacity-20" />
          </div>
          <div>
            <h3 className="text-base font-semibold text-foreground/70">No applications yet</h3>
            <p className="text-sm text-muted-foreground/60 mt-1 max-w-xs">
              Applications will appear here once you start sending events to the system
            </p>
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {apps.apps.map((app, i) => (
            <AppCard key={app.app_id} app={app} index={i} />
          ))}
        </div>
      )}
    </div>
  );
}
