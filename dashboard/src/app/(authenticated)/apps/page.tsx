"use client";

import { useState, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Package, Activity, ArrowRight } from "lucide-react";
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

function formatRelativeTime(timestamp: number | null, now: number): string {
  if (!timestamp) return "No activity";
  const diff = now - timestamp * 1000;
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "Just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function AppCard({ app, index, now }: { app: AppSummary; index: number; now: number }) {
  const relTime = formatRelativeTime(app.last_activity, now);
  const isRecent = app.last_activity && now - app.last_activity * 1000 < 3600000;
  const memTotal = app.total_memories;
  const l2Pct = memTotal > 0 ? (app.l2_count / memTotal) * 100 : 0;

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.04, duration: 0.3, ease: [0.16, 1, 0.3, 1] }}
    >
      <Link href={`/apps/${app.app_id}/`}>
        <Card className="glass-card group relative overflow-hidden hover:shadow-[0_0_40px_rgba(0,0,0,0.4)] transition-all duration-500 cursor-pointer h-full">
          

          <CardContent className="p-6 relative z-10 flex flex-col justify-between h-full">
            <div>
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-lg bg-card border border-border flex items-center justify-center group-hover:border-primary/20 group-hover:bg-primary/5 transition-all duration-500">
                    <Package className="w-3.5 h-3.5 text-muted-foreground group-hover:text-primary transition-colors" />
                  </div>
                  <h3 className="text-foreground/70 group-hover:text-white text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{app.app_id}</h3>
                </div>
                <div className={`w-1 h-1 rounded-full ${isRecent ? "bg-success shadow-[0_0_5px_rgba(34,197,94,0.8)]" : "bg-white/10"}`} />
              </div>

              <div className="grid grid-cols-3 gap-2 mb-6">
                {[
                  { label: "USR", value: formatNumber(app.total_users) },
                  { label: "EVT", value: formatNumber(app.total_events) },
                  { label: "MEM", value: formatNumber(app.total_memories) },
                ].map(({ label, value }) => (
                  <div key={label} className="flex flex-col gap-0.5">
                    <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{label}</span>
                    <span className="font-mono text-[11px] text-foreground/60">{value}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="space-y-1.5 pt-4 border-t border-border">
              <div className="flex items-center justify-between">
                <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">L2 Elevation</span>
                <span className="font-mono text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{l2Pct.toFixed(0)}%</span>
              </div>
              <div className="h-0.5 w-full bg-card rounded-full overflow-hidden">
                <div
                  className="h-full bg-primary transition-all duration-1000"
                  style={{ width: `${Math.min(l2Pct, 100)}%` }}
                />
              </div>
              <div className="flex justify-between items-center mt-1">
                <span className="font-mono italic text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{relTime}</span>
                <ArrowRight className="w-3 h-3 text-muted-foreground/10 group-hover:text-primary/40 group-hover:translate-x-0.5 transition-all" />
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
    <div className="space-y-8 relative pb-10">
      <div className="absolute top-0 right-0 w-[600px] h-[300px] blob-bg opacity-20 pointer-events-none -z-10 mix-blend-screen" />

      <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
        <div className="flex items-center gap-3">
          <div className="w-1 h-6 bg-primary/40 rounded-full" />
          <h1 className="text-sm font-bold tracking-[0.3em] uppercase text-muted-foreground/60">
            Applications
          </h1>
        </div>
        <p className="mt-2 ml-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
          {loading ? "Initializing…" : error ? "Sync failure" : `${apps?.total_count ?? 0} Instances deployed`}
        </p>
      </motion.div>

      {error && (
        <div className="glass-card rounded-xl border-destructive/20 bg-destructive/5 p-4 text-xs text-destructive flex items-center gap-3">
          <Activity className="w-4 h-4 shrink-0 opacity-70" />
          <span className="font-mono tracking-tight">{error}</span>
        </div>
      )}

      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 gap-3">
          {[...Array(8)].map((_, i) => (
            <Skeleton key={i} className="h-44 glass-card rounded-xl opacity-10" />
          ))}
        </div>
      ) : !apps || apps.total_count === 0 ? (
        <div className="glass-card rounded-2xl border-dashed py-24 flex flex-col items-center text-center gap-4">
          <div className="w-12 h-12 rounded-2xl bg-card border border-border flex items-center justify-center">
            <Package className="w-5 h-5 opacity-20" />
          </div>
          <div>
            <h3 className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Zero Registry</h3>
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 gap-3">
          {apps.apps.map((app, i) => (
            <AppCard key={app.app_id} app={app} index={i} now={Date.now()} />
          ))}
        </div>
      )}
    </div>
  );
}
