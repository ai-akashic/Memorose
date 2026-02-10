"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Package, Users, Database, Activity } from "lucide-react";
import { formatNumber } from "@/lib/utils";
import Link from "next/link";
import { getToken } from "@/lib/auth";

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

function AppCard({ app }: { app: AppSummary }) {
  const lastActivity = app.last_activity
    ? new Date(app.last_activity * 1000).toLocaleString()
    : "No activity";

  return (
    <Link href={`/apps/${app.app_id}/`}>
      <Card className="group hover:shadow-lg hover:border-primary/50 transition-all duration-200 cursor-pointer h-full">
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary/10 group-hover:bg-primary/20 transition-colors">
              <Package className="w-4 h-4 text-primary" />
            </div>
            <span className="group-hover:text-primary transition-colors">{app.app_id}</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col space-y-1">
              <span className="text-xs text-muted-foreground flex items-center gap-1">
                <Users className="w-3 h-3" />
                Users
              </span>
              <span className="text-xl font-bold">
                {formatNumber(app.total_users)}
              </span>
            </div>
            <div className="flex flex-col space-y-1">
              <span className="text-xs text-muted-foreground flex items-center gap-1">
                <Activity className="w-3 h-3" />
                Events
              </span>
              <span className="text-xl font-bold">
                {formatNumber(app.total_events)}
              </span>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col space-y-1">
              <span className="text-xs text-muted-foreground flex items-center gap-1">
                <Database className="w-3 h-3" />
                Memories
              </span>
              <span className="text-lg font-semibold">
                {formatNumber(app.total_memories)}
              </span>
            </div>
            <div className="flex flex-col space-y-1">
              <span className="text-xs text-muted-foreground">L1 / L2</span>
              <span className="text-sm font-mono text-muted-foreground">
                {formatNumber(app.l1_count)} / {formatNumber(app.l2_count)}
              </span>
            </div>
          </div>

          <div className="pt-3 border-t space-y-1">
            <span className="text-xs text-muted-foreground">Last Activity</span>
            <div className="text-xs font-medium">{lastActivity}</div>
          </div>
        </CardContent>
      </Card>
    </Link>
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
          headers: {
            Authorization: `Bearer ${getToken()}`,
          },
        });

        if (!response.ok) {
          throw new Error(`Failed to fetch apps: ${response.status}`);
        }

        const data = await response.json();
        setApps(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setLoading(false);
      }
    }

    fetchApps();
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Applications</h1>
          <p className="text-muted-foreground mt-2">
            Loading applications...
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[...Array(6)].map((_, i) => (
            <Card key={i} className="h-[280px]">
              <CardHeader>
                <Skeleton className="h-6 w-32" />
              </CardHeader>
              <CardContent className="space-y-3">
                <Skeleton className="h-4 w-full" />
                <Skeleton className="h-4 w-3/4" />
                <Skeleton className="h-4 w-1/2" />
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-6">
        <h1 className="text-3xl font-bold tracking-tight">Applications</h1>
        <Card className="border-destructive/50">
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-destructive">
              <Activity className="w-5 h-5" />
              <span className="font-medium">Error: {error}</span>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (!apps || apps.total_count === 0) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Applications</h1>
          <p className="text-muted-foreground mt-2">
            Manage and monitor your applications
          </p>
        </div>
        <Card className="border-dashed">
          <CardContent className="pt-12 pb-12">
            <div className="flex flex-col items-center text-center">
              <div className="flex h-20 w-20 items-center justify-center rounded-full bg-muted">
                <Package className="w-10 h-10 text-muted-foreground" />
              </div>
              <h3 className="mt-6 text-lg font-semibold">No applications found</h3>
              <p className="text-sm text-muted-foreground mt-2 max-w-sm">
                Applications will appear here once you start sending events to the system
              </p>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Applications</h1>
          <p className="text-muted-foreground mt-2">
            {apps.total_count} {apps.total_count === 1 ? "application" : "applications"} found
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {apps.apps.map((app) => (
          <AppCard key={app.app_id} app={app} />
        ))}
      </div>
    </div>
  );
}
