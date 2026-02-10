"use client";

import { useState, useEffect } from "react";
import { useParams } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Package,
  Users,
  Database,
  Activity,
  TrendingUp,
  Server,
  FileText,
  Zap,
} from "lucide-react";
import { formatNumber } from "@/lib/utils";
import { getToken } from "@/lib/auth";
import Link from "next/link";

interface AppStats {
  app_id: string;
  overview: {
    total_events: number;
    total_users: number;
    total_memories: number;
    l1_count: number;
    l2_count: number;
    memory_pipeline_status: string;
    avg_memories_per_user: number;
  };
  users: Array<{
    user_id: string;
    event_count: number;
    memory_count: number;
    last_activity: number | null;
  }>;
  recent_activity: Array<{
    timestamp: number;
    user_id: string;
    event_type: string;
    stream_id: string;
  }>;
  performance: {
    total_storage_bytes: number;
    avg_event_size_bytes: number;
    l1_generation_rate: number;
    l2_generation_rate: number;
  };
}

function StatCard({
  label,
  value,
  sub,
  icon: Icon,
  color = "text-primary",
}: {
  label: string;
  value: string | number;
  sub?: string;
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
        <div className="text-xl font-bold tracking-tight">
          {typeof value === "number" ? formatNumber(value) : value}
        </div>
        {sub && <div className="text-[11px] text-muted-foreground mt-0.5">{sub}</div>}
      </CardContent>
    </Card>
  );
}

function OverviewTab({ stats }: { stats: AppStats }) {
  const pipelineColor =
    stats.overview.memory_pipeline_status === "healthy"
      ? "text-success"
      : stats.overview.memory_pipeline_status === "generating_l2"
      ? "text-warning"
      : "text-muted-foreground";

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard label="Total Events" value={stats.overview.total_events} icon={Activity} />
        <StatCard label="Total Users" value={stats.overview.total_users} icon={Users} />
        <StatCard
          label="Total Memories"
          value={stats.overview.total_memories}
          sub={`${stats.overview.l1_count} L1 / ${stats.overview.l2_count} L2`}
          icon={Database}
        />
        <StatCard
          label="Avg Memories/User"
          value={stats.overview.avg_memories_per_user.toFixed(1)}
          icon={TrendingUp}
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Memory Pipeline Status</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <div className={`w-2 h-2 rounded-full ${pipelineColor === "text-success" ? "bg-green-500" : pipelineColor === "text-warning" ? "bg-yellow-500" : "bg-gray-500"}`} />
            <span className={`font-medium ${pipelineColor}`}>
              {stats.overview.memory_pipeline_status.replace(/_/g, " ").toUpperCase()}
            </span>
          </div>
          <p className="text-sm text-muted-foreground mt-2">
            {stats.overview.memory_pipeline_status === "healthy"
              ? "All memory generation stages are working correctly"
              : stats.overview.memory_pipeline_status === "generating_l2"
              ? "L1 memories exist, L2 generation in progress"
              : "Waiting for events to generate memories"}
          </p>
        </CardContent>
      </Card>
    </div>
  );
}

function UsersTab({ stats }: { stats: AppStats }) {
  if (stats.users.length === 0) {
    return (
      <Card>
        <CardContent className="pt-6">
          <div className="text-center py-8">
            <Users className="w-12 h-12 mx-auto text-muted-foreground mb-4" />
            <p className="text-muted-foreground">No user activity yet</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">User Activity</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-2">
          <div className="grid grid-cols-4 gap-4 pb-2 border-b text-xs font-medium text-muted-foreground">
            <div>User ID</div>
            <div className="text-right">Events</div>
            <div className="text-right">Memories</div>
            <div className="text-right">Last Activity</div>
          </div>
          {stats.users.slice(0, 20).map((user) => (
            <div
              key={user.user_id}
              className="grid grid-cols-4 gap-4 py-2 text-sm border-b last:border-0"
            >
              <div className="font-mono truncate">{user.user_id}</div>
              <div className="text-right font-mono">{formatNumber(user.event_count)}</div>
              <div className="text-right font-mono">{formatNumber(user.memory_count)}</div>
              <div className="text-right text-muted-foreground text-xs">
                {user.last_activity
                  ? new Date(user.last_activity * 1000).toLocaleString()
                  : "N/A"}
              </div>
            </div>
          ))}
        </div>
        {stats.users.length > 20 && (
          <p className="text-xs text-muted-foreground mt-4 text-center">
            Showing top 20 of {stats.users.length} users
          </p>
        )}
      </CardContent>
    </Card>
  );
}

function ActivityTab({ stats }: { stats: AppStats }) {
  if (stats.recent_activity.length === 0) {
    return (
      <Card>
        <CardContent className="pt-6">
          <div className="text-center py-8">
            <Activity className="w-12 h-12 mx-auto text-muted-foreground mb-4" />
            <p className="text-muted-foreground">No recent activity</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  // Count event types
  const eventTypeCounts = stats.recent_activity.reduce((acc, activity) => {
    acc[activity.event_type] = (acc[activity.event_type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Event Type Distribution</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {Object.entries(eventTypeCounts).map(([type, count]) => (
              <div key={type} className="flex flex-col">
                <span className="text-xs text-muted-foreground capitalize">{type}</span>
                <span className="text-lg font-semibold">{formatNumber(count)}</span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Recent Activity Log</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            <div className="grid grid-cols-4 gap-4 pb-2 border-b text-xs font-medium text-muted-foreground">
              <div>Timestamp</div>
              <div>User</div>
              <div>Type</div>
              <div>Stream</div>
            </div>
            {stats.recent_activity.slice(0, 50).map((activity, idx) => (
              <div
                key={idx}
                className="grid grid-cols-4 gap-4 py-2 text-sm border-b last:border-0"
              >
                <div className="text-xs text-muted-foreground">
                  {new Date(activity.timestamp * 1000).toLocaleString()}
                </div>
                <div className="font-mono text-xs truncate">{activity.user_id}</div>
                <div>
                  <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-primary/10 text-primary">
                    {activity.event_type}
                  </span>
                </div>
                <div className="font-mono text-xs truncate">{activity.stream_id}</div>
              </div>
            ))}
          </div>
          {stats.recent_activity.length > 50 && (
            <p className="text-xs text-muted-foreground mt-4 text-center">
              Showing 50 of {stats.recent_activity.length} recent activities
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function PerformanceTab({ stats }: { stats: AppStats }) {
  const formatBytes = (bytes: number) => {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
  };

  const formatPercentage = (value: number) => `${(value * 100).toFixed(1)}%`;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          label="Total Storage"
          value={formatBytes(stats.performance.total_storage_bytes)}
          icon={Server}
        />
        <StatCard
          label="Avg Event Size"
          value={formatBytes(stats.performance.avg_event_size_bytes)}
          icon={FileText}
        />
        <StatCard
          label="L1 Generation Rate"
          value={formatPercentage(stats.performance.l1_generation_rate)}
          sub="Events → L1 Memories"
          icon={Zap}
        />
        <StatCard
          label="L2 Generation Rate"
          value={formatPercentage(stats.performance.l2_generation_rate)}
          sub="L1 → L2 Memories"
          icon={TrendingUp}
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Performance Insights</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div>
            <p className="text-sm font-medium">Storage Efficiency</p>
            <p className="text-sm text-muted-foreground">
              Average event size is {formatBytes(stats.performance.avg_event_size_bytes)}.
              Total storage usage is {formatBytes(stats.performance.total_storage_bytes)}.
            </p>
          </div>
          <div>
            <p className="text-sm font-medium">Memory Generation Pipeline</p>
            <p className="text-sm text-muted-foreground">
              {stats.performance.l1_generation_rate > 0
                ? `${formatPercentage(stats.performance.l1_generation_rate)} of events are being consolidated into L1 memories.`
                : "L1 memory generation has not started yet."}
              {stats.performance.l2_generation_rate > 0
                ? ` ${formatPercentage(stats.performance.l2_generation_rate)} of L1 memories are being elevated to L2 topics.`
                : " L2 topic generation is pending."}
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

export default function AppDetailClient() {
  const params = useParams();
  const app_id = params.app_id as string;
  const [stats, setStats] = useState<AppStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchStats() {
      try {
        const response = await fetch(`/v1/dashboard/apps/${app_id}/stats`, {
          headers: {
            Authorization: `Bearer ${getToken()}`,
          },
        });

        if (!response.ok) {
          throw new Error(`Failed to fetch app stats: ${response.status}`);
        }

        const data = await response.json();
        setStats(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setLoading(false);
      }
    }

    fetchStats();
  }, [app_id]);

  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-96" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-4">
        <h1 className="text-2xl font-bold">Application Details</h1>
        <Card>
          <CardContent className="pt-6">
            <div className="text-destructive">Error: {error}</div>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (!stats) {
    return null;
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Link href="/apps/" className="text-muted-foreground hover:text-foreground">
          Apps
        </Link>
        <span className="text-muted-foreground">/</span>
        <div className="flex items-center gap-2">
          <Package className="w-5 h-5 text-primary" />
          <h1 className="text-2xl font-bold">{app_id}</h1>
        </div>
      </div>

      <Tabs defaultValue="overview" className="w-full">
        <TabsList className="grid w-full grid-cols-4 max-w-2xl">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="users">Users</TabsTrigger>
          <TabsTrigger value="activity">Activity</TabsTrigger>
          <TabsTrigger value="performance">Performance</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-4">
          <OverviewTab stats={stats} />
        </TabsContent>

        <TabsContent value="users" className="mt-4">
          <UsersTab stats={stats} />
        </TabsContent>

        <TabsContent value="activity" className="mt-4">
          <ActivityTab stats={stats} />
        </TabsContent>

        <TabsContent value="performance" className="mt-4">
          <PerformanceTab stats={stats} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
