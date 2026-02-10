"use client";

import { useClusterStatus, useStats } from "@/lib/hooks";
import { useUserFilter } from "../layout";
import { isShardedCluster } from "@/lib/types";
import type { ClusterStatusSingle, ClusterStatusSharded, ShardStatus } from "@/lib/types";
import { formatNumber, formatDuration } from "@/lib/utils";
import {
  Activity,
  Database,
  GitBranch,
  Clock,
  Zap,
  Star,
  Server,
  Layers,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";

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
    <Card className="group hover:shadow-md hover:border-primary/20 transition-all duration-200">
      <CardContent className="pt-4 pb-3">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{label}</span>
          <div className={`p-1.5 rounded-lg bg-muted/50 group-hover:bg-primary/10 transition-colors`}>
            <Icon className={`w-3.5 h-3.5 ${color}`} />
          </div>
        </div>
        <div className="text-2xl font-bold tracking-tight">{typeof value === "number" ? formatNumber(value) : value}</div>
        {sub && <div className="text-xs text-muted-foreground mt-1">{sub}</div>}
      </CardContent>
    </Card>
  );
}

function ShardRaftCard({ shard }: { shard: ShardStatus }) {
  const stateColor = {
    Leader: "text-success",
    Follower: "text-primary",
    Candidate: "text-warning",
  }[shard.raft_state] || "text-muted-foreground";

  const isHealthy = shard.replication_lag <= 10;

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-xs flex items-center gap-1.5">
            <Layers className="w-3.5 h-3.5 text-primary" />
            Shard {shard.shard_id}
          </CardTitle>
          <div className="flex items-center gap-1.5">
            <div className={`w-1.5 h-1.5 rounded-full ${isHealthy ? 'bg-success' : 'bg-warning'} animate-pulse`} />
            <span className="text-[10px] text-muted-foreground">
              {isHealthy ? 'Healthy' : 'Degraded'}
            </span>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-2">
        {[
          { label: "State", value: shard.raft_state, className: `font-medium ${stateColor}` },
          { label: "Raft Node ID", value: shard.raft_node_id, className: "font-mono" },
          { label: "Term", value: shard.current_term, className: "font-mono" },
          { label: "Log Index", value: shard.last_log_index, className: "font-mono" },
          { label: "Applied", value: shard.last_applied, className: "font-mono" },
          { label: "Replication Lag", value: `${shard.replication_lag} entries`, className: `font-mono ${shard.replication_lag > 10 ? "text-warning" : "text-success"}` },
          { label: "Voters", value: shard.voters.length, className: "font-mono" },
        ].map((row) => (
          <div key={row.label} className="flex justify-between text-xs">
            <span className="text-muted-foreground">{row.label}</span>
            <span className={row.className}>{row.value}</span>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function RaftStatusCard({ cluster }: { cluster: ClusterStatusSingle }) {
  const stateColor = {
    Leader: "text-success",
    Follower: "text-primary",
    Candidate: "text-warning",
  }[cluster.raft_state] || "text-muted-foreground";

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs flex items-center gap-1.5">
          <GitBranch className="w-3.5 h-3.5 text-primary" />
          Raft Consensus
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-2">
        {[
          { label: "State", value: cluster.raft_state, className: `font-medium ${stateColor}` },
          { label: "Term", value: cluster.current_term, className: "font-mono" },
          { label: "Log Index", value: cluster.last_log_index, className: "font-mono" },
          { label: "Applied", value: cluster.last_applied, className: "font-mono" },
          { label: "Replication Lag", value: `${cluster.replication_lag} entries`, className: `font-mono ${cluster.replication_lag > 10 ? "text-warning" : "text-success"}` },
          { label: "Voters", value: cluster.voters.length, className: "font-mono" },
        ].map((row) => (
          <div key={row.label} className="flex justify-between text-xs">
            <span className="text-muted-foreground">{row.label}</span>
            <span className={row.className}>{row.value}</span>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function HeartbeatCard({ cluster }: { cluster: ClusterStatusSingle }) {
  const config = cluster.config || {
    heartbeat_interval_ms: 500,
    election_timeout_min_ms: 1500,
    election_timeout_max_ms: 3000,
  };

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs flex items-center gap-1.5">
          <Activity className="w-3.5 h-3.5 text-primary" />
          Heartbeat & Health
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Heartbeat Configuration */}
        <div className="space-y-2">
          <div className="flex justify-between text-xs">
            <span className="text-muted-foreground">Heartbeat Interval</span>
            <span className="font-mono">{config.heartbeat_interval_ms}ms</span>
          </div>
          <div className="flex justify-between text-xs">
            <span className="text-muted-foreground">Election Timeout</span>
            <span className="font-mono">{config.election_timeout_min_ms}-{config.election_timeout_max_ms}ms</span>
          </div>
        </div>

        <div className="border-t pt-2">
          <div className="text-xs text-muted-foreground mb-2">Node Status</div>
          <div className="space-y-2">
            {cluster.voters.map((nodeId) => {
              const isLeader = nodeId === cluster.current_leader;
              const isSelf = nodeId === cluster.node_id;
              const isHealthy = cluster.replication_lag === 0 || isLeader;

              return (
                <div key={nodeId} className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className={`w-2 h-2 rounded-full ${isHealthy ? 'bg-success' : 'bg-warning'} animate-pulse`} />
                    <span className="text-xs font-mono">
                      Node {nodeId}
                      {isSelf && <span className="ml-1 text-muted-foreground">(self)</span>}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    {isLeader && (
                      <Badge variant="outline" className="text-[10px] h-4 px-1.5 bg-success/10 text-success border-success/20">
                        Leader
                      </Badge>
                    )}
                    <span className="text-xs text-success">Online</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Health Summary */}
        <div className="rounded-lg bg-success/5 border border-success/20 p-2">
          <div className="flex items-center gap-2 text-xs text-success">
            <div className="w-1.5 h-1.5 rounded-full bg-success" />
            <span className="font-medium">All nodes healthy</span>
          </div>
          <p className="text-[10px] text-muted-foreground mt-1">
            Heartbeats active every {config.heartbeat_interval_ms}ms
          </p>
        </div>
      </CardContent>
    </Card>
  );
}

function PipelineCard({ stats }: { stats: NonNullable<ReturnType<typeof useStats>["data"]> }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs flex items-center gap-1.5">
          <Zap className="w-3.5 h-3.5 text-primary" />
          Memory Pipeline
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {[
          { label: "L0 (Pending)", count: stats.pending_events, color: "bg-warning" },
          { label: "L1 (Consolidated)", count: stats.memory_by_level.l1, color: "bg-primary" },
          { label: "L2 (Insights)", count: stats.memory_by_level.l2, color: "bg-success" },
        ].map((level) => (
          <div key={level.label}>
            <div className="flex justify-between text-xs mb-1">
              <span className="text-muted-foreground">{level.label}</span>
              <span className="font-mono">{formatNumber(level.count)}</span>
            </div>
            <div className="h-1.5 bg-muted rounded-full overflow-hidden">
              <div
                className={`h-full ${level.color} rounded-full transition-all`}
                style={{
                  width: `${Math.min(100, (level.count / Math.max(stats.total_memory_units, 1)) * 100)}%`,
                }}
              />
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function SingleShardTopology({ cluster }: { cluster: ClusterStatusSingle }) {
  const hasNodes = cluster.voters.length > 0 || cluster.learners.length > 0;

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs flex items-center gap-1.5">
          <Server className="w-3.5 h-3.5 text-primary" />
          Cluster Topology
        </CardTitle>
      </CardHeader>
      <CardContent>
        {hasNodes ? (
          <div className="flex items-center justify-center gap-5 py-3">
            {cluster.voters.map((nodeId) => {
              const isLeader = nodeId === cluster.current_leader;
              const isSelf = nodeId === cluster.node_id;
              return (
                <div key={nodeId} className="text-center">
                  <div
                    className={`w-14 h-14 rounded-xl flex items-center justify-center border-2 ${
                      isLeader
                        ? "border-success bg-success/10"
                        : "border-border bg-muted"
                    }`}
                  >
                    <div className="text-center">
                      {isLeader && <Star className="w-3 h-3 text-success mx-auto mb-0.5" />}
                      <span className={`text-xs font-mono ${isSelf ? "font-bold" : ""}`}>
                        N{nodeId}
                      </span>
                    </div>
                  </div>
                  <div className="text-xs text-muted-foreground mt-1.5">
                    {isLeader ? "Leader" : "Follower"}
                  </div>
                  <div className="text-xs text-muted-foreground font-mono">
                    :{3000 + nodeId - 1}
                  </div>
                </div>
              );
            })}
            {cluster.learners.map((nodeId) => (
              <div key={nodeId} className="text-center">
                <div className="w-14 h-14 rounded-xl flex items-center justify-center border-2 border-dashed border-border bg-muted/50">
                  <span className="text-xs font-mono">N{nodeId}</span>
                </div>
                <div className="text-xs text-muted-foreground mt-1.5">Learner</div>
              </div>
            ))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-6 text-center">
            <div className="w-14 h-14 rounded-xl flex items-center justify-center border-2 border-success bg-success/10 mb-2">
              <div className="text-center">
                <Star className="w-3 h-3 text-success mx-auto mb-0.5" />
                <span className="text-xs font-mono font-bold">N{cluster.node_id}</span>
              </div>
            </div>
            <div className="text-xs text-muted-foreground">
              {cluster.raft_state} &middot; Term {cluster.current_term}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function ShardedTopology({ cluster }: { cluster: ClusterStatusSharded }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs flex items-center gap-1.5">
          <Server className="w-3.5 h-3.5 text-primary" />
          Sharded Cluster &middot; Node {cluster.physical_node_id} &middot; {cluster.shard_count} Shards
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-center gap-4 py-3 flex-wrap">
          {cluster.shards.map((shard) => {
            const isLeader = shard.raft_state === "Leader";
            return (
              <div key={shard.shard_id} className="text-center">
                <div
                  className={`w-16 h-16 rounded-xl flex items-center justify-center border-2 ${
                    isLeader
                      ? "border-success bg-success/10"
                      : "border-border bg-muted"
                  }`}
                >
                  <div className="text-center">
                    {isLeader && <Star className="w-3 h-3 text-success mx-auto mb-0.5" />}
                    <span className="text-xs font-mono font-bold">S{shard.shard_id}</span>
                  </div>
                </div>
                <div className="text-xs text-muted-foreground mt-1.5">
                  {shard.raft_state}
                </div>
                <div className="text-xs text-muted-foreground font-mono">
                  T{shard.current_term}
                </div>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}

export default function ClusterPage() {
  const { userId } = useUserFilter();
  const { data: cluster, isLoading: clusterLoading } = useClusterStatus();
  const { data: stats, isLoading: statsLoading } = useStats(userId || undefined);

  if (clusterLoading || statsLoading) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Cluster Overview</h1>
          <p className="text-muted-foreground mt-2">Loading cluster information...</p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {[1, 2, 3].map((i) => (
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

  const sharded = cluster && isShardedCluster(cluster);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">
            Cluster Overview
          </h1>
          {sharded && (
            <p className="text-muted-foreground mt-2">
              Managing {(cluster as ClusterStatusSharded).shard_count} shards across distributed nodes
            </p>
          )}
          {!sharded && (
            <p className="text-muted-foreground mt-2">
              Single-node Raft cluster with distributed consensus
            </p>
          )}
        </div>
        {stats && (
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-muted/50">
            <Clock className="w-4 h-4 text-muted-foreground" />
            <span className="text-sm font-medium">{formatDuration(stats.uptime_seconds)}</span>
          </div>
        )}
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
        <StatCard
          label="Total Events"
          value={stats?.total_events ?? 0}
          sub={`${stats?.pending_events ?? 0} pending`}
          icon={Activity}
        />
        <StatCard
          label="Memory Units"
          value={stats?.total_memory_units ?? 0}
          sub={`L1: ${stats?.memory_by_level.l1 ?? 0} | L2: ${stats?.memory_by_level.l2 ?? 0}`}
          icon={Database}
          color="text-success"
        />
        <StatCard
          label="Graph Edges"
          value={stats?.total_edges ?? 0}
          icon={GitBranch}
          color="text-warning"
        />
        <StatCard
          label={sharded ? "Shards" : "Nodes"}
          value={sharded ? (cluster as ClusterStatusSharded).shard_count : (cluster as ClusterStatusSingle)?.voters.length ?? 0}
          sub={!sharded ? `Node ${(cluster as ClusterStatusSingle)?.node_id} is Leader` : undefined}
          icon={Server}
        />
      </div>

      {/* Topology */}
      {cluster && !sharded && <SingleShardTopology cluster={cluster as ClusterStatusSingle} />}
      {cluster && sharded && <ShardedTopology cluster={cluster as ClusterStatusSharded} />}

      {/* Raft Status per shard or single */}
      {cluster && sharded ? (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {(cluster as ClusterStatusSharded).shards.map((shard) => (
            <ShardRaftCard key={shard.shard_id} shard={shard} />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {cluster && !sharded && <RaftStatusCard cluster={cluster as ClusterStatusSingle} />}
          {cluster && !sharded && <HeartbeatCard cluster={cluster as ClusterStatusSingle} />}
          {stats && <PipelineCard stats={stats} />}
        </div>
      )}

      {/* Pipeline (shown separately for sharded) */}
      {sharded && stats && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <PipelineCard stats={stats} />
        </div>
      )}
    </div>
  );
}
