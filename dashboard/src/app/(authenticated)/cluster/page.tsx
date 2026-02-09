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
        <div className="text-xl font-bold tracking-tight">{typeof value === "number" ? formatNumber(value) : value}</div>
        {sub && <div className="text-[11px] text-muted-foreground mt-0.5">{sub}</div>}
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

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs flex items-center gap-1.5">
          <Layers className="w-3.5 h-3.5 text-primary" />
          Shard {shard.shard_id}
        </CardTitle>
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
        <h1 className="text-lg font-semibold tracking-tight">Cluster Overview</h1>
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
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold tracking-tight">
          Cluster Overview
          {sharded && (
            <span className="ml-2 text-xs font-normal text-muted-foreground">
              ({(cluster as ClusterStatusSharded).shard_count} shards)
            </span>
          )}
        </h1>
        {stats && (
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <Clock className="w-3 h-3" />
            {formatDuration(stats.uptime_seconds)}
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
          label={sharded ? "Shards" : "Node"}
          value={sharded ? (cluster as ClusterStatusSharded).shard_count : (cluster as ClusterStatusSingle)?.node_id ?? 0}
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
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {cluster && !sharded && <RaftStatusCard cluster={cluster as ClusterStatusSingle} />}
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
