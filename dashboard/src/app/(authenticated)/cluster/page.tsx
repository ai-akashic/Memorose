"use client";

import { useClusterStatus, useStats, usePendingCount } from "@/lib/hooks";
import { isShardedCluster } from "@/lib/types";
import type { ClusterStatusSingle, ClusterStatusSharded, ShardStatus } from "@/lib/types";
import { formatNumber, formatDuration } from "@/lib/utils";
import { api } from "@/lib/api";
import { useOrgScope } from "@/lib/org-scope";
import {
  Activity,
  Database,
  GitBranch,
  Clock,
  Zap,
  Star,
  Server,
  Layers,
  UserMinus,
  Hourglass,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { motion } from "framer-motion";

function StatCard({
  label,
  value,
  icon: Icon,
  color = "text-primary",
  delay = 0,
}: {
  label: string;
  value: string | number;
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
        <CardContent className="p-5 flex flex-col justify-between h-full relative z-10">
          <div className="flex items-center justify-between">
            <Icon className={`w-4 h-4 ${color} opacity-60 group-hover:opacity-100 transition-opacity`} />
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
              {label}
            </span>
          </div>
          <div className="text-3xl font-bold tracking-tighter font-mono text-foreground/90 mt-4 group-hover:text-white transition-colors">
            {typeof value === "number" ? formatNumber(value) : value}
          </div>
        </CardContent>
        
      </Card>
    </motion.div>
  );
}

function RaftMetricsGrid({ data, stateColor }: { data: ShardStatus | ClusterStatusSingle, stateColor: string }) {
  return (
    <div className="grid grid-cols-3 gap-2 mt-4">
      <div className="glass-card p-2 rounded-lg flex flex-col justify-center items-center">
        <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">State</span>
        <span className={`text-xs font-bold uppercase mt-1 ${stateColor}`}>{data.raft_state}</span>
      </div>
      <div className="glass-card p-2 rounded-lg flex flex-col justify-center items-center">
        <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Term</span>
        <span className="text-xs font-mono font-bold text-foreground/80 mt-1">{data.current_term}</span>
      </div>
      <div className="glass-card p-2 rounded-lg flex flex-col justify-center items-center">
        <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Log Index</span>
        <span className="text-xs font-mono font-bold text-foreground/80 mt-1">{data.last_log_index}</span>
      </div>
      <div className="glass-card p-2 rounded-lg flex flex-col justify-center items-center">
        <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Applied</span>
        <span className="text-xs font-mono font-bold text-foreground/80 mt-1">{data.last_applied}</span>
      </div>
      <div className="glass-card p-2 rounded-lg flex flex-col justify-center items-center">
        <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Lag</span>
        <span className={`text-xs font-mono font-bold mt-1 ${data.replication_lag > 10 ? "text-warning" : "text-success"}`}>{data.replication_lag}</span>
      </div>
      <div className="glass-card p-2 rounded-lg flex flex-col justify-center items-center">
        <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Voters</span>
        <span className="text-xs font-mono font-bold text-foreground/80 mt-1">{data.voters?.length ?? 0}</span>
      </div>
    </div>
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
    <Card className="glass-card transition-colors">
      <CardHeader className="p-4 pb-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Layers className="w-4 h-4 text-primary opacity-60" />
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">S{shard.shard_id}</span>
          </div>
          <div className={`w-2 h-2 rounded-full ${isHealthy ? 'bg-success shadow-[0_0_8px_rgba(34,197,94,0.5)]' : 'bg-warning shadow-[0_0_8px_rgba(245,158,11,0.5)]'} animate-pulse`} />
        </div>
      </CardHeader>
      <CardContent className="p-4 pt-2">
        <RaftMetricsGrid data={shard} stateColor={stateColor} />
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
    <Card className="glass-card transition-colors">
      <CardHeader className="p-4 pb-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <GitBranch className="w-4 h-4 text-primary opacity-60" />
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Consensus</span>
          </div>
          <div className={`w-2 h-2 rounded-full ${cluster.replication_lag <= 10 ? 'bg-success shadow-[0_0_8px_rgba(34,197,94,0.5)]' : 'bg-warning shadow-[0_0_8px_rgba(245,158,11,0.5)]'} animate-pulse`} />
        </div>
      </CardHeader>
      <CardContent className="p-4 pt-2">
        <RaftMetricsGrid data={cluster} stateColor={stateColor} />
      </CardContent>
    </Card>
  );
}

function HeartbeatCard({ cluster, onRemoveNode }: { cluster: ClusterStatusSingle; onRemoveNode: (id: number) => Promise<void> }) {
  const config = cluster.config || {
    heartbeat_interval_ms: 500,
    election_timeout_min_ms: 1500,
    election_timeout_max_ms: 3000,
  };

  return (
    <Card className="glass-card transition-colors">
      <CardHeader className="p-4 pb-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-primary opacity-60" />
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Heartbeat</span>
          </div>
        </div>
      </CardHeader>
      <CardContent className="p-4 pt-4 space-y-4">
        <div className="grid grid-cols-2 gap-2">
          <div className="glass-card p-2 rounded-lg flex flex-col items-center justify-center">
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Interval</span>
            <span className="text-xs font-mono font-bold text-foreground/80 mt-1">{config.heartbeat_interval_ms}ms</span>
          </div>
          <div className="glass-card p-2 rounded-lg flex flex-col items-center justify-center">
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Timeout</span>
            <span className="text-xs font-mono font-bold text-foreground/80 mt-1">{config.election_timeout_min_ms}ms</span>
          </div>
        </div>

        <div className="space-y-1">
          {cluster.voters.map((nodeId) => {
            const isLeader = nodeId === cluster.current_leader;
            const isSelf = nodeId === cluster.node_id;
            const isHealthy = cluster.replication_lag === 0 || isLeader;

            return (
              <div key={nodeId} className="flex items-center justify-between py-1.5 px-2 rounded-md hover:bg-card transition-colors">
                <div className="flex items-center gap-2">
                  <div className={`w-1.5 h-1.5 rounded-full ${isHealthy ? 'bg-success shadow-[0_0_5px_rgba(34,197,94,0.5)]' : 'bg-warning shadow-[0_0_5px_rgba(245,158,11,0.5)]'} animate-pulse`} />
                  <span className="text-xs font-mono text-foreground/80">N{nodeId}</span>
                  {isSelf && <span className="ml-1 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">(Self)</span>}
                </div>
                <div className="flex items-center gap-2">
                  {isLeader && <Star className="w-3 h-3 text-success opacity-80" />}
                  {!isSelf && !isLeader && (
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-5 w-5 hover:bg-destructive/10 hover:text-destructive text-muted-foreground/30 transition-colors"
                      onClick={() => onRemoveNode(nodeId)}
                    >
                      <UserMinus className="w-3 h-3" />
                    </Button>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}

function PipelineCard({ stats }: { stats: NonNullable<ReturnType<typeof useStats>["data"]> }) {
  return (
    <Card className="glass-card transition-colors">
      <CardHeader className="p-4 pb-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Zap className="w-4 h-4 text-primary opacity-60" />
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Pipeline</span>
          </div>
        </div>
      </CardHeader>
      <CardContent className="p-4 pt-5 space-y-5">
        {[
          { label: "L0", count: stats.pending_events, color: "bg-warning", glow: "shadow-[0_0_10px_rgba(245,158,11,0.3)]" },
          { label: "L1", count: stats.memory_by_level.l1, color: "bg-primary", glow: "shadow-[0_0_10px_rgba(56,125,255,0.3)]" },
          { label: "L2", count: stats.memory_by_level.l2, color: "bg-success", glow: "shadow-[0_0_10px_rgba(34,197,94,0.3)]" },
        ].map((level) => (
          <div key={level.label} className="relative">
            <div className="flex justify-between items-end mb-1.5">
              <span className="font-mono text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{level.label}</span>
              <span className="text-xs font-mono text-foreground/80">{formatNumber(level.count)}</span>
            </div>
            <div className="h-1 bg-card rounded-full overflow-hidden border border-border">
              <div
                className={`h-full ${level.color} ${level.glow} rounded-full transition-all duration-1000`}
                style={{ width: `${Math.max(2, Math.min(100, (level.count / Math.max(stats.total_memory_units, 1)) * 100))}%` }}
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
    <Card className="glass-card">
      <CardHeader className="pb-3 border-b border-border">
        <CardTitle className="text-xs flex items-center gap-2">
          <div className="p-1.5 rounded-md bg-primary/10 border border-primary/20">
            <Server className="w-3.5 h-3.5 text-primary" />
          </div>
          <span className="uppercase tracking-widest text-muted-foreground/80 font-bold">Cluster Topology</span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {hasNodes ? (
          <div className="flex items-center justify-center gap-6 py-6">
            {cluster.voters.map((nodeId) => {
              const isLeader = nodeId === cluster.current_leader;
              const isSelf = nodeId === cluster.node_id;
              return (
                <div key={nodeId} className="text-center group">
                  <div
                    className={`relative w-16 h-16 rounded-2xl flex items-center justify-center border-2 transition-all duration-300 mx-auto ${
                      isLeader
                        ? "border-success bg-success/10 shadow-[0_0_20px_rgba(34,197,94,0.15)] group-hover:scale-105"
                        : "border-white/10 bg-black/40 group-hover:border-white/20"
                    }`}
                  >
                    <div className="text-center">
                      {isLeader && <Star className="w-3.5 h-3.5 text-success mx-auto mb-1 animate-pulse" />}
                      <span className={`text-sm font-mono ${isSelf ? "font-bold text-white" : "text-foreground/70"}`}>
                        N{nodeId}
                      </span>
                    </div>
                  </div>
                  <div className="mt-3 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
                    {isLeader ? <span className="text-success">Leader</span> : "Follower"}
                  </div>
                  <div className="font-mono mt-0.5 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
                    :{3000 + nodeId - 1}
                  </div>
                </div>
              );
            })}
            {cluster.learners.map((nodeId) => (
              <div key={nodeId} className="text-center">
                <div className="w-16 h-16 rounded-2xl flex items-center justify-center border-2 border-dashed border-border bg-card mx-auto">
                  <span className="text-sm font-mono text-muted-foreground/50">N{nodeId}</span>
                </div>
                <div className="mt-3 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Learner</div>
              </div>
            ))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-10 text-center">
            <div className="relative w-16 h-16 rounded-2xl flex items-center justify-center border-2 border-success bg-success/10 mb-4">
              <div className="text-center">
                <Star className="w-4 h-4 text-success mx-auto mb-1 animate-pulse" />
                <span className="text-sm font-mono font-bold text-white">N{cluster.node_id}</span>
              </div>
            </div>
            <div className="font-medium text-[11px] uppercase tracking-widest text-muted-foreground">
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
    <Card className="glass-card">
      <CardHeader className="pb-3 border-b border-border">
        <CardTitle className="text-xs flex items-center gap-2">
          <div className="p-1.5 rounded-md bg-primary/10 border border-primary/20">
            <Server className="w-3.5 h-3.5 text-primary" />
          </div>
          <span className="uppercase tracking-widest text-muted-foreground/80 font-bold">
            Sharded Cluster &middot; Node {cluster.physical_node_id} &middot; {cluster.shard_count} Shards
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-center gap-5 py-6 flex-wrap">
          {cluster.shards.map((shard) => {
            const isLeader = shard.raft_state === "Leader";
            return (
              <div key={shard.shard_id} className="text-center group">
                <div
                  className={`w-16 h-16 rounded-2xl flex items-center justify-center border-2 transition-all duration-300 mx-auto ${
                    isLeader
                      ? "border-success bg-success/10 shadow-[0_0_20px_rgba(34,197,94,0.15)] group-hover:scale-105"
                      : "border-white/10 bg-black/40 group-hover:border-white/20"
                  }`}
                >
                  <div className="text-center">
                    {isLeader && <Star className="w-3.5 h-3.5 text-success mx-auto mb-1 animate-pulse" />}
                    <span className="text-sm font-mono font-bold text-white">S{shard.shard_id}</span>
                  </div>
                </div>
                <div className={`text-[10px] uppercase tracking-wider font-bold mt-3 ${isLeader ? 'text-success' : 'text-muted-foreground/60'}`}>
                  {shard.raft_state}
                </div>
                <div className="font-mono mt-0.5 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
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
  const { orgId } = useOrgScope();
  const scopedOrgId = orgId.trim();
  const { data: cluster, isLoading: clusterLoading, mutate: mutateCluster } = useClusterStatus();
  const { data: stats, isLoading: statsLoading } = useStats(undefined, scopedOrgId || undefined);
  const { data: pendingData } = usePendingCount();

  async function handleRemoveNode(nodeId: number) {
    try {
      await api.leaveCluster(nodeId);
      mutateCluster();
    } catch {
      // ignore — user will see no change
    }
  }

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
          {scopedOrgId && (
            <p className="mt-2 font-mono text-xs text-muted-foreground">
              Memory counters scoped to org: {scopedOrgId}
            </p>
          )}
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
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3">
        <StatCard
          label="Total Events"
          value={stats?.total_events ?? 0}
          icon={Activity}
          delay={0.1}
        />
        <StatCard
          label="Memory Units"
          value={stats?.total_memory_units ?? 0}
          icon={Database}
          color="text-success"
          delay={0.15}
        />
        <StatCard
          label="Graph Edges"
          value={stats?.total_edges ?? 0}
          icon={GitBranch}
          color="text-warning"
          delay={0.2}
        />
        <StatCard
          label={sharded ? "Shards" : "Nodes"}
          value={sharded ? (cluster as ClusterStatusSharded).shard_count : (cluster as ClusterStatusSingle)?.voters.length ?? 0}
          icon={Server}
          delay={0.25}
        />
        <StatCard
          label="Pending Q"
          value={pendingData?.pending ?? "—"}
          icon={Hourglass}
          color="text-warning"
          delay={0.3}
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
          {cluster && !sharded && (
            <HeartbeatCard
              cluster={cluster as ClusterStatusSingle}
              onRemoveNode={handleRemoveNode}
            />
          )}
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
