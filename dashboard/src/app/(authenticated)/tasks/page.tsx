"use client";

import { useState } from "react";
import { useUserFilter } from "../layout";
import { useTaskTree, useReadyTasks } from "@/lib/hooks";
import { api } from "@/lib/api";
import { TaskTreeViewer } from "@/components/TaskTreeViewer";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  CheckSquare,
  RefreshCw,
  Clock,
  CheckCircle2,
  XCircle,
  AlertCircle,
  Loader2,
  GitBranch,
  Zap,
} from "lucide-react";
import type { ReadyTask } from "@/lib/types";
import { motion } from "framer-motion";

function statusLabel(status: ReadyTask["status"]): string {
  if (status === "Pending") return "Pending";
  if (status === "InProgress") return "In Progress";
  if (status === "Completed") return "Completed";
  if (status === "Cancelled") return "Cancelled";
  if (typeof status === "object" && "Blocked" in status) return `Blocked: ${status.Blocked}`;
  if (typeof status === "object" && "Failed" in status) return `Failed: ${status.Failed}`;
  return String(status);
}

function StatusBadge({ status }: { status: ReadyTask["status"] }) {
  const label = statusLabel(status);
  if (status === "Completed") return <Badge variant="outline" className="text-[10px] text-success border-success/30 bg-success/5">{label}</Badge>;
  if (status === "InProgress") return <Badge variant="outline" className="text-[10px] text-primary border-primary/30 bg-primary/5">{label}</Badge>;
  if (status === "Cancelled") return <Badge variant="outline" className="text-[10px] text-muted-foreground">{label}</Badge>;
  if (typeof status === "object" && "Blocked" in status) return <Badge variant="outline" className="text-[10px] text-warning border-warning/30 bg-warning/5">{label}</Badge>;
  if (typeof status === "object" && "Failed" in status) return <Badge variant="outline" className="text-[10px] text-destructive border-destructive/30 bg-destructive/5">{label}</Badge>;
  return <Badge variant="outline" className="text-[10px]">{label}</Badge>;
}

function ReadyTaskRow({
  task,
  userId,
  onUpdated,
  index,
}: {
  task: ReadyTask;
  userId: string;
  onUpdated: () => void;
  index: number;
}) {
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState("");

  async function markStatus(newStatus: string) {
    setBusy(true);
    try {
      await api.updateTaskStatus(userId, task.task_id, {
        status: newStatus,
        ...(result ? { result_summary: result } : {}),
      });
      onUpdated();
    } catch {
      // swallow
    } finally {
      setBusy(false);
    }
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05, duration: 0.25 }}
      className="group glass-card rounded-xl p-4 border border-white/[0.06] hover:border-primary/20 hover:bg-white/[0.03] transition-all duration-200"
    >
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium leading-tight">{task.title}</p>
          {task.description && task.description !== task.title && (
            <p className="text-xs text-muted-foreground mt-1 line-clamp-2 leading-relaxed">{task.description}</p>
          )}
        </div>
        <StatusBadge status={task.status} />
      </div>

      <div className="flex items-center gap-3 text-[10px] text-muted-foreground/50 mb-3">
        <span className="flex items-center gap-1">
          <Clock className="w-3 h-3" />
          {new Date(task.created_at).toLocaleString()}
        </span>
        {task.agent_id && (
          <>
            <span>·</span>
            <span className="font-mono bg-white/5 px-1.5 py-0.5 rounded border border-white/5">{task.agent_id}</span>
          </>
        )}
      </div>

      <div className="flex items-center gap-2">
        <Input
          value={result}
          onChange={(e) => setResult(e.target.value)}
          placeholder="Result summary (optional)"
          className="h-7 text-xs flex-1 bg-white/5 border-white/10 focus:border-primary/40 placeholder:text-muted-foreground/30"
        />
        <Button
          size="sm"
          variant="outline"
          disabled={busy}
          className="h-7 px-2.5 text-xs gap-1.5 border-success/20 text-success hover:bg-success/10 hover:text-success hover:border-success/40"
          onClick={() => markStatus("Completed")}
        >
          {busy ? <Loader2 className="w-3 h-3 animate-spin" /> : <CheckCircle2 className="w-3 h-3" />}
          Done
        </Button>
        <Button
          size="sm"
          variant="outline"
          disabled={busy}
          className="h-7 px-2.5 text-xs gap-1.5 border-white/10 text-muted-foreground hover:text-destructive hover:bg-destructive/10 hover:border-destructive/20"
          onClick={() => markStatus("Cancelled")}
        >
          <XCircle className="w-3 h-3" />
          Cancel
        </Button>
      </div>
    </motion.div>
  );
}

export default function TasksPage() {
  const { userId } = useUserFilter();
  const { data: trees, isLoading: treeLoading, mutate: mutateTree } = useTaskTree(userId || undefined);
  const { data: ready, isLoading: readyLoading, mutate: mutateReady } = useReadyTasks(userId || undefined);

  function handleUpdated() {
    mutateReady();
    mutateTree();
  }

  return (
    <div className="space-y-6 relative">
      <div className="absolute top-0 right-0 w-[500px] h-[300px] blob-bg opacity-20 pointer-events-none -z-10 mix-blend-screen" />

      <motion.div
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4 }}
        className="flex items-center justify-between"
      >
        <div>
          <div className="flex items-center gap-2.5">
            <CheckSquare className="w-6 h-6 text-primary" />
            <h1 className="text-3xl font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-b from-white to-white/60">
              Tasks
            </h1>
          </div>
          <p className="text-muted-foreground mt-1 text-sm">
            {userId ? `Viewing tasks for user: ${userId}` : "Set a User ID in the sidebar to filter tasks"}
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          className="gap-1.5 text-xs border-white/10 hover:bg-white/5"
          onClick={() => { mutateTree(); mutateReady(); }}
        >
          <RefreshCw className="w-3.5 h-3.5" />
          Refresh
        </Button>
      </motion.div>

      {!userId ? (
        <motion.div
          initial={{ opacity: 0, scale: 0.97 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.4, delay: 0.1 }}
          className="glass-card rounded-xl border border-dashed border-white/10 py-16 flex flex-col items-center text-center gap-4"
        >
          <div className="w-14 h-14 rounded-2xl bg-white/[0.03] border border-white/5 flex items-center justify-center">
            <AlertCircle className="w-7 h-7 opacity-20" />
          </div>
          <div>
            <p className="text-sm font-medium text-foreground/60">No user selected</p>
            <p className="text-xs text-muted-foreground/50 mt-1">Enter a User ID in the sidebar filter to load tasks</p>
          </div>
        </motion.div>
      ) : (
        <Tabs defaultValue="tree">
          <TabsList className="bg-white/[0.04] border border-white/[0.06]">
            <TabsTrigger value="tree" className="gap-1.5 text-xs">
              <GitBranch className="w-3.5 h-3.5" />
              Task Tree
            </TabsTrigger>
            <TabsTrigger value="ready" className="gap-1.5 text-xs">
              <Zap className="w-3.5 h-3.5" />
              Ready
              {ready && ready.length > 0 && (
                <Badge className="ml-1 h-4 px-1.5 text-[10px] bg-primary/80">{ready.length}</Badge>
              )}
            </TabsTrigger>
          </TabsList>

          <TabsContent value="tree" className="mt-4">
            {treeLoading ? (
              <div className="space-y-3">
                {[1, 2].map((i) => <Skeleton key={i} className="h-40 glass-card rounded-xl opacity-20" />)}
              </div>
            ) : (
              <TaskTreeViewer trees={trees ?? []} />
            )}
          </TabsContent>

          <TabsContent value="ready" className="mt-4">
            <Card className="glass-card border-white/[0.06]">
              <CardHeader className="pb-3 border-b border-white/5">
                <CardTitle className="text-xs flex items-center gap-2">
                  <div className="p-1.5 rounded-md bg-primary/10 border border-primary/10">
                    <CheckCircle2 className="w-3 h-3 text-primary" />
                  </div>
                  <span className="uppercase tracking-widest text-muted-foreground/70 font-bold">Ready to Execute</span>
                </CardTitle>
              </CardHeader>
              <CardContent className="pt-4">
                {readyLoading ? (
                  <div className="space-y-2">
                    {[1, 2].map((i) => <Skeleton key={i} className="h-24 glass-card rounded-xl opacity-20" />)}
                  </div>
                ) : !ready || ready.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-10 text-center">
                    <div className="w-12 h-12 rounded-2xl bg-white/[0.03] border border-white/5 flex items-center justify-center mb-3">
                      <CheckCircle2 className="w-6 h-6 opacity-20" />
                    </div>
                    <p className="text-sm text-muted-foreground/60 font-medium">No ready tasks</p>
                    <p className="text-xs text-muted-foreground/40 mt-1">Tasks with no pending dependencies will appear here</p>
                  </div>
                ) : (
                  <div className="space-y-2">
                    {ready.map((task, i) => (
                      <ReadyTaskRow
                        key={task.task_id}
                        task={task}
                        userId={userId}
                        onUpdated={handleUpdated}
                        index={i}
                      />
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
}
