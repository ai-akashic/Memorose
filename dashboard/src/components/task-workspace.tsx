"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { useTranslations } from "next-intl";
import { CheckCircle2, Clock, GitBranch, Loader2, XCircle, Zap } from "lucide-react";
import { useReadyTasks, useTaskTree } from "@/lib/hooks";
import { api } from "@/lib/api";
import type { ReadyTask } from "@/lib/types";
import { TaskTreeViewer } from "@/components/TaskTreeViewer";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

function statusBadgeClass(status: ReadyTask["status"]): string {
  if (status === "Completed") return "border-success/30 bg-success/5 text-success";
  if (status === "InProgress") return "border-primary/30 bg-primary/5 text-primary";
  if (status === "Cancelled") return "border-border bg-background/60 text-muted-foreground";
  if (typeof status === "object" && "Blocked" in status) {
    return "border-warning/30 bg-warning/5 text-warning";
  }
  if (typeof status === "object" && "Failed" in status) {
    return "border-destructive/30 bg-destructive/5 text-destructive";
  }
  return "border-border bg-background/60 text-muted-foreground";
}

function StatusBadge({ status }: { status: ReadyTask["status"] }) {
  const t = useTranslations("Memories.tasks");
  let label: string;

  if (status === "Pending") label = t("status.Pending");
  else if (status === "InProgress") label = t("status.InProgress");
  else if (status === "Completed") label = t("status.Completed");
  else if (status === "Cancelled") label = t("status.Cancelled");
  else if (typeof status === "object" && "Blocked" in status) label = t("status.Blocked", { reason: status.Blocked });
  else if (typeof status === "object" && "Failed" in status) label = t("status.Failed", { reason: status.Failed });
  else label = String(status);

  return (
    <Badge
      variant="outline"
      className={`min-h-6 rounded-full px-2.5 text-[11px] font-medium tracking-wide ${statusBadgeClass(status)}`}
    >
      {label}
    </Badge>
  );
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
  const t = useTranslations("Memories.tasks");
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
      // keep the current optimistic-free flow silent for now
    } finally {
      setBusy(false);
    }
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05, duration: 0.25 }}
      className="group glass-card rounded-2xl border border-border/70 px-4 py-4 transition-all duration-200 hover:border-primary/20 hover:bg-card/80"
    >
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0 flex-1 space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <p className="text-sm font-semibold leading-tight text-foreground">{task.title}</p>
            {task.progress > 0 && task.progress < 1 && (
              <span className="rounded-full border border-primary/20 bg-primary/5 px-2 py-0.5 text-[10px] font-medium uppercase tracking-[0.18em] text-primary">
                {(task.progress * 100).toFixed(0)}% progress
              </span>
            )}
          </div>
          {task.description && task.description !== task.title && (
            <p className="line-clamp-2 max-w-3xl text-sm leading-relaxed text-muted-foreground">
              {task.description}
            </p>
          )}
        </div>

        <div className="flex items-start justify-between gap-3 lg:min-w-[180px] lg:flex-col lg:items-end">
          <StatusBadge status={task.status} />
          <div className="text-right text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            {t("ready.label")}
          </div>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-center gap-x-3 gap-y-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
        <span className="inline-flex items-center gap-1.5">
          <Clock className="h-3.5 w-3.5" />
          {new Date(task.created_at).toLocaleString()}
        </span>
        {task.agent_id && (
          <span className="rounded-full border border-border/80 bg-background/70 px-2.5 py-1 font-mono text-[10px] tracking-[0.16em] text-foreground/75">
            {task.agent_id}
          </span>
        )}
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-center">
        <Input
          value={result}
          onChange={(e) => setResult(e.target.value)}
          placeholder={t("ready.resultPlaceholder")}
          className="h-10 bg-background/70 border-border/80 focus:border-primary/40 text-sm text-foreground"
        />
        <div className="flex flex-wrap gap-2 lg:justify-end">
          <Button
            size="sm"
            variant="outline"
            disabled={busy}
            className="h-10 min-w-[112px] gap-2 border-success/20 bg-success/5 px-3 text-xs font-medium uppercase tracking-[0.16em] text-success hover:border-success/40 hover:bg-success/10 hover:text-success"
            onClick={() => markStatus("Completed")}
          >
            {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <CheckCircle2 className="h-3.5 w-3.5" />}
            {t("ready.complete")}
          </Button>
          <Button
            size="sm"
            variant="outline"
            disabled={busy}
            className="h-10 min-w-[112px] gap-2 border-border bg-background/70 px-3 text-xs font-medium uppercase tracking-[0.16em] text-muted-foreground hover:border-destructive/20 hover:bg-destructive/10 hover:text-destructive"
            onClick={() => markStatus("Cancelled")}
          >
            <XCircle className="h-3.5 w-3.5" />
            {t("ready.cancel")}
          </Button>
        </div>
      </div>
    </motion.div>
  );
}

export function TaskWorkspace({
  userId,
  defaultTab = "tree",
}: {
  userId?: string;
  defaultTab?: "tree" | "ready";
}) {
  const tTasks = useTranslations("Memories.tasks");
  const tMemories = useTranslations("Memories");
  const { data: trees, isLoading: treeLoading, mutate: mutateTree } = useTaskTree(userId);
  const { data: ready, isLoading: readyLoading, mutate: mutateReady } = useReadyTasks(userId);

  function handleUpdated() {
    mutateReady();
    mutateTree();
  }

  if (!userId || userId === "all") {
    return (
      <div className="flex flex-col items-center justify-center p-8 text-center text-muted-foreground border rounded-lg bg-muted/5 border-dashed mt-4">
        <p>{tMemories("taskScope")}</p>
      </div>
    );
  }

  return (
    <Tabs defaultValue={defaultTab} className="space-y-4">
      <TabsList className="h-auto rounded-2xl border border-border/70 bg-background/70 p-1">
        <TabsTrigger value="tree" className="gap-1.5 rounded-xl px-4 py-2 text-xs font-medium uppercase tracking-[0.16em]">
          <GitBranch className="w-3.5 h-3.5" />
          {tTasks("tabs.tree")}
        </TabsTrigger>
        <TabsTrigger value="ready" className="gap-1.5 rounded-xl px-4 py-2 text-xs font-medium uppercase tracking-[0.16em]">
          <Zap className="w-3.5 h-3.5" />
          {tTasks("tabs.ready")}
          {ready && ready.length > 0 && (
            <Badge className="ml-1 h-5 rounded-full bg-primary/80 px-1.5 text-[11px] text-primary-foreground">
              {ready.length}
            </Badge>
          )}
        </TabsTrigger>
      </TabsList>

      <TabsContent value="tree" className="mt-4">
        {treeLoading ? (
          <div className="space-y-3">
            {[1, 2].map((i) => (
              <Skeleton key={i} className="h-40 glass-card rounded-xl opacity-20" />
            ))}
          </div>
        ) : (
          <TaskTreeViewer trees={trees ?? []} />
        )}
      </TabsContent>

      <TabsContent value="ready" className="mt-4">
        <Card className="glass-card border border-border/70">
          <CardHeader className="border-b border-border/70 pb-4">
            <CardTitle className="flex items-center gap-2 text-xs">
              <div className="rounded-lg border border-primary/10 bg-primary/10 p-1.5">
                <CheckCircle2 className="h-3 w-3 text-primary" />
              </div>
              <span className="font-bold uppercase tracking-[0.18em] text-muted-foreground/70">
                {tTasks("ready.sectionTitle")}
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-5">
            {readyLoading ? (
              <div className="space-y-3">
                {[1, 2].map((i) => (
                  <Skeleton key={i} className="h-32 glass-card rounded-2xl opacity-20" />
                ))}
              </div>
            ) : !ready || ready.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12 text-center">
                <div className="mb-3 flex h-12 w-12 items-center justify-center rounded-2xl border border-border bg-background/70">
                  <CheckCircle2 className="h-6 w-6 opacity-20" />
                </div>
                <p className="text-sm font-medium text-muted-foreground/60">{tTasks("ready.empty")}</p>
                <p className="mt-1 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                  {tTasks("ready.emptyDescription")}
                </p>
              </div>
            ) : (
              <div className="space-y-3">
                {ready.map((task, index) => (
                  <ReadyTaskRow
                    key={task.task_id}
                    task={task}
                    userId={userId}
                    onUpdated={handleUpdated}
                    index={index}
                  />
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </TabsContent>
    </Tabs>
  );
}
