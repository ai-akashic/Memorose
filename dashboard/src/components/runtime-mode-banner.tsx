"use client";

import type { ClusterStatus } from "@/lib/types";
import { Card, CardContent } from "@/components/ui/card";

export function RuntimeModeBanner({
  cluster,
  t,
}: {
  cluster: ClusterStatus;
  t: (key: string) => string;
}) {
  const standalone = cluster.runtime_mode === "standalone";

  return (
    <Card className="glass-card overflow-hidden border-white/[0.04]">
      <CardContent className="flex flex-col gap-3 p-4 md:flex-row md:items-center md:justify-between">
        <div className="space-y-1">
          <div className="label-xs">
            {t("runtime.title")}
          </div>
          <div className="flex items-center gap-2">
            <span
              className={`rounded-full px-2 py-1 text-[10px] font-bold uppercase tracking-wider ${
                standalone ? "bg-success/10 text-success" : "bg-primary/10 text-primary"
              }`}
            >
              {standalone ? t("runtime.standalone") : t("runtime.cluster")}
            </span>
            <span className="rounded-full bg-card px-2 py-1 text-[10px] font-mono uppercase tracking-wider text-foreground/75">
              {cluster.write_path}
            </span>
          </div>
        </div>
        <div className="max-w-xl text-sm text-muted-foreground">
          {standalone ? t("runtime.standaloneDesc") : t("runtime.clusterDesc")}
        </div>
      </CardContent>
    </Card>
  );
}
