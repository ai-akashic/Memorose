"use client";

import { useState, useCallback, useEffect, useMemo } from "react";
import dynamic from "next/dynamic";
import { useAgents, useGraph, useMemories, useStoredString, useTaskTree } from "@/lib/hooks";
import { api } from "@/lib/api";
import { useOrgScope } from "@/lib/org-scope";
import { truncate } from "@/lib/utils";
import type { MemoryUnit, SearchResult } from "@/lib/types";
import { TaskTreeViewer } from "@/components/TaskTreeViewer";
import {
  Search,
  ChevronLeft,
  ChevronRight,
  Loader2,
  Network,
  List,
  CheckSquare,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { useTranslations } from "next-intl";

function LevelBadge({ level }: { level: number }) {
  const colors: Record<number, string> = {
    0: "bg-muted text-muted-foreground border-border",
    1: "bg-primary/15 text-primary border-primary/20 hover:bg-primary/20",
    2: "bg-success/15 text-success border-success/20 hover:bg-success/20",
    3: "bg-amber-500/15 text-amber-500 border-amber-500/20 hover:bg-amber-500/20",
  };
  return (
    <Badge variant="outline" className={`text-[11px] px-1.5 py-0 font-mono ${colors[level] || "bg-muted text-muted-foreground"}`}>
      L{level}
    </Badge>
  );
}

function ImportanceBar({ value }: { value: number }) {
  return (
    <div className="w-16 h-1 bg-card rounded-full overflow-hidden border border-border">
      <div
        className="h-full bg-primary transition-all duration-700"
        style={{ width: `${value * 100}%` }}
      />
    </div>
  );
}

function MemoryDetailSheet({
  memory,
  open,
  onOpenChange,
}: {
  memory: MemoryUnit | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const t = useTranslations("Memories");
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-md glass-card border-l">
        <SheetHeader className="pb-6">
          <SheetTitle className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("detail.title")}</SheetTitle>
        </SheetHeader>

        {memory && (
          <div className="space-y-6 px-2">
            <div className="flex flex-col gap-1.5">
              <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("detail.identifier")}</span>
              <p className="font-mono text-[11px] text-foreground/70 break-all">{memory.id}</p>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="flex flex-col gap-1.5">
                <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("detail.user")}</span>
                <p className="font-mono text-[11px] text-foreground/70">{memory.user_id}</p>
              </div>
              <div className="flex flex-col gap-1.5">
                <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("detail.application")}</span>
                <p className="font-mono text-[11px] text-foreground/70">{memory.app_id}</p>
              </div>
            </div>

            <div className="flex flex-col gap-2">
              <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("detail.payload")}</span>
              <div className="bg-card border border-border rounded-xl p-4">
                <p className="text-xs text-foreground/90 leading-relaxed whitespace-pre-wrap">{memory.content}</p>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-6 py-2">
              <div className="flex flex-col gap-2">
                <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("detail.hierarchy")}</span>
                <LevelBadge level={memory.level} />
              </div>
              <div className="flex flex-col gap-2">
                <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("detail.significance")}</span>
                <ImportanceBar value={memory.importance} />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 opacity-60">
              <div className="flex flex-col gap-1">
                <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("detail.telemetry")}</span>
                <p className="text-[11px] font-mono text-muted-foreground">ACC: {memory.access_count} · STRM: {memory.stream_id}</p>
              </div>
            </div>

            {memory.keywords.length > 0 && (
              <div className="flex flex-col gap-2">
                <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("detail.keywords")}</span>
                <div className="flex flex-wrap gap-1.5">
                  {memory.keywords.map((kw) => (
                    <span key={kw} className="bg-card px-2 py-0.5 rounded-full border border-border text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{kw}</span>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
}

const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-[400px] text-muted-foreground">
      <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading graph...
    </div>
  ),
});

function KnowledgeGraph({ userId, orgId }: { userId?: string; orgId?: string }) {
  const t = useTranslations("Memories");
  const { data, isLoading } = useGraph(200, userId, orgId);

  const graphData = useMemo(() => {
    if (!data) return { nodes: [], links: [] };
    return {
      nodes: data.nodes.map((n) => ({
        id: n.id,
        label: n.label,
        level: n.level,
        importance: n.importance,
      })),
      links: data.edges.map((e) => ({
        source: e.source,
        target: e.target,
        relation: e.relation,
        weight: e.weight,
      })),
    };
  }, [data]);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const nodeColor = useCallback((node: any) => {
    return node.level === 2 ? "hsl(142, 76%, 36%)" : "hsl(217, 91%, 60%)";
  }, []);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const nodeVal = useCallback((node: any) => {
    return 2 + (node.importance ?? 0.5) * 4;
  }, []);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const linkCanvasObject = useCallback((link: any, ctx: CanvasRenderingContext2D, scale: number) => {
    if (!link.source || !link.target) return;
    const start = link.source;
    const end = link.target;

    ctx.beginPath();
    ctx.moveTo(start.x, start.y);
    ctx.lineTo(end.x, end.y);

    const isLevel2 = start.level === 2 || end.level === 2;
    const color = isLevel2 ? 'rgba(34, 197, 94, 0.4)' : 'rgba(56, 125, 255, 0.4)';
    const glowColor = isLevel2 ? 'rgba(34, 197, 94, 0.8)' : 'rgba(56, 125, 255, 0.8)';

    ctx.strokeStyle = color;
    ctx.lineWidth = ((link.weight ?? 0.5) * 2) / scale;
    ctx.shadowColor = glowColor;
    ctx.shadowBlur = 8 / scale;
    ctx.stroke();
    ctx.shadowBlur = 0; // reset
  }, []);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const nodeCanvasObject = useCallback((node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
    const label = node.label || "";
    const fontSize = 12 / globalScale;
    ctx.font = `${fontSize}px Sans-Serif`;
    const r = Math.sqrt(Math.max(0, nodeVal(node)));

    // Add breathing glow effect using Date.now()
    const t = Date.now() / 1000;
    const pulse = Math.sin(t * 2 + node.id.charCodeAt(0)) * 0.5 + 0.5;

    const color = nodeColor(node);

    ctx.beginPath();
    ctx.arc(node.x, node.y, r, 0, 2 * Math.PI, false);
    ctx.fillStyle = color;
    ctx.shadowColor = color;
    ctx.shadowBlur = (r * 2 + pulse * 4) / globalScale;
    ctx.fill();
    ctx.shadowBlur = 0; // reset

    // Node label
    if (globalScale > 1.5) {
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillStyle = 'rgba(255, 255, 255, 0.8)';
      ctx.fillText(label, node.x, node.y + r + fontSize);
    }
  }, [nodeColor, nodeVal]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-[500px] text-muted-foreground glass-card rounded-xl">
        <Loader2 className="w-5 h-5 animate-spin mr-2" /> {t("graph.loading")}
      </div>
    );
  }

  if (!data || data.nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-[500px] text-muted-foreground glass-card rounded-xl">
        <Network className="w-5 h-5 mr-2 opacity-50" /> {t("graph.empty")}
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full relative group">
      <div className="absolute top-4 left-4 z-10 flex gap-3 text-[11px] font-mono uppercase tracking-wider text-muted-foreground bg-background/40 px-3 py-1.5 rounded-full border border-border">
        <span className="flex items-center gap-1.5"><div className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse" /> {data.stats.node_count} {t("graph.nodes")}</span>
        <span className="flex items-center gap-1.5"><div className="w-1.5 h-1.5 rounded-full bg-primary/50" /> {data.stats.edge_count} {t("graph.edges")}</span>
      </div>
      <div className="flex-1 min-h-[500px] rounded-xl glass-card overflow-hidden">
        <ForceGraph2D
          graphData={graphData}
          nodeVal={nodeVal}
          linkCanvasObject={linkCanvasObject}
          nodeCanvasObject={nodeCanvasObject}
          backgroundColor="transparent"
          d3VelocityDecay={0.3}
          d3AlphaDecay={0.02}
        />
      </div>
    </div>
  );
}

function SearchPlayground({
  globalUserId,
  orgId,
}: {
  globalUserId?: string;
  orgId?: string;
}) {
  const t = useTranslations("Memories");
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState("hybrid");
  const [searchUserId, setSearchUserId] = useState(globalUserId || "");
  const [appId, setAppId] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [queryTime, setQueryTime] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);

  useEffect(() => {
    setSearchUserId(globalUserId || "");
  }, [globalUserId]);

  async function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    if (!query.trim()) return;
    if (!searchUserId.trim()) {
      setSearchError(t("search.noUserId"));
      setResults([]);
      setQueryTime(null);
      return;
    }

    setLoading(true);
    setSearchError(null);
    try {
      const res = await api.search({
        query,
        mode,
        limit: 10,
        user_id: searchUserId.trim(),
        app_id: appId || undefined,
        org_id: orgId || undefined,
      });
      setResults(res.results);
      setQueryTime(res.query_time_ms);
    } catch (error) {
      setResults([]);
      setQueryTime(null);
      setSearchError(error instanceof Error ? error.message : "Search failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <form onSubmit={handleSearch} className="space-y-2.5 mb-5">
        <div className="flex gap-2">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground/50 pointer-events-none" />
            <Input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="pl-9 bg-card border-border focus:border-primary/40"
              placeholder={t("search.placeholder")}
            />
          </div>
          <Select value={mode} onValueChange={setMode}>
            <SelectTrigger className="w-[120px] bg-card border-border">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="hybrid">{t("search.hybrid")}</SelectItem>
              <SelectItem value="text">{t("search.text")}</SelectItem>
              <SelectItem value="vector">{t("search.vector")}</SelectItem>
            </SelectContent>
          </Select>
          <Button type="submit" disabled={loading} size="icon" className="shrink-0">
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
          </Button>
        </div>
        <div className="flex gap-2">
          <Input
            type="text"
            value={searchUserId}
            onChange={(e) => setSearchUserId(e.target.value)}
            placeholder={t("search.userIdPlaceholder")}
            className="flex-1 h-8 font-mono bg-card border-border focus:border-primary/40 text-[11px] font-medium uppercase tracking-widest text-muted-foreground"
          />
          <Input
            type="text"
            value={appId}
            onChange={(e) => setAppId(e.target.value)}
            placeholder={t("search.appIdPlaceholder")}
            className="flex-1 h-8 font-mono bg-card border-border focus:border-primary/40 text-[11px] font-medium uppercase tracking-widest text-muted-foreground"
          />
        </div>
        {searchError && (
          <p className="text-[11px] font-medium uppercase tracking-widest text-destructive">
            {searchError}
          </p>
        )}
      </form>

      {queryTime !== null && (
        <p className="font-semibold mb-3 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
          {results.length} results &middot; {queryTime.toFixed(1)}ms
        </p>
      )}

      <div className="space-y-2 max-h-[380px] overflow-y-auto pr-1">
        {results.map((r) => (
          <div key={r.unit.id} className="p-3 rounded-xl glass-card hover:border-primary/20 transition-all duration-200">
            <div className="flex items-center gap-2 mb-1.5">
              <LevelBadge level={r.unit.level} />
              {r.unit.memory_type === "procedural" ? (
                <Badge variant="outline" className="text-[11px] h-5 px-1.5 text-accent border-accent/30 bg-accent/5">{t("types.procedural")}</Badge>
              ) : (
                <Badge variant="outline" className="text-[11px] h-5 px-1.5 text-primary border-primary/30 bg-primary/5">{t("types.factual")}</Badge>
              )}
              <span className="ml-auto font-mono text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
                {(r.score * 100).toFixed(1)}%
              </span>
            </div>
            <p className="text-sm leading-relaxed">{truncate(r.unit.content, 200)}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function MemoryContent({ content }: { content: string }) {
  const t = useTranslations("Memories");
  const [expanded, setExpanded] = useState(false);
  const [copied, setCopied] = useState(false);

  const handleCopy = (e: React.MouseEvent) => {
    e.stopPropagation();
    navigator.clipboard.writeText(content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleToggle = (e: React.MouseEvent) => {
    e.stopPropagation();
    setExpanded(!expanded);
  };

  return (
    <div className="flex items-start w-full gap-4">
      <div className={`text-[13px] leading-snug max-w-[600px] ${expanded ? 'whitespace-pre-wrap' : 'line-clamp-2'}`}>
        {content}
      </div>
      <div className="ml-auto flex gap-2 items-center flex-shrink-0">
        <button
          onClick={handleToggle}
          className="text-[11px] text-primary hover:text-primary/80 hover:underline whitespace-nowrap bg-transparent border-none cursor-pointer p-0 font-normal"
        >
          {expanded ? t("actions.collapse") : t("actions.view")}
        </button>
        <span className="text-muted-foreground/30">·</span>
        <button
          onClick={handleCopy}
          className="text-[11px] text-primary hover:text-primary/80 hover:underline whitespace-nowrap bg-transparent border-none cursor-pointer p-0 font-normal"
        >
          {copied ? t("actions.copied") : t("actions.copy")}
        </button>
      </div>
    </div>
  );
}

function MemoryListTab({ userId, orgId }: { userId?: string; orgId?: string }) {
  const t = useTranslations("Memories");
  const [levelFilter, setLevelFilter] = useState<string>("all");
  const [agentId, setAgentId] = useState<string>("all");
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState("importance");
  const [selectedMemory, setSelectedMemory] = useState<MemoryUnit | null>(null);

  const { data: agentsData } = useAgents();
  const parsedLevel = levelFilter === "all" ? undefined : Number(levelFilter);
  const parsedAgentId = agentId === "all" ? undefined : agentId;

  const { data: memories, isLoading } = useMemories({
    level: parsedLevel,
    page,
    limit: 20,
    sort,
    org_id: orgId,
    user_id: userId,
    agent_id: parsedAgentId,
  });

  const handleViewDetail = useCallback(async (id: string) => {
    try {
      const unit = await api.memory(id);
      setSelectedMemory(unit);
    } catch {
      // ignore
    }
  }, []);

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-4 bg-card p-2 rounded-xl border border-border">
        <ToggleGroup
          type="single"
          value={levelFilter}
          onValueChange={(v) => { setLevelFilter(v || "all"); setPage(1); }}
          className="bg-card p-0.5 rounded-lg border border-border"
        >
          {["all", "0", "1", "2", "3"].map(v => (
            <ToggleGroupItem key={v} value={v} className="h-7 px-3 data-[state=on]:bg-white/10 data-[state=on]:text-white transition-all text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
              {v === "all" ? t("filters.allLevels") : `L${v}`}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>

        <div className="h-4 w-px bg-card mx-1" />

        <Select value={agentId} onValueChange={(v) => { setAgentId(v); setPage(1); }}>
          <SelectTrigger className="w-[140px] h-8 bg-transparent border-border hover:bg-card transition-all text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
            <SelectValue placeholder="AGENT" />
          </SelectTrigger>
          <SelectContent className="glass-card">
            <SelectItem value="all" className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("filters.allAgents")}</SelectItem>
            {agentsData?.agents.map((a) => (
              <SelectItem key={a.agent_id} value={a.agent_id} className="text-[11px] font-mono">{a.agent_id}</SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select value={sort} onValueChange={setSort}>
          <SelectTrigger className="w-[140px] h-8 bg-transparent border-border hover:bg-card transition-all text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
            <SelectValue />
          </SelectTrigger>
          <SelectContent className="glass-card">
            <SelectItem value="importance" className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("filters.importance")}</SelectItem>
            <SelectItem value="recent" className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("filters.recent")}</SelectItem>
            <SelectItem value="access_count" className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("filters.accessCount")}</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Memory Table */}
      <div className="glass-card rounded-xl overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow className="border-border hover:bg-transparent bg-card">
              <TableHead className="w-24 px-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.user")}</TableHead>
              <TableHead className="w-24 px-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.agent")}</TableHead>
              <TableHead className="text-center w-14 px-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.level")}</TableHead>
              <TableHead className="w-28 px-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.score")}</TableHead>
              <TableHead className="text-center w-16 px-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.access")}</TableHead>
              <TableHead className="text-center w-14 px-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.refs")}</TableHead>
              <TableHead className="px-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("table.content")}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i} className="border-border">
                  <TableCell colSpan={7}>
                    <Skeleton className="h-10 w-full opacity-20" />
                  </TableCell>
                </TableRow>
              ))
            ) : memories?.items.length === 0 ? (
              <TableRow className="border-border hover:bg-transparent">
                <TableCell colSpan={7} className="text-center text-muted-foreground py-16">
                  <div className="flex flex-col items-center gap-3">
                    <div className="w-12 h-12 rounded-2xl bg-card border border-border flex items-center justify-center">
                      <List className="w-6 h-6 opacity-20" />
                    </div>
                    <p className="text-sm font-medium">{t("empty")}</p>
                  </div>
                </TableCell>
              </TableRow>
            ) : (
              memories?.items.map((m) => {
                const canOpenDetail = m.item_type !== "event";
                return (
                  <TableRow
                    key={m.id}
                    onClick={() => {
                      const selection = window.getSelection();
                      const selectingText =
                        !!selection &&
                        selection.type === "Range" &&
                        selection.toString().trim().length > 0;
                      if (selectingText) return;

                      if (canOpenDetail) {
                        handleViewDetail(m.id);
                      }
                    }}
                    className={`border-white/5 transition-colors ${canOpenDetail ? "cursor-pointer group hover:bg-white/[0.03]" : "opacity-95"}`}
                  >
                    <TableCell>
                      <span className="text-xs font-mono truncate block max-w-[100px] text-foreground/80">{m.user_id}</span>
                    </TableCell>
                    <TableCell>
                      {m.agent_id ? (
                        <Badge variant="outline" className="text-[11px] h-5 px-1.5 font-mono bg-card border-border">{m.agent_id}</Badge>
                      ) : (
                        <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">—</span>
                      )}
                    </TableCell>
                    <TableCell className="text-center">
                      <div className="flex flex-col items-center gap-1">
                        <LevelBadge level={m.level} />
                        {m.memory_type === "procedural" ? (
                          <span className="text-accent/80 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("types.procedural")}</span>
                        ) : m.memory_type === "factual" ? (
                          <span className="text-primary/80 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("types.factual")}</span>
                        ) : null}
                      </div>
                    </TableCell>
                    <TableCell><ImportanceBar value={m.importance} /></TableCell>
                    <TableCell className="text-center font-mono text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{m.access_count}</TableCell>
                    <TableCell className="text-center font-mono text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{m.reference_count}</TableCell>
                    <TableCell>
                      <MemoryContent content={m.content} />
                      {m.keywords.length > 0 && (
                        <div className="flex gap-1.5 mt-2 flex-wrap">
                          {m.keywords.slice(0, 3).map((kw) => (
                            <span key={kw} className="bg-card px-1.5 py-0.5 rounded border border-border text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{kw}</span>
                          ))}
                        </div>
                      )}
                    </TableCell>
                  </TableRow>
                );
              })
            )}
          </TableBody>
        </Table>

        {/* Pagination */}
        {memories && memories.total > 20 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-border bg-card">
            <span className="font-medium text-[11px] uppercase tracking-widest text-muted-foreground">
              {t("pagination.showing", { from: (page - 1) * 20 + 1, to: Math.min(page * 20, memories.total), total: memories.total })}
            </span>
            <div className="flex gap-1">
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setPage(Math.max(1, page - 1))}
                disabled={page === 1}
              >
                <ChevronLeft className="w-4 h-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setPage(page + 1)}
                disabled={page * 20 >= memories.total}
              >
                <ChevronRight className="w-4 h-4" />
              </Button>
            </div>
          </div>
        )}
      </div>

      {/* Detail Sheet */}
      <MemoryDetailSheet
        memory={selectedMemory}
        open={!!selectedMemory}
        onOpenChange={(open) => { if (!open) setSelectedMemory(null); }}
      />
    </div>
  );
}

function TasksTab({ userId }: { userId?: string }) {
  const t = useTranslations("Memories");
  const { data: trees, isLoading, error } = useTaskTree(userId);

  if (!userId || userId === "all") {
    return (
      <div className="flex flex-col items-center justify-center p-8 text-center text-muted-foreground border rounded-lg bg-muted/5 border-dashed mt-4">
        <p>{t("taskScope")}</p>
      </div>
    );
  }

  if (isLoading) return <div className="p-4 text-sm text-muted-foreground flex items-center gap-2"><Loader2 className="w-4 h-4 animate-spin" /> {t("graph.loading")}</div>;
  if (error) return <div className="p-4 text-sm text-red-500 border border-red-200 bg-red-50/50 rounded-lg">Error loading tasks: {error.message}</div>;

  return (
    <div className="flex-1 overflow-auto pr-2 mt-4">
      <TaskTreeViewer trees={trees || []} />
    </div>
  );
}

export default function MemoriesPage() {
  const t = useTranslations("Memories");
  const [userIdInput, setUserIdInput] = useStoredString("memorose-dashboard-memories-user");
  const { orgId } = useOrgScope();
  const userId = userIdInput.trim();
  const scopedOrgId = orgId.trim();

  return (
    <div className="space-y-6 h-full flex flex-col relative">
      <div className="absolute top-0 right-0 w-[500px] h-[250px] blob-bg opacity-20 pointer-events-none -z-10 mix-blend-screen" />
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-foreground">{t("title")}</h1>
          <p className="text-sm text-muted-foreground mt-1">
            {scopedOrgId ? t("subtitleOrg", { orgId: scopedOrgId }) : t("subtitle")}
          </p>
        </div>

        <div className="flex w-full max-w-xl flex-col gap-2 sm:flex-row sm:items-center">
          <Input
            value={userIdInput}
            onChange={(e) => setUserIdInput(e.target.value)}
            placeholder={t("setUserId")}
            className="h-10 font-mono bg-card border-border"
          />
        </div>
      </div>

      <Tabs defaultValue="list" className="flex-1 flex flex-col min-h-0">
        <TabsList className="bg-card border border-border self-start p-1 rounded-xl">
          <TabsTrigger value="list" className="gap-2 px-4 data-[state=active]:bg-white/5 data-[state=active]:text-white transition-all text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
            <List className="w-3 h-3 opacity-60" /> {t("tabs.list")}
          </TabsTrigger>
          <TabsTrigger value="graph" className="gap-2 px-4 data-[state=active]:bg-white/5 data-[state=active]:text-white transition-all text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
            <Network className="w-3 h-3 opacity-60" /> {t("tabs.graph")}
          </TabsTrigger>
          <TabsTrigger value="search" className="gap-2 px-4 data-[state=active]:bg-white/5 data-[state=active]:text-white transition-all text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
            <Search className="w-3 h-3 opacity-60" /> {t("tabs.search")}
          </TabsTrigger>
          <TabsTrigger value="tasks" className="gap-2 px-4 data-[state=active]:bg-white/5 data-[state=active]:text-white transition-all text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
            <CheckSquare className="w-3 h-3 opacity-60" /> {t("tabs.tasks")}
          </TabsTrigger>
        </TabsList>

        <TabsContent value="list" className="flex-1 mt-4">
          <MemoryListTab userId={userId || undefined} orgId={scopedOrgId || undefined} />
        </TabsContent>
        <TabsContent value="graph" className="flex-1 mt-4">
          <KnowledgeGraph userId={userId || undefined} orgId={scopedOrgId || undefined} />
        </TabsContent>
        <TabsContent value="search" className="flex-1 mt-4">
          <SearchPlayground globalUserId={userId || undefined} orgId={scopedOrgId || undefined} />
        </TabsContent>
        <TabsContent value="tasks" className="flex-1 mt-0">
          <TasksTab userId={userId || undefined} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
