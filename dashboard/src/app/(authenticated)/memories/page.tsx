"use client";

import { useState, useCallback, useEffect, useMemo } from "react";
import dynamic from "next/dynamic";
import { useMemories, useGraph } from "@/lib/hooks";
import { useUserFilter } from "../layout";
import { api } from "@/lib/api";
import { truncate, formatNumber } from "@/lib/utils";
import type { MemoryUnit, SearchResult, GraphData } from "@/lib/types";
import {
  Search,
  ChevronLeft,
  ChevronRight,
  Loader2,
  Network,
  Brain,
  List,
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

function LevelBadge({ level }: { level: number }) {
  const colors: Record<number, string> = {
    0: "bg-muted text-muted-foreground border-border",
    1: "bg-primary/15 text-primary border-primary/20 hover:bg-primary/20",
    2: "bg-success/15 text-success border-success/20 hover:bg-success/20",
  };
  return (
    <Badge variant="outline" className={`text-[11px] px-1.5 py-0 font-mono ${colors[level] || "bg-muted text-muted-foreground"}`}>
      L{level}
    </Badge>
  );
}

function ImportanceBar({ value }: { value: number }) {
  return (
    <div className="flex items-center gap-2">
      <div className="w-14 h-1 bg-muted rounded-full overflow-hidden">
        <div
          className="h-full bg-primary/80 rounded-full transition-all"
          style={{ width: `${value * 100}%` }}
        />
      </div>
      <span className="text-[11px] font-mono text-muted-foreground">{value.toFixed(2)}</span>
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
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-md">
        <SheetHeader>
          <SheetTitle>Memory Detail</SheetTitle>
        </SheetHeader>

        {memory && (
          <div className="space-y-3 px-4 pb-4">
            <div className="rounded-md bg-muted/30 px-3 py-2">
              <label className="text-[11px] uppercase tracking-wider text-muted-foreground">ID</label>
              <p className="font-mono text-xs break-all mt-0.5">{memory.id}</p>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="rounded-md bg-muted/30 px-3 py-2">
                <label className="text-[11px] uppercase tracking-wider text-muted-foreground">User</label>
                <p className="font-mono text-xs mt-0.5">{memory.user_id}</p>
              </div>
              <div className="rounded-md bg-muted/30 px-3 py-2">
                <label className="text-[11px] uppercase tracking-wider text-muted-foreground">App</label>
                <p className="font-mono text-xs mt-0.5">{memory.app_id}</p>
              </div>
            </div>
            <div>
              <label className="text-[11px] uppercase tracking-wider text-muted-foreground">Content</label>
              <p className="text-sm mt-1 whitespace-pre-wrap leading-relaxed">{memory.content}</p>
            </div>
            <div className="h-px bg-border" />
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-[11px] uppercase tracking-wider text-muted-foreground">Level</label>
                <p className="mt-1"><LevelBadge level={memory.level} /></p>
              </div>
              <div>
                <label className="text-[11px] uppercase tracking-wider text-muted-foreground">Importance</label>
                <p className="mt-1"><ImportanceBar value={memory.importance} /></p>
              </div>
              <div>
                <label className="text-[11px] uppercase tracking-wider text-muted-foreground">Access Count</label>
                <p className="text-sm font-mono mt-0.5">{memory.access_count}</p>
              </div>
              <div>
                <label className="text-[11px] uppercase tracking-wider text-muted-foreground">Stream</label>
                <p className="text-sm font-mono mt-0.5 truncate">{memory.stream_id}</p>
              </div>
            </div>
            {memory.keywords.length > 0 && (
              <>
                <div className="h-px bg-border" />
                <div>
                  <label className="text-[11px] uppercase tracking-wider text-muted-foreground">Keywords</label>
                  <div className="flex flex-wrap gap-1 mt-1.5">
                    {memory.keywords.map((kw) => (
                      <Badge key={kw} variant="secondary" className="text-[11px] font-normal">{kw}</Badge>
                    ))}
                  </div>
                </div>
              </>
            )}
            <div className="h-px bg-border" />
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-[11px] uppercase tracking-wider text-muted-foreground">Transaction Time</label>
                <p className="text-xs font-mono mt-0.5">{new Date(memory.transaction_time).toLocaleString()}</p>
              </div>
              {memory.valid_time && (
                <div>
                  <label className="text-[11px] uppercase tracking-wider text-muted-foreground">Valid Time</label>
                  <p className="text-xs font-mono mt-0.5">{new Date(memory.valid_time).toLocaleString()}</p>
                </div>
              )}
            </div>
            {memory.references.length > 0 && (
              <>
                <div className="h-px bg-border" />
                <div>
                  <label className="text-[11px] uppercase tracking-wider text-muted-foreground">References ({memory.references.length})</label>
                  <div className="space-y-0.5 mt-1">
                    {memory.references.map((ref) => (
                      <p key={ref} className="text-[11px] font-mono text-muted-foreground truncate">{ref}</p>
                    ))}
                  </div>
                </div>
              </>
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

function KnowledgeGraph({ userId }: { userId?: string }) {
  const { data, isLoading } = useGraph(200, userId);

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
        <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading neural graph...
      </div>
    );
  }

  if (!data || data.nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-[500px] text-muted-foreground glass-card rounded-xl">
        <Network className="w-5 h-5 mr-2 opacity-50" /> No neural pathways established yet
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full relative group">
      <div className="absolute top-4 left-4 z-10 flex gap-3 text-[11px] font-mono uppercase tracking-wider text-muted-foreground bg-background/40 backdrop-blur-md px-3 py-1.5 rounded-full border border-white/10">
        <span className="flex items-center gap-1.5"><div className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse" /> {data.stats.node_count} nodes</span>
        <span className="flex items-center gap-1.5"><div className="w-1.5 h-1.5 rounded-full bg-primary/50" /> {data.stats.edge_count} edges</span>
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

function SearchPlayground({ globalUserId }: { globalUserId?: string }) {
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState("hybrid");
  const [searchUserId, setSearchUserId] = useState(globalUserId || "");
  const [appId, setAppId] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [queryTime, setQueryTime] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setSearchUserId(globalUserId || "");
  }, [globalUserId]);

  async function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    if (!query.trim()) return;
    setLoading(true);
    try {
      const res = await api.search({
        query,
        mode,
        limit: 10,
        user_id: searchUserId || undefined,
        app_id: appId || undefined,
      });
      setResults(res.results);
      setQueryTime(res.query_time_ms);
    } catch {
      setResults([]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <form onSubmit={handleSearch} className="space-y-2 mb-4">
        <div className="flex gap-2">
          <Input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="flex-1"
            placeholder="Search memories..."
          />
          <Select value={mode} onValueChange={setMode}>
            <SelectTrigger className="w-[120px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="hybrid">Hybrid</SelectItem>
              <SelectItem value="text">Text</SelectItem>
              <SelectItem value="vector">Vector</SelectItem>
            </SelectContent>
          </Select>
          <Button type="submit" disabled={loading} size="icon">
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
          </Button>
        </div>
        <div className="flex gap-2">
          <Input
            type="text"
            value={searchUserId}
            onChange={(e) => setSearchUserId(e.target.value)}
            placeholder="user_id"
            className="flex-1 h-7 text-xs"
          />
          <Input
            type="text"
            value={appId}
            onChange={(e) => setAppId(e.target.value)}
            placeholder="app_id"
            className="flex-1 h-7 text-xs"
          />
        </div>
      </form>

      {queryTime !== null && (
        <p className="text-xs text-muted-foreground mb-3">
          {results.length} results in {queryTime}ms
        </p>
      )}

      <div className="space-y-2 max-h-[350px] overflow-y-auto">
        {results.map((r) => (
          <div key={r.unit.id} className="p-3 rounded-lg bg-background border border-border">
            <div className="flex items-center gap-2 mb-1">
              <LevelBadge level={r.unit.level} />
              {r.unit.memory_type === "procedural" ? (
                <Badge variant="outline" className="text-[10px] h-5 px-1.5 text-accent border-accent/30 bg-accent/5">Procedural</Badge>
              ) : (
                <Badge variant="outline" className="text-[10px] h-5 px-1.5 text-primary border-primary/30 bg-primary/5">Factual</Badge>
              )}
              <span className="text-xs font-mono text-muted-foreground">
                score: {r.score.toFixed(4)}
              </span>
            </div>
            <p className="text-sm">{truncate(r.unit.content, 200)}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function MemoryContent({ content }: { content: string }) {
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
          {expanded ? "collapse" : "view"}
        </button>
        <span className="text-muted-foreground/30">·</span>
        <button
          onClick={handleCopy}
          className="text-[11px] text-primary hover:text-primary/80 hover:underline whitespace-nowrap bg-transparent border-none cursor-pointer p-0 font-normal"
        >
          {copied ? "copied!" : "copy"}
        </button>
      </div>
    </div>
  );
}

function MemoryListTab({ userId }: { userId?: string }) {
  const [levelFilter, setLevelFilter] = useState<string>("all");
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState("importance");
  const [selectedMemory, setSelectedMemory] = useState<MemoryUnit | null>(null);

  const parsedLevel = levelFilter === "all" ? undefined : Number(levelFilter);

  const { data: memories, isLoading } = useMemories({
    level: parsedLevel,
    page,
    limit: 20,
    sort,
    user_id: userId,
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
      <div className="flex flex-wrap items-center gap-3">
        <ToggleGroup
          type="single"
          value={levelFilter}
          onValueChange={(v) => { setLevelFilter(v || "all"); setPage(1); }}
        >
          <ToggleGroupItem value="all" aria-label="All levels">All</ToggleGroupItem>
          <ToggleGroupItem value="0" aria-label="Level 0">L0</ToggleGroupItem>
          <ToggleGroupItem value="1" aria-label="Level 1">L1</ToggleGroupItem>
          <ToggleGroupItem value="2" aria-label="Level 2">L2</ToggleGroupItem>
        </ToggleGroup>

        <Select value={sort} onValueChange={setSort}>
          <SelectTrigger className="w-[180px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="importance">Sort: Importance</SelectItem>
            <SelectItem value="recent">Sort: Recent</SelectItem>
            <SelectItem value="access_count">Sort: Access Count</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Memory Table */}
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow className="bg-muted/20 hover:bg-muted/20">
              <TableHead className="text-xs w-24">User</TableHead>
              <TableHead className="text-xs text-center w-14">Level</TableHead>
              <TableHead className="text-xs w-28">Importance</TableHead>
              <TableHead className="text-xs text-center w-16">Access</TableHead>
              <TableHead className="text-xs text-center w-14">Refs</TableHead>
              <TableHead className="text-xs">Content</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i}>
                  <TableCell colSpan={6}>
                    <Skeleton className="h-4 w-full" />
                  </TableCell>
                </TableRow>
              ))
            ) : memories?.items.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                  No memories found
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
                    className={canOpenDetail ? "cursor-pointer group" : "opacity-95"}
                  >
                    <TableCell>
                      <span className="text-xs font-mono truncate block max-w-[100px]">{m.user_id}</span>
                    </TableCell>
                    <TableCell className="text-center">
                      <div className="flex flex-col items-center gap-1">
                        <LevelBadge level={m.level} />
                        {m.memory_type === "procedural" ? (
                          <span className="text-[9px] uppercase tracking-wider text-accent">Procedural</span>
                        ) : m.memory_type === "factual" ? (
                          <span className="text-[9px] uppercase tracking-wider text-primary">Factual</span>
                        ) : null}
                      </div>
                    </TableCell>
                    <TableCell><ImportanceBar value={m.importance} /></TableCell>
                    <TableCell className="text-center font-mono text-xs">{m.access_count}</TableCell>
                    <TableCell className="text-center font-mono text-xs">{m.reference_count}</TableCell>
                    <TableCell>
                      <MemoryContent content={m.content} />
                      {m.keywords.length > 0 && (
                        <div className="flex gap-1.5 mt-2">
                          {m.keywords.slice(0, 3).map((kw) => (
                            <span key={kw} className="text-[11px] text-muted-foreground/70">{kw}</span>
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
          <div className="flex items-center justify-between px-4 py-3 border-t border-border">
            <span className="text-sm text-muted-foreground">
              Showing {(page - 1) * 20 + 1}-{Math.min(page * 20, memories.total)} of {memories.total}
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

export default function MemoriesPage() {
  const { userId } = useUserFilter();

  return (
    <div className="space-y-6 h-full flex flex-col">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Memory Explorer</h1>
      </div>

      <Tabs defaultValue="list" className="flex-1 flex flex-col min-h-0">
        <TabsList className="bg-muted/30 self-start">
          <TabsTrigger value="list" className="gap-1.5">
            <List className="w-3.5 h-3.5" /> List
          </TabsTrigger>
          <TabsTrigger value="graph" className="gap-1.5">
            <Network className="w-3.5 h-3.5" /> Graph
          </TabsTrigger>
          <TabsTrigger value="search" className="gap-1.5">
            <Search className="w-3.5 h-3.5" /> Search
          </TabsTrigger>
        </TabsList>

        <TabsContent value="list" className="flex-1 mt-4">
          <MemoryListTab userId={userId || undefined} />
        </TabsContent>
        <TabsContent value="graph" className="flex-1 mt-4">
          <KnowledgeGraph userId={userId || undefined} />
        </TabsContent>
        <TabsContent value="search" className="flex-1 mt-4">
          <SearchPlayground globalUserId={userId || undefined} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
