"use client";

import { useState, useRef, useEffect } from "react";
import { api } from "@/lib/api";
import { getToken } from "@/lib/auth";
import { useStoredString } from "@/lib/hooks";
import { useOrgScope } from "@/lib/org-scope";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Loader2, Send, Bot, Sparkles, Search, SlidersHorizontal, Trash2 } from "lucide-react";
import { cn } from "@/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import type { ForgetPreviewResponse, RetrieveResponse } from "@/lib/types";
import { useTranslations } from "next-intl";
import { DashboardHero } from "@/components/dashboard-chrome";
import { MemoryAssets } from "@/components/memory-assets";

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
}

function ScopeBadge({
  text,
  tone = "neutral",
}: {
  text: string;
  tone?: "neutral" | "danger";
}) {
  return (
    <div
      className={cn(
        "inline-flex items-center gap-2 rounded-full border px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider",
        tone === "danger"
          ? "border-destructive/20 bg-destructive/8 text-destructive/90"
          : "border-border/70 bg-background/55 text-muted-foreground/85"
      )}
    >
      <span className="h-1.5 w-1.5 rounded-full bg-current opacity-70" />
      <span className="font-mono normal-case tracking-normal">{text}</span>
    </div>
  );
}

function PanelIntro({
  icon: Icon,
  eyebrow,
  title,
  description,
  trailing,
}: {
  icon: React.ElementType;
  eyebrow: string;
  title: string;
  description?: string;
  trailing?: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
      <div className="min-w-0">
        <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground/70">
          <Icon className="h-3.5 w-3.5 text-primary/70" />
          {eyebrow}
        </div>
        <h3 className="mt-3 text-lg font-semibold tracking-tight text-foreground/92">{title}</h3>
        {description ? <p className="mt-1 max-w-2xl text-sm leading-6 text-muted-foreground">{description}</p> : null}
      </div>
      {trailing ? <div className="shrink-0">{trailing}</div> : null}
    </div>
  );
}

function TypingIndicator() {
  return (
    <div className="flex space-x-1.5 items-center p-2">
      <motion.div
        className="w-1.5 h-1.5 bg-primary/60 rounded-full"
        animate={{ y: [0, -4, 0] }}
        transition={{ duration: 0.6, repeat: Infinity, delay: 0 }}
      />
      <motion.div
        className="w-1.5 h-1.5 bg-primary/60 rounded-full"
        animate={{ y: [0, -4, 0] }}
        transition={{ duration: 0.6, repeat: Infinity, delay: 0.2 }}
      />
      <motion.div
        className="w-1.5 h-1.5 bg-primary/60 rounded-full"
        animate={{ y: [0, -4, 0] }}
        transition={{ duration: 0.6, repeat: Infinity, delay: 0.4 }}
      />
    </div>
  );
}

function ChatPanel() {
  const t = useTranslations("Playground");
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [streaming, setStreaming] = useState(false);
  const [userId] = useStoredString("memorose-playground-chat-user", "default-playground-user");
  const { orgId } = useOrgScope();
  const scrollRef = useRef<HTMLDivElement>(null);
  const streamingMessageRef = useRef<string>("");
  const scopedOrgId = orgId.trim();

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, streaming]);

  const handleSend = async () => {
    if (!input.trim() || loading) return;

    const currentUserId = userId.trim() || "default-playground-user";

    const userMessage: Message = {
      id: Date.now().toString(),
      role: "user",
      content: input.trim(),
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    const messageContent = input.trim();
    setInput("");
    setLoading(true);
    setStreaming(true);
    streamingMessageRef.current = "";

    try {
      await api.ingestEvent({
        user_id: currentUserId,
        stream_id: "chat",
        content: {
          type: "text",
          data: messageContent,
        },
      });

      const response = await fetch("/v1/dashboard/chat", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getToken()}`,
        },
        body: JSON.stringify({
          message: messageContent,
          user_id: currentUserId,
          ...(scopedOrgId ? { org_id: scopedOrgId } : {}),
          context_limit: 5,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();

      if (!reader) {
        throw new Error("No response body");
      }

      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: "",
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value);
        const lines = chunk.split("\n");

        for (const line of lines) {
          if (line.startsWith("data: ")) {
            const data = line.slice(6);
            if (data === "[DONE]") continue;

            streamingMessageRef.current += data;
            setMessages((prev) => {
              const newMessages = [...prev];
              const lastMsg = newMessages[newMessages.length - 1];
              if (lastMsg.role === "assistant") {
                lastMsg.content = streamingMessageRef.current;
              }
              return newMessages;
            });
          } else if (line.startsWith("event: ")) {
            const event = line.slice(7);
            if (event === "done") {
              break;
            } else if (event === "error") {
              throw new Error("Stream error");
            }
          }
        }
      }
    } catch (error) {
      console.error("Chat error:", error);
      setMessages((prev) => [
        ...prev,
        {
          id: Date.now().toString(),
          role: "assistant",
          content: `${t("chat.errorPrefix")}: ${error instanceof Error ? error.message : t("retrieve.unknownError")}`,
          timestamp: new Date(),
        },
      ]);
    } finally {
      setLoading(false);
      setStreaming(false);
      streamingMessageRef.current = "";
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
      <Card className="relative flex h-full min-h-0 flex-1 flex-col overflow-hidden rounded-[1.6rem] border border-white/8 bg-card/75">
        <div className="pointer-events-none absolute inset-x-0 top-0 h-28 bg-[linear-gradient(180deg,rgba(255,255,255,0.06),transparent)]" />

        <div className="min-h-0 flex-1 overflow-y-auto px-5 py-5 scroll-smooth md:px-6" ref={scrollRef}>
          <div className="mx-auto max-w-3xl space-y-6">
            <AnimatePresence>
              {messages.length === 0 && (
                <motion.div
                  initial={{ opacity: 0, scale: 0.98 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.98 }}
                  className="flex min-h-[min(36vh,22rem)] flex-col items-center justify-center px-8 text-muted-foreground"
                >
                  <Bot className="h-10 w-10 opacity-45" />
                </motion.div>
              )}
            </AnimatePresence>

            <AnimatePresence initial={false}>
              {messages.map((message) => (
                <motion.div
                  key={message.id}
                  initial={{ opacity: 0, y: 15 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ type: "spring", stiffness: 350, damping: 30 }}
                  className={cn(
                    "flex gap-4",
                    message.role === "user" ? "justify-end" : "justify-start"
                  )}
                >
                  {message.role === "assistant" && (
                    <div className="w-9 h-9 rounded-full bg-card border border-border flex items-center justify-center shrink-0 mt-1">
                      <Bot className="w-5 h-5 text-primary opacity-60" />
                    </div>
                  )}
                  <div
                    className={cn(
                      "max-w-[82%] rounded-[1.35rem] px-5 py-4 text-sm leading-7 backdrop-blur-md",
                      message.role === "user"
                        ? "rounded-tr-md bg-primary text-primary-foreground shadow-[0_14px_34px_rgba(255,92,92,0.24)]"
                        : "rounded-tl-md border border-white/[0.06] bg-white/[0.03] text-foreground/90 shadow-[0_10px_24px_rgba(0,0,0,0.12)]"
                    )}
                  >
                    <p className="whitespace-pre-wrap">{message.content}</p>
                    {message.role === "assistant" && message.content === "" && !streaming && (
                      <span className="opacity-40 italic text-sm">{t("chat.emptyResponse")}</span>
                    )}
                    {message.role === "assistant" && message.content === "" && streaming && (
                      <TypingIndicator />
                    )}
                  </div>
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        </div>

        <div className="sticky bottom-0 z-20 shrink-0 border-t border-white/6 bg-card/90 px-5 py-4 backdrop-blur md:px-6">
          <div className="group relative mx-auto max-w-3xl">
            <div className="absolute -inset-1 rounded-[1.6rem] bg-gradient-to-r from-primary/18 via-primary/8 to-transparent blur-xl opacity-20 transition duration-1000 group-hover:opacity-40" />
            <div className="relative rounded-[1.6rem] border border-white/8 bg-background/60 p-2.5 backdrop-blur-xl">
              <div className="flex items-center gap-4">
              <Input
                type="text"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder={t("chat.placeholder")}
                disabled={loading}
                className="h-12 flex-1 border-none bg-transparent px-4 text-sm focus-visible:ring-0 placeholder:text-muted-foreground/35"
              />
              <Button
                onClick={handleSend}
                disabled={loading || !input.trim()}
                size="icon"
                className={cn(
                  "mr-1 h-10 w-10 rounded-xl transition-all duration-300",
                  input.trim()
                    ? "bg-primary text-primary-foreground shadow-[0_0_18px_rgba(255,92,92,0.32)] hover:scale-[1.03]"
                    : "bg-white/5 text-muted-foreground/40 hover:bg-white/10"
                )}
              >
                {loading ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <Send className="w-5 h-5" />
                )}
              </Button>
              </div>
            </div>
          </div>
        </div>
      </Card>
    </div>
  );
}

function RetrievePanel() {
  const t = useTranslations("Playground");
  const [userId] = useStoredString("memorose-playground-retrieve-user", "default-playground-user");
  const [streamId, setStreamId] = useStoredString("memorose-playground-retrieve-stream", "chat");
  const [query, setQuery] = useState("");
  const [limit, setLimit] = useState("10");
  const [minScore, setMinScore] = useState("");
  const [graphDepth, setGraphDepth] = useState("");
  const [validTimeStart, setValidTimeStart] = useState("");
  const [validTimeEnd, setValidTimeEnd] = useState("");
  const [asOf, setAsOf] = useState("");
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<RetrieveResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const { orgId } = useOrgScope();
  const scopedOrgId = orgId.trim();
  const [streams] = useState<string[]>(["chat", "system", "logs", "internal"]);

  async function handleRetrieve() {
    if (!query.trim() || !streamId.trim()) return;
    
    const currentUserId = userId.trim() || "default-playground-user";
    
    setLoading(true);
    setError(null);
    setResults(null);
    try {
      const body = {
        query: query.trim(),
        ...(limit ? { limit: Number(limit) } : {}),
        ...(minScore ? { min_score: Number(minScore) } : {}),
        ...(graphDepth ? { graph_depth: Number(graphDepth) } : {}),
        ...(validTimeStart ? { start_time: validTimeStart } : {}),
        ...(validTimeEnd ? { end_time: validTimeEnd } : {}),
        ...(asOf ? { as_of: asOf } : {}),
        ...(scopedOrgId ? { org_id: scopedOrgId } : {}),
      };
      const res = await api.retrieve(currentUserId, streamId.trim(), body);
      setResults(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : t("retrieve.unknownError"));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-6">
      <Card className="rounded-[1.6rem] border border-white/8 bg-card/75 p-6">
        <div className="space-y-6">
          <PanelIntro
            icon={Search}
            eyebrow={t("tabs.retrieve")}
            title={t("retrieve.button")}
            description={t("subtitle")}
            trailing={
              scopedOrgId ? (
                <ScopeBadge text={t("retrieve.orgScope", { orgId: scopedOrgId })} />
              ) : undefined
            }
          />

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-[minmax(0,17rem)_minmax(0,1fr)]">
            <div className="space-y-2 max-w-sm">
              <label className="px-1 label-xs">{t("retrieve.streamId")}</label>
              <Input
                list="stream-suggestions"
                value={streamId}
                onChange={(e) => setStreamId(e.target.value)}
                className="h-11 border-border/70 bg-background/45 text-sm font-mono"
              />
              <datalist id="stream-suggestions">
                {streams.map(s => <option key={s} value={s} />)}
              </datalist>
            </div>

            <div className="flex flex-col gap-4 lg:justify-end">
            <Input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={t("retrieve.queryPlaceholder")}
                className="h-14 flex-1 border-border/70 bg-background/45 px-6 text-base placeholder:text-muted-foreground/20"
              onKeyDown={(e) => e.key === "Enter" && handleRetrieve()}
            />
              <div className="flex flex-wrap items-center gap-3">
                <Button
                  onClick={handleRetrieve}
                  disabled={loading || !query.trim() || !streamId.trim()}
                  className="h-12 gap-3 rounded-2xl px-6 text-[10px] font-bold uppercase tracking-wider"
                >
                  {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Search className="w-5 h-5" />}
                  {t("retrieve.button")}
                </Button>
                <span className="rounded-full border border-border/70 bg-background/35 px-3 py-1 text-xs font-mono text-muted-foreground/75">
                  {streamId.trim() || "chat"}
                </span>
              </div>
            </div>
          </div>
        </div>
      </Card>

      <Card className="rounded-[1.6rem] border border-white/8 bg-card/70 p-6">
        <div className="space-y-4">
          <div className="mb-2 flex items-center gap-2">
            <SlidersHorizontal className="h-3.5 w-3.5 text-muted-foreground/45" />
            <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">{t("retrieve.parameters")}</span>
          </div>
          <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6">
            {[
              { label: t("retrieve.params.limit"), value: limit, setter: setLimit, placeholder: "10" },
              { label: t("retrieve.params.score"), value: minScore, setter: setMinScore, placeholder: "0.0" },
              { label: t("retrieve.params.depth"), value: graphDepth, setter: setGraphDepth, placeholder: "1" },
              { label: t("retrieve.params.from"), value: validTimeStart, setter: setValidTimeStart, placeholder: "ISO" },
              { label: t("retrieve.params.to"), value: validTimeEnd, setter: setValidTimeEnd, placeholder: "ISO" },
              { label: t("retrieve.params.asOf"), value: asOf, setter: setAsOf, placeholder: "NOW" },
            ].map((p) => (
              <div key={p.label} className="space-y-2">
                <label className="px-1 label-xs">{p.label}</label>
                <Input value={p.value} onChange={(e) => p.setter(e.target.value)} placeholder={p.placeholder} className="h-10 border-border/70 bg-background/45 text-xs font-mono" />
              </div>
            ))}
          </div>
        </div>
      </Card>

      {/* Error */}
      {error && (
        <div className="text-sm text-destructive bg-destructive/10 rounded-lg px-4 py-2 border border-destructive/20">
          {error}
        </div>
      )}

      {/* Results */}
      {results && (
        <div className="space-y-3">
          <div className="flex items-center justify-between gap-4">
            <span className="text-sm font-medium text-foreground/90">{t("retrieve.results", { count: results.results.length })}</span>
            <span className="rounded-full border border-border/70 bg-background/45 px-3 py-1 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
              {results.query_time_ms.toFixed(1)}ms
            </span>
          </div>
          {results.results.length === 0 ? (
            <Card className="rounded-[1.6rem] border border-dashed border-border/70 bg-card/60">
              <div className="p-8 text-center text-sm text-muted-foreground">{t("retrieve.noResults")}</div>
            </Card>
          ) : (
            results.results.map((r, i) => (
              <Card key={r.unit.id || i} className="rounded-[1.6rem] border border-white/8 bg-card/75">
                <div className="p-5">
                  <div className="mb-3 flex items-start justify-between gap-4">
                    <p className="flex-1 text-sm leading-7 text-foreground/92">{r.unit.content}</p>
                    <div className="shrink-0 text-right">
                      <div className="text-sm font-mono font-semibold text-primary">{(r.score * 100).toFixed(1)}%</div>
                      <div className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">{t("retrieve.scoreLabel")}</div>
                    </div>
                  </div>
                  <div className="flex flex-wrap items-center gap-2.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                    <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1 font-mono">{r.unit.id.substring(0, 8)}</span>
                    <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1">L{r.unit.level}</span>
                    {r.unit.memory_type && <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1 capitalize">{r.unit.memory_type}</span>}
                    {r.unit.keywords.slice(0, 4).map((kw) => (
                      <span key={kw} className="rounded-full border border-white/8 bg-white/[0.03] px-2.5 py-1">{kw}</span>
                    ))}
                  </div>
                  {r.unit.assets.length > 0 ? (
                    <div className="mt-3">
                      <MemoryAssets assets={r.unit.assets.slice(0, 2)} compact />
                    </div>
                  ) : null}
                </div>
              </Card>
            ))
          )}
        </div>
      )}
    </div>
  );
}

function ForgetPanel() {
  const t = useTranslations("Playground");
  const [userId] = useStoredString("memorose-playground-forget-user", "default-playground-user");
  const { orgId } = useOrgScope();
  const scopedOrgId = orgId.trim();
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<"logical" | "hard">("logical");
  const [limit, setLimit] = useState("10");
  const [preview, setPreview] = useState<ForgetPreviewResponse | null>(null);
  const [loadingPreview, setLoadingPreview] = useState(false);
  const [executing, setExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  async function handlePreview() {
    const currentUserId = userId.trim() || "default-playground-user";
    if (!query.trim()) return;

    setLoadingPreview(true);
    setError(null);
    setSuccess(null);
    setPreview(null);
    try {
      const result = await api.forgetPreview({
        user_id: currentUserId,
        query: query.trim(),
        mode,
        ...(limit ? { limit: Number(limit) } : {}),
        ...(scopedOrgId ? { org_id: scopedOrgId } : {}),
      });
      setPreview(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : t("forget.errors.preview"));
    } finally {
      setLoadingPreview(false);
    }
  }

  async function handleExecute() {
    const currentUserId = userId.trim() || "default-playground-user";
    if (!preview) return;

    setExecuting(true);
    setError(null);
    setSuccess(null);
    try {
      const result = await api.forgetExecute({
        user_id: currentUserId,
        preview_id: preview.preview_id,
        confirm: true,
        ...(scopedOrgId ? { org_id: scopedOrgId } : {}),
      });
      setSuccess(
        t("forget.executeSuccess", {
          memories: result.forgotten_memory_unit_count,
          events: result.forgotten_event_count,
        })
      );
      setPreview(null);
      setQuery("");
    } catch (err) {
      setError(err instanceof Error ? err.message : t("forget.errors.execute"));
    } finally {
      setExecuting(false);
    }
  }

  return (
    <div className="space-y-6">
      <Card className="rounded-[1.6rem] border border-white/8 bg-card/75 p-6">
        <div className="space-y-6">
          <PanelIntro
            icon={Trash2}
            eyebrow={t("tabs.forget")}
            title={t("forget.title")}
            description={t("forget.description")}
            trailing={
              scopedOrgId ? (
                <ScopeBadge text={t("forget.orgScope", { orgId: scopedOrgId })} tone={mode === "hard" ? "danger" : "neutral"} />
              ) : undefined
            }
          />

          <div className="grid gap-4 md:grid-cols-[1fr_180px_120px]">
            <Input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder={t("forget.placeholder")}
              className="h-12 border-border/70 bg-background/45 px-4 text-base"
              onKeyDown={(event) => event.key === "Enter" && handlePreview()}
            />
            <div className="grid grid-cols-2 gap-2 rounded-2xl border border-border/70 bg-background/45 p-1.5">
              <Button
                type="button"
                variant={mode === "logical" ? "default" : "ghost"}
                className="h-11 rounded-xl"
                onClick={() => setMode("logical")}
              >
                {t("forget.modes.logical")}
              </Button>
              <Button
                type="button"
                variant={mode === "hard" ? "destructive" : "ghost"}
                className="h-11 rounded-xl"
                onClick={() => setMode("hard")}
              >
                {t("forget.modes.hard")}
              </Button>
            </div>
            <Input
              value={limit}
              onChange={(event) => setLimit(event.target.value)}
              placeholder="10"
              className="h-12 border-border/70 bg-background/45 text-sm font-mono"
            />
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Button
              onClick={handlePreview}
              disabled={loadingPreview || executing || !query.trim()}
              className="gap-2"
            >
              {loadingPreview ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
              {t("forget.preview")}
            </Button>
            <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
              {mode === "hard" ? t("forget.modeHintHard") : t("forget.modeHintLogical")}
            </span>
          </div>
        </div>
      </Card>

      {error ? (
        <div className="text-sm text-destructive bg-destructive/10 rounded-lg px-4 py-2 border border-destructive/20">
          {error}
        </div>
      ) : null}

      {success ? (
        <div className="text-sm text-success bg-success/10 rounded-lg px-4 py-2 border border-success/20">
          {success}
        </div>
      ) : null}

      {preview ? (
        <div className="space-y-4">
          <Card className="rounded-[1.6rem] border border-white/8 bg-card/75 p-6">
            <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
              <div className="space-y-2">
                <p className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                  {t("forget.previewSummary")}
                </p>
                <p className="text-sm text-foreground/90">{preview.query}</p>
                <div className="flex flex-wrap gap-2 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                  <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1">{t("forget.summary.memories", { count: preview.summary.memory_unit_count })}</span>
                  <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1">{t("forget.summary.events", { count: preview.summary.event_count })}</span>
                  <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1">{preview.mode === "hard" ? t("forget.modes.hard") : t("forget.modes.logical")}</span>
                </div>
              </div>
              <Button
                onClick={handleExecute}
                disabled={executing}
                variant={preview.mode === "hard" ? "destructive" : "default"}
                className="gap-2"
              >
                {executing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                {t("forget.execute")}
              </Button>
            </div>
          </Card>

          <div className="grid gap-4 xl:grid-cols-2">
            <Card className="rounded-[1.6rem] border border-white/8 bg-card/70 p-5">
              <div className="mb-4 flex items-center justify-between">
                <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                  {t("forget.previewMemories")}
                </span>
                <span className="text-[10px] font-mono text-muted-foreground">
                  {preview.matched_units.length}
                </span>
              </div>
              <div className="space-y-3 max-h-[520px] overflow-y-auto pr-1">
                {preview.matched_units.length === 0 ? (
                  <div className="rounded-2xl border border-border/60 bg-card/40 p-4 text-sm text-muted-foreground">
                    {t("forget.emptyMemories")}
                  </div>
                ) : (
                  preview.matched_units.map((unit) => (
                    <div key={unit.id} className="rounded-2xl border border-border/60 bg-card/40 p-4">
                      <div className="mb-2 flex flex-wrap items-center gap-2 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                        <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1 font-mono">{unit.id.slice(0, 8)}</span>
                        <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1">L{unit.level}</span>
                        <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1">{unit.memory_type}</span>
                      </div>
                      <p className="text-sm leading-relaxed text-foreground/90">{unit.content}</p>
                      {unit.keywords.length > 0 ? (
                        <div className="mt-3 flex flex-wrap gap-1.5">
                          {unit.keywords.slice(0, 6).map((keyword) => (
                            <span
                              key={keyword}
                              className="rounded-full border border-border/70 bg-background/30 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider text-muted-foreground"
                            >
                              {keyword}
                            </span>
                          ))}
                        </div>
                      ) : null}
                      {unit.assets.length > 0 ? (
                        <div className="mt-3">
                          <MemoryAssets assets={unit.assets.slice(0, 2)} compact />
                        </div>
                      ) : null}
                    </div>
                  ))
                )}
              </div>
            </Card>

            <Card className="rounded-[1.6rem] border border-white/8 bg-card/70 p-5">
              <div className="mb-4 flex items-center justify-between">
                <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                  {t("forget.previewEvents")}
                </span>
                <span className="text-[10px] font-mono text-muted-foreground">
                  {preview.matched_events.length}
                </span>
              </div>
              <div className="space-y-3 max-h-[520px] overflow-y-auto pr-1">
                {preview.matched_events.length === 0 ? (
                  <div className="rounded-2xl border border-border/60 bg-card/40 p-4 text-sm text-muted-foreground">
                    {t("forget.emptyEvents")}
                  </div>
                ) : (
                  preview.matched_events.map((event) => (
                    <div key={event.id} className="rounded-2xl border border-border/60 bg-card/40 p-4">
                      <div className="mb-2 flex flex-wrap items-center gap-2 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                        <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1 font-mono">{event.id.slice(0, 8)}</span>
                        <span className="rounded-full border border-border/70 bg-background/35 px-2.5 py-1">{new Date(event.transaction_time).toLocaleString()}</span>
                      </div>
                      <p className="text-sm leading-relaxed text-foreground/90">{event.content}</p>
                    </div>
                  ))
                )}
              </div>
            </Card>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default function PlaygroundPage() {
  const t = useTranslations("Playground");
  return (
    <div className="relative mx-auto flex min-h-0 w-full max-w-6xl flex-1 flex-col overflow-hidden">
      <div className="mb-5 shrink-0">
        <DashboardHero
          icon={Sparkles}
          kicker={t("title")}
          title={t("title")}
          description={t("subtitle")}
        />
      </div>

      <Tabs defaultValue="chat" className="flex min-h-0 flex-1 flex-col">
        <TabsList className="mb-5 grid w-full max-w-lg shrink-0 grid-cols-3 rounded-2xl border border-white/8 bg-card/65 p-1.5">
          <TabsTrigger value="chat" className="gap-1.5 rounded-xl text-xs font-bold uppercase tracking-wider">
            <Bot className="w-3.5 h-3.5" />
            {t("tabs.chat")}
          </TabsTrigger>
          <TabsTrigger value="retrieve" className="gap-1.5 rounded-xl text-xs font-bold uppercase tracking-wider">
            <Search className="w-3.5 h-3.5" />
            {t("tabs.retrieve")}
          </TabsTrigger>
          <TabsTrigger value="forget" className="gap-1.5 rounded-xl text-xs font-bold uppercase tracking-wider">
            <Trash2 className="w-3.5 h-3.5" />
            {t("tabs.forget")}
          </TabsTrigger>
        </TabsList>

        <TabsContent value="chat" className="mt-0 min-h-0 flex-1 overflow-hidden data-[state=active]:flex data-[state=active]:flex-col">
          <ChatPanel />
        </TabsContent>

        <TabsContent value="retrieve" className="flex-1 min-h-0 mt-0 data-[state=active]:flex data-[state=active]:flex-col overflow-y-auto">
          <RetrievePanel />
        </TabsContent>

        <TabsContent value="forget" className="flex-1 min-h-0 mt-0 data-[state=active]:flex data-[state=active]:flex-col overflow-y-auto">
          <ForgetPanel />
        </TabsContent>
      </Tabs>
    </div>
  );
}
