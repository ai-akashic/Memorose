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
import { Loader2, Send, Bot, Sparkles, Search, SlidersHorizontal } from "lucide-react";
import { cn } from "@/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import type { RetrieveResponse } from "@/lib/types";
import { useTranslations } from "next-intl";
import { DashboardHero, DashboardStatRail } from "@/components/dashboard-chrome";
import { MemoryAssets } from "@/components/memory-assets";

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
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

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="flex flex-col flex-1 min-h-0">
      {scopedOrgId && (
        <div className="mb-4 self-end rounded-lg border border-border/70 bg-background/50 px-3 py-2 text-[11px] font-mono text-muted-foreground">
          {t("chat.orgScope", { orgId: scopedOrgId })}
        </div>
      )}

      <Card className="flex-1 flex flex-col overflow-hidden glass-card rounded-3xl relative">

        <div className="flex-1 overflow-y-auto p-8 z-10 scroll-smooth" ref={scrollRef}>
          <div className="space-y-8 max-w-3xl mx-auto">
            <AnimatePresence>
              {messages.length === 0 && (
                <motion.div
                  initial={{ opacity: 0, scale: 0.98 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.98 }}
                  className="flex flex-col items-center justify-center h-[50vh] text-muted-foreground"
                >
                  <div className="w-20 h-20 rounded-[40px] bg-card border border-border flex items-center justify-center mb-8">
                    <Bot className="w-10 h-10 opacity-30" />
                  </div>
                  <h3 className="text-xl font-bold tracking-tight text-foreground/80 mb-3 uppercase tracking-[0.2em]">{t("chat.welcomeTitle")}</h3>
                  <p className="text-sm opacity-50 max-w-xs text-center leading-relaxed">
                    {t("chat.welcomeDesc")}
                  </p>
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
                      "max-w-[80%] rounded-[20px] px-6 py-4 text-[14.5px] leading-relaxed backdrop-blur-md",
                      message.role === "user"
                        ? "bg-primary text-primary-foreground shadow-[0_8px_24px_rgba(255,92,92,0.25)] rounded-tr-sm"
                        : "bg-white/[0.03] border border-white/[0.05] rounded-tl-sm text-foreground/90 font-medium shadow-sm"
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

        {/* Input Area */}
        <div className="p-6 z-20">
          <div className="max-w-3xl mx-auto relative group">
            <div className="absolute -inset-1 bg-gradient-to-r from-primary/20 via-primary/10 to-transparent rounded-3xl blur-xl opacity-20 group-hover:opacity-40 transition duration-1000" />
            <div className="relative flex gap-4 items-center glass-card rounded-2xl p-2 shadow-[0_8px_32px_rgba(0,0,0,0.2)]">
              <Input
                type="text"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyPress={handleKeyPress}
                placeholder={t("chat.placeholder")}
                disabled={loading}
                className="flex-1 bg-transparent border-none focus-visible:ring-0 text-[15px] h-12 px-4 placeholder:text-muted-foreground/30"
              />
              <Button
                onClick={handleSend}
                disabled={loading || !input.trim()}
                size="icon"
                className={cn(
                  "h-10 w-10 rounded-xl transition-all duration-300 mr-1",
                  input.trim() ? "bg-primary text-primary-foreground shadow-[0_0_15px_rgba(255,92,92,0.4)] hover:scale-105" : "bg-white/5 text-muted-foreground/40 hover:bg-white/10"
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
      {/* Context + Query */}
      <Card className="glass-card p-6 rounded-3xl">
        <div className="space-y-6">
          <div className="grid grid-cols-1 gap-6">
            <div className="space-y-2 max-w-sm">
              <label className="px-1 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("retrieve.streamId")}</label>
              <Input
                list="stream-suggestions"
                value={streamId}
                onChange={(e) => setStreamId(e.target.value)}
                className="h-11 text-[13px] font-mono bg-card border-border"
              />
              <datalist id="stream-suggestions">
                {streams.map(s => <option key={s} value={s} />)}
              </datalist>
            </div>
          </div>

          {scopedOrgId && (
            <div className="rounded-lg border border-border/70 bg-background/50 px-3 py-2 text-[11px] font-mono text-muted-foreground">
              {t("retrieve.orgScope", { orgId: scopedOrgId })}
            </div>
          )}

          <div className="flex gap-4">
            <Input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={t("retrieve.queryPlaceholder")}
              className="flex-1 h-14 bg-card border-border text-base px-6 placeholder:text-muted-foreground/10"
              onKeyDown={(e) => e.key === "Enter" && handleRetrieve()}
            />
            <Button
              onClick={handleRetrieve}
              disabled={loading || !query.trim() || !streamId.trim()}
              className="h-14 px-8 gap-3 rounded-2xl text-[11px] font-medium uppercase tracking-widest text-muted-foreground"
            >
              {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Search className="w-5 h-5" />}
              {t("retrieve.button")}
            </Button>
          </div>
        </div>
      </Card>

      {/* Advanced params */}
      <Card className="glass-card p-6 rounded-3xl">
        <div className="space-y-4">
          <div className="flex items-center gap-2 mb-2">
            <SlidersHorizontal className="w-3.5 h-3.5 text-muted-foreground/40" />
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("retrieve.parameters")}</span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
            {[
              { label: t("retrieve.params.limit"), value: limit, setter: setLimit, placeholder: "10" },
              { label: t("retrieve.params.score"), value: minScore, setter: setMinScore, placeholder: "0.0" },
              { label: t("retrieve.params.depth"), value: graphDepth, setter: setGraphDepth, placeholder: "1" },
              { label: t("retrieve.params.from"), value: validTimeStart, setter: setValidTimeStart, placeholder: "ISO" },
              { label: t("retrieve.params.to"), value: validTimeEnd, setter: setValidTimeEnd, placeholder: "ISO" },
              { label: t("retrieve.params.asOf"), value: asOf, setter: setAsOf, placeholder: "NOW" },
            ].map((p) => (
              <div key={p.label} className="space-y-2">
                <label className="px-1 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{p.label}</label>
                <Input value={p.value} onChange={(e) => p.setter(e.target.value)} placeholder={p.placeholder} className="h-10 text-[11px] font-mono bg-card border-border" />
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
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium">{t("retrieve.results", { count: results.results.length })}</span>
            <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{results.query_time_ms.toFixed(1)}ms</span>
          </div>
          {results.results.length === 0 ? (
            <Card>
              <div className="p-6 text-center text-muted-foreground text-sm">{t("retrieve.noResults")}</div>
            </Card>
          ) : (
            results.results.map((r, i) => (
              <Card key={r.unit.id || i} className="glass-card">
                <div className="p-4">
                  <div className="flex items-start justify-between gap-3 mb-2">
                    <p className="text-sm leading-relaxed flex-1">{r.unit.content}</p>
                    <div className="shrink-0 text-right">
                      <div className="text-sm font-mono font-semibold text-primary">{(r.score * 100).toFixed(1)}%</div>
                      <div className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("retrieve.scoreLabel")}</div>
                    </div>
                  </div>
                  <div className="flex items-center gap-3 flex-wrap text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
                    <span className="font-mono">{r.unit.id.substring(0, 8)}</span>
                    <span>L{r.unit.level}</span>
                    {r.unit.memory_type && <span className="capitalize">{r.unit.memory_type}</span>}
                    {r.unit.keywords.slice(0, 4).map((kw) => (
                      <span key={kw} className="bg-muted/40 rounded px-1.5 py-0.5">{kw}</span>
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

export default function PlaygroundPage() {
  const t = useTranslations("Playground");
  return (
    <div className="flex-1 flex flex-col min-h-0 w-full max-w-5xl mx-auto relative">
      <div className="mb-4 shrink-0">
        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.5 }}
        >
          <DashboardHero
            icon={Sparkles}
            kicker={t("title")}
            title={t("title")}
            description={t("subtitle")}
          >
            <DashboardStatRail
              items={[
                { label: t("tabs.chat"), value: "Live", tone: "primary" },
                { label: t("tabs.retrieve"), value: "Search", tone: "success" },
              ]}
            />
          </DashboardHero>
        </motion.div>
      </div>

      <Tabs defaultValue="chat" className="flex-1 flex flex-col min-h-0">
        <TabsList className="grid w-full max-w-xs grid-cols-2 mb-4 shrink-0">
          <TabsTrigger value="chat" className="gap-1.5">
            <Bot className="w-3.5 h-3.5" />
            {t("tabs.chat")}
          </TabsTrigger>
          <TabsTrigger value="retrieve" className="gap-1.5">
            <Search className="w-3.5 h-3.5" />
            {t("tabs.retrieve")}
          </TabsTrigger>
        </TabsList>

        <TabsContent value="chat" className="flex-1 min-h-0 mt-0 data-[state=active]:flex data-[state=active]:flex-col">
          <ChatPanel />
        </TabsContent>

        <TabsContent value="retrieve" className="flex-1 min-h-0 mt-0 data-[state=active]:flex data-[state=active]:flex-col overflow-y-auto">
          <RetrievePanel />
        </TabsContent>
      </Tabs>
    </div>
  );
}
