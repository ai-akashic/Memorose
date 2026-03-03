"use client";

import { useState, useRef, useEffect } from "react";
import { api } from "@/lib/api";
import { getToken } from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Loader2, Send, User, Bot, Sparkles, Search, SlidersHorizontal } from "lucide-react";
import { cn } from "@/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import type { RetrieveResponse } from "@/lib/types";

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
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [streaming, setStreaming] = useState(false);
  const [userId, setUserId] = useState("demo-user");
  const [appId, setAppId] = useState("playground");
  const scrollRef = useRef<HTMLDivElement>(null);
  const streamingMessageRef = useRef<string>("");

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, streaming]);

  const handleSend = async () => {
    if (!input.trim() || loading) return;

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
        user_id: userId,
        app_id: appId,
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
          user_id: userId,
          app_id: appId,
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
          content: `Error: ${error instanceof Error ? error.message : "Unknown error"}`,
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
    <div className="flex flex-col h-full">
      {/* Config bar */}
      <motion.div
        initial={{ opacity: 0, x: 20 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.5, delay: 0.1 }}
        className="flex gap-4 p-3 glass-card rounded-xl shadow-lg border-white/5 mb-4 self-end"
      >
        <div className="flex flex-col gap-1">
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold px-1">Session Entity</label>
          <Input
            type="text"
            value={userId}
            onChange={(e) => setUserId(e.target.value)}
            placeholder="User ID"
            className="w-32 h-8 text-xs font-mono bg-black/20 border-white/10"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold px-1">Context Scope</label>
          <Input
            type="text"
            value={appId}
            onChange={(e) => setAppId(e.target.value)}
            placeholder="App ID"
            className="w-32 h-8 text-xs font-mono bg-black/20 border-white/10"
          />
        </div>
      </motion.div>

      <Card className="flex-1 flex flex-col overflow-hidden glass-card rounded-2xl border-white/10 shadow-2xl relative">
        <div className="absolute inset-0 bg-gradient-to-b from-transparent to-black/20 pointer-events-none" />

        <div className="flex-1 overflow-y-auto p-6 z-10 scroll-smooth" ref={scrollRef}>
          <div className="space-y-6 max-w-3xl mx-auto">
            <AnimatePresence>
              {messages.length === 0 && (
                <motion.div
                  initial={{ opacity: 0, scale: 0.95 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.95 }}
                  className="flex flex-col items-center justify-center h-[50vh] text-muted-foreground"
                >
                  <div className="w-16 h-16 rounded-3xl bg-white/[0.03] backdrop-blur-xl border border-white/10 flex items-center justify-center mb-6 shadow-[inset_0_1px_0_rgba(255,255,255,0.1)]">
                    <Bot className="w-8 h-8 opacity-40" />
                  </div>
                  <h3 className="text-lg font-medium text-foreground/80 mb-2">Initialize Cognitive Stream</h3>
                  <p className="text-sm opacity-60 max-w-xs text-center">
                    All dialogs are persistently encoded into the L0/L1/L2 memory hierarchy in real-time.
                  </p>
                </motion.div>
              )}
            </AnimatePresence>

            <AnimatePresence initial={false}>
              {messages.map((message) => (
                <motion.div
                  key={message.id}
                  initial={{ opacity: 0, y: 15, scale: 0.98 }}
                  animate={{ opacity: 1, y: 0, scale: 1 }}
                  transition={{ type: "spring", stiffness: 400, damping: 30 }}
                  className={cn(
                    "flex gap-4",
                    message.role === "user" ? "justify-end" : "justify-start"
                  )}
                >
                  {message.role === "assistant" && (
                    <div className="w-8 h-8 rounded-full bg-white/[0.05] border border-white/10 flex items-center justify-center shrink-0 shadow-sm mt-1">
                      <Bot className="w-4 h-4 text-primary" />
                    </div>
                  )}
                  <div
                    className={cn(
                      "max-w-[75%] rounded-2xl px-5 py-3.5 shadow-sm text-sm leading-relaxed",
                      message.role === "user"
                        ? "bg-primary text-primary-foreground shadow-[0_0_15px_rgba(255,255,255,0.1)] rounded-tr-sm"
                        : "glass-card bg-black/40 border-white/5 rounded-tl-sm text-foreground/90"
                    )}
                  >
                    <p className="whitespace-pre-wrap">{message.content}</p>
                    {message.role === "assistant" && message.content === "" && !streaming && (
                      <span className="opacity-50 italic">Error retrieving response.</span>
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
        <div className="p-4 z-20">
          <div className="max-w-3xl mx-auto relative group">
            <div className="absolute -inset-1 bg-gradient-to-r from-primary/20 via-white/5 to-primary/20 rounded-xl blur opacity-30 group-hover:opacity-60 transition duration-1000 group-hover:duration-200" />
            <div className="relative flex gap-3 items-center glass-card bg-black/60 rounded-xl p-2 border-white/10">
              <Input
                type="text"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyPress={handleKeyPress}
                placeholder="Message the agent..."
                disabled={loading}
                className="flex-1 bg-transparent border-none shadow-none focus-visible:ring-0 text-base h-12 px-4 placeholder:text-muted-foreground/50"
              />
              <Button
                onClick={handleSend}
                disabled={loading || !input.trim()}
                size="icon"
                className={cn(
                  "h-10 w-10 rounded-lg transition-all duration-300",
                  input.trim() ? "bg-primary text-primary-foreground shadow-[0_0_15px_rgba(255,255,255,0.3)]" : "bg-white/5 text-muted-foreground hover:bg-white/10"
                )}
              >
                {loading ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Send className="w-4 h-4" />
                )}
              </Button>
            </div>
          </div>
          <div className="text-center mt-3">
            <p className="text-[10px] text-muted-foreground/60 uppercase tracking-widest">
              Secured by Memorose Engine
            </p>
          </div>
        </div>
      </Card>
    </div>
  );
}

function RetrievePanel() {
  const [userId, setUserId] = useState("demo-user");
  const [appId, setAppId] = useState("playground");
  const [streamId, setStreamId] = useState("chat");
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

  async function handleRetrieve() {
    if (!query.trim()) return;
    setLoading(true);
    setError(null);
    setResults(null);
    try {
      const body = {
        query: query.trim(),
        ...(limit ? { limit: Number(limit) } : {}),
        ...(minScore ? { min_score: Number(minScore) } : {}),
        ...(graphDepth ? { graph_depth: Number(graphDepth) } : {}),
        ...(validTimeStart ? { valid_time_start: validTimeStart } : {}),
        ...(validTimeEnd ? { valid_time_end: validTimeEnd } : {}),
        ...(asOf ? { as_of: asOf } : {}),
      };
      const res = await api.retrieve(userId, appId, streamId, body);
      setResults(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-4">
      {/* Context + Query */}
      <Card className="glass-card border-white/10">
        <div className="p-4 space-y-4">
          <div className="grid grid-cols-3 gap-3">
            <div className="space-y-1">
              <label className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold">User ID</label>
              <Input value={userId} onChange={(e) => setUserId(e.target.value)} className="h-8 text-xs font-mono bg-black/20 border-white/10" />
            </div>
            <div className="space-y-1">
              <label className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold">App ID</label>
              <Input value={appId} onChange={(e) => setAppId(e.target.value)} className="h-8 text-xs font-mono bg-black/20 border-white/10" />
            </div>
            <div className="space-y-1">
              <label className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold">Stream ID</label>
              <Input value={streamId} onChange={(e) => setStreamId(e.target.value)} className="h-8 text-xs font-mono bg-black/20 border-white/10" />
            </div>
          </div>

          <div className="flex gap-3">
            <Input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Enter retrieval query..."
              className="flex-1 h-10 bg-black/20 border-white/10"
              onKeyDown={(e) => e.key === "Enter" && handleRetrieve()}
            />
            <Button onClick={handleRetrieve} disabled={loading || !query.trim()} className="h-10 gap-1.5">
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
              Retrieve
            </Button>
          </div>
        </div>
      </Card>

      {/* Advanced params */}
      <Card className="glass-card border-white/10">
        <div className="p-4">
          <div className="flex items-center gap-1.5 mb-3">
            <SlidersHorizontal className="w-3.5 h-3.5 text-muted-foreground" />
            <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Advanced Parameters</span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground">Limit</label>
              <Input value={limit} onChange={(e) => setLimit(e.target.value)} placeholder="10" className="h-8 text-xs font-mono bg-black/20 border-white/10" />
            </div>
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground">Min Score</label>
              <Input value={minScore} onChange={(e) => setMinScore(e.target.value)} placeholder="0.0–1.0" className="h-8 text-xs font-mono bg-black/20 border-white/10" />
            </div>
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground">Graph Depth</label>
              <Input value={graphDepth} onChange={(e) => setGraphDepth(e.target.value)} placeholder="1–5" className="h-8 text-xs font-mono bg-black/20 border-white/10" />
            </div>
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground">Valid From</label>
              <Input value={validTimeStart} onChange={(e) => setValidTimeStart(e.target.value)} placeholder="ISO 8601" className="h-8 text-xs font-mono bg-black/20 border-white/10" />
            </div>
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground">Valid To</label>
              <Input value={validTimeEnd} onChange={(e) => setValidTimeEnd(e.target.value)} placeholder="ISO 8601" className="h-8 text-xs font-mono bg-black/20 border-white/10" />
            </div>
            <div className="space-y-1">
              <label className="text-[10px] text-muted-foreground">As Of</label>
              <Input value={asOf} onChange={(e) => setAsOf(e.target.value)} placeholder="ISO 8601" className="h-8 text-xs font-mono bg-black/20 border-white/10" />
            </div>
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
            <span className="text-sm font-medium">{results.results.length} results</span>
            <span className="text-xs text-muted-foreground">{results.query_time_ms.toFixed(1)}ms</span>
          </div>
          {results.results.length === 0 ? (
            <Card>
              <div className="p-6 text-center text-muted-foreground text-sm">No matching memories found</div>
            </Card>
          ) : (
            results.results.map((r, i) => (
              <Card key={r.unit.id || i} className="glass-card border-white/10">
                <div className="p-4">
                  <div className="flex items-start justify-between gap-3 mb-2">
                    <p className="text-sm leading-relaxed flex-1">{r.unit.content}</p>
                    <div className="shrink-0 text-right">
                      <div className="text-sm font-mono font-semibold text-primary">{(r.score * 100).toFixed(1)}%</div>
                      <div className="text-[10px] text-muted-foreground">score</div>
                    </div>
                  </div>
                  <div className="flex items-center gap-3 text-[10px] text-muted-foreground flex-wrap">
                    <span className="font-mono">{r.unit.id.substring(0, 8)}</span>
                    <span>L{r.unit.level}</span>
                    {r.unit.memory_type && <span className="capitalize">{r.unit.memory_type}</span>}
                    {r.unit.keywords.slice(0, 4).map((kw) => (
                      <span key={kw} className="bg-muted/40 rounded px-1.5 py-0.5">{kw}</span>
                    ))}
                  </div>
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
  return (
    <div className="flex flex-col h-[calc(100vh-6rem)] max-w-5xl mx-auto w-full relative">
      <div className="mb-4">
        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.5 }}
        >
          <div className="flex items-center gap-2 mb-1">
            <Sparkles className="w-5 h-5 text-primary" />
            <h1 className="text-2xl font-semibold tracking-tight">Interactive Canvas</h1>
          </div>
          <p className="text-sm text-muted-foreground">
            Engage with your memory-augmented agent or retrieve memories directly
          </p>
        </motion.div>
      </div>

      <Tabs defaultValue="chat" className="flex-1 flex flex-col min-h-0">
        <TabsList className="grid w-full max-w-xs grid-cols-2 mb-4">
          <TabsTrigger value="chat" className="gap-1.5">
            <Bot className="w-3.5 h-3.5" />
            Chat
          </TabsTrigger>
          <TabsTrigger value="retrieve" className="gap-1.5">
            <Search className="w-3.5 h-3.5" />
            Retrieve
          </TabsTrigger>
        </TabsList>

        <TabsContent value="chat" className="flex-1 flex flex-col min-h-0 mt-0">
          <ChatPanel />
        </TabsContent>

        <TabsContent value="retrieve" className="flex-1 overflow-y-auto mt-0">
          <RetrievePanel />
        </TabsContent>
      </Tabs>
    </div>
  );
}
