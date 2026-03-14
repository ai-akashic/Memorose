"use client";

import { useState } from "react";
import { Copy, Plus, KeyRound, Terminal, Trash2 } from "lucide-react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

const MOCK_APPS = [
  { id: "app_a1b2c3", name: "Customer Support Bot", description: "Production memory layer for support channels.", keys: 2 },
  { id: "app_x9y8z7", name: "Internal Coding Assistant", description: "Dev team knowledge graph.", keys: 1 },
];

const MOCK_KEYS = [
  { id: "key_1", name: "Production Key", key: "sk_live_8f92a1...4b2e", created: "2026-03-01", lastUsed: "2 mins ago" },
  { id: "key_2", name: "Developer Testing", key: "sk_test_11x9f0...9q8w", created: "2026-03-10", lastUsed: "Never" },
];

export default function AppsPage() {
  const [selectedApp, setSelectedApp] = useState(MOCK_APPS[0]);

  return (
    <div className="flex flex-col h-full gap-8">
      {/* Header Section */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold tracking-tight text-foreground">Applications</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Manage your agent memory contexts and API keys.
          </p>
        </div>
        <Button className="h-9 gap-2">
          <Plus className="w-4 h-4" />
          Create App
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Left Column: Apps List */}
        <div className="col-span-1 space-y-4">
          <div className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
            Your Apps
          </div>
          <div className="space-y-2">
            {MOCK_APPS.map((app) => (
              <button
                key={app.id}
                onClick={() => setSelectedApp(app)}
                className={`w-full text-left p-4 rounded-lg border transition-all duration-200 ${
                  selectedApp.id === app.id
                    ? "bg-muted border-border"
                    : "bg-card border-border/50 hover:border-border hover:bg-muted/50"
                }`}
              >
                <div className="font-semibold text-[14px] text-foreground">{app.name}</div>
                <div className="text-xs font-mono text-muted-foreground mt-1">{app.id}</div>
              </button>
            ))}
          </div>
        </div>

        {/* Right Column: App Detail & API Keys */}
        <div className="col-span-2 space-y-6">
          <Card className="glass-card p-6">
            <div className="flex items-start justify-between">
              <div>
                <h2 className="text-lg font-bold">{selectedApp.name}</h2>
                <p className="text-sm text-muted-foreground mt-1">{selectedApp.description}</p>
                <div className="flex items-center gap-2 mt-4">
                  <span className="text-[11px] uppercase tracking-widest text-muted-foreground font-medium">App ID</span>
                  <code className="text-xs font-mono bg-muted px-2 py-1 rounded border border-border">
                    {selectedApp.id}
                  </code>
                  <Button variant="ghost" size="icon" className="h-6 w-6 text-muted-foreground">
                    <Copy className="w-3 h-3" />
                  </Button>
                </div>
              </div>
            </div>
          </Card>

          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground flex items-center gap-2">
                <KeyRound className="w-3.5 h-3.5" />
                API Keys
              </div>
              <Button variant="outline" size="sm" className="h-8 text-xs">
                Generate Key
              </Button>
            </div>

            <Card className="glass-card overflow-hidden">
              <table className="w-full text-left border-collapse">
                <thead>
                  <tr className="border-b border-border bg-muted/30">
                    <th className="px-4 py-3 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Name</th>
                    <th className="px-4 py-3 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Secret Key</th>
                    <th className="px-4 py-3 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Last Used</th>
                    <th className="px-4 py-3 text-right"></th>
                  </tr>
                </thead>
                <tbody className="text-sm">
                  {MOCK_KEYS.map((key) => (
                    <tr key={key.id} className="border-b border-border last:border-0 hover:bg-muted/20 transition-colors">
                      <td className="px-4 py-3 font-medium">{key.name}</td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <code className="text-xs font-mono text-muted-foreground">{key.key}</code>
                          <Button variant="ghost" size="icon" className="h-5 w-5 opacity-50 hover:opacity-100">
                            <Copy className="w-3 h-3" />
                          </Button>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-muted-foreground text-xs">{key.lastUsed}</td>
                      <td className="px-4 py-3 text-right">
                        <Button variant="ghost" size="icon" className="h-7 w-7 text-muted-foreground hover:text-destructive hover:bg-destructive/10">
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Card>

            {/* Quick Start Guide */}
            <Card className="glass-card p-5 mt-4 border-primary/20 bg-primary/5">
              <div className="flex items-center gap-2 text-primary font-medium text-sm mb-3">
                <Terminal className="w-4 h-4" />
                Quick Integration
              </div>
              <pre className="text-xs font-mono bg-background border border-border rounded-md p-4 overflow-x-auto text-muted-foreground">
<span className="text-primary">curl</span> -X POST https://api.memorose.io/v1/users/dylan/streams/sys-01/events \
  -H <span className="text-green-400">&quot;Authorization: Bearer sk_live_8f92a1...4b2e&quot;</span> \
  -H <span className="text-green-400">&quot;Content-Type: application/json&quot;</span> \
  -d <span className="text-yellow-200">{"'{ \"content\": \"I prefer dark mode UI.\" }'"}</span>
              </pre>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}