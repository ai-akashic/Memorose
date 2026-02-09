"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import { getMustChangePassword, setMustChangePassword } from "@/lib/auth";
import type { AppConfig, VersionInfo } from "@/lib/types";
import {
  Settings as SettingsIcon,
  Shield,
  Info,
  Loader2,
  Check,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

function ConfigSection({ title, data }: { title: string; data: Record<string, unknown> }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-xs">{title}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-1.5">
        {Object.entries(data).map(([key, value]) => (
          <div key={key} className="flex justify-between text-xs">
            <span className="text-muted-foreground">{key}</span>
            <span className="font-mono text-right max-w-[60%] truncate">
              {typeof value === "object" ? JSON.stringify(value) : String(value)}
            </span>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function PasswordForm() {
  const [current, setCurrent] = useState("");
  const [newPw, setNewPw] = useState("");
  const [confirm, setConfirm] = useState("");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<{ type: "success" | "error"; text: string } | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setMessage(null);

    if (newPw !== confirm) {
      setMessage({ type: "error", text: "Passwords do not match" });
      return;
    }
    if (newPw.length < 8) {
      setMessage({ type: "error", text: "Password must be at least 8 characters" });
      return;
    }

    setLoading(true);
    try {
      await api.changePassword(current, newPw);
      setMustChangePassword(false);
      setMessage({ type: "success", text: "Password updated successfully" });
      setCurrent("");
      setNewPw("");
      setConfirm("");
    } catch (err: unknown) {
      setMessage({ type: "error", text: err instanceof Error ? err.message : "Failed" });
    } finally {
      setLoading(false);
    }
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-xs flex items-center gap-1.5">
          <Shield className="w-3.5 h-3.5 text-primary" />
          Change Password
        </CardTitle>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit} className="space-y-3 max-w-sm">
          <div className="space-y-1">
            <Label htmlFor="current-pw" className="text-xs text-muted-foreground">Current Password</Label>
            <Input
              id="current-pw"
              type="password"
              value={current}
              onChange={(e) => setCurrent(e.target.value)}
              required
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="new-pw" className="text-xs text-muted-foreground">New Password</Label>
            <Input
              id="new-pw"
              type="password"
              value={newPw}
              onChange={(e) => setNewPw(e.target.value)}
              required
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="confirm-pw" className="text-xs text-muted-foreground">Confirm New Password</Label>
            <Input
              id="confirm-pw"
              type="password"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
              required
            />
          </div>

          {message && (
            <div
              className={`text-sm rounded-lg px-3 py-2 ${
                message.type === "success"
                  ? "bg-success/10 text-success"
                  : "bg-destructive/10 text-destructive"
              }`}
            >
              {message.text}
            </div>
          )}

          <Button type="submit" disabled={loading} size="sm">
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Check className="w-4 h-4" />}
            Update Password
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

export default function SettingsPage() {
  const mustChange = typeof window !== "undefined" && getMustChangePassword();
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [version, setVersion] = useState<VersionInfo | null>(null);

  useEffect(() => {
    api.config().then(setConfig).catch(() => {});
    api.version().then(setVersion).catch(() => {});
  }, []);

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold tracking-tight">Settings</h1>

      <Tabs defaultValue={mustChange ? "security" : "config"}>
        <TabsList>
          <TabsTrigger value="config" className="gap-2">
            <SettingsIcon className="w-4 h-4" />
            Configuration
          </TabsTrigger>
          <TabsTrigger value="security" className="gap-2">
            <Shield className="w-4 h-4" />
            Security
          </TabsTrigger>
          <TabsTrigger value="about" className="gap-2">
            <Info className="w-4 h-4" />
            About
          </TabsTrigger>
        </TabsList>

        <TabsContent value="config" className="space-y-4">
          {config && (
            <>
              <ConfigSection title="Raft" data={config.raft as Record<string, unknown>} />
              <ConfigSection title="Worker" data={config.worker as Record<string, unknown>} />
              <ConfigSection title="LLM" data={config.llm as Record<string, unknown>} />
              <ConfigSection title="Storage" data={config.storage as Record<string, unknown>} />
            </>
          )}
        </TabsContent>

        <TabsContent value="security" className="space-y-4">
          {mustChange && (
            <div className="rounded-xl bg-warning/10 border border-warning/30 p-4 text-sm text-warning">
              You are using the default password. Please change it before continuing.
            </div>
          )}
          <PasswordForm />
        </TabsContent>

        <TabsContent value="about">
          {version && (
            <Card className="max-w-md">
              <CardHeader className="pb-3">
                <CardTitle className="text-xs">Memorose</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Version</span>
                  <span className="font-mono">{version.version}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Build Time</span>
                  <span className="font-mono">{version.build_time}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Features</span>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {version.features.map((f) => (
                      <Badge key={f} variant="secondary">{f}</Badge>
                    ))}
                  </div>
                </div>
              </CardContent>
            </Card>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
