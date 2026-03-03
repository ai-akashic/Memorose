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
  Key,
  AlertTriangle,
  Cpu,
  HardDrive,
  Brain,
  Server,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { motion } from "framer-motion";

const sectionIcon: Record<string, React.ElementType> = {
  Raft: Server,
  Worker: Cpu,
  LLM: Brain,
  Storage: HardDrive,
};

function ConfigSection({ title, data }: { title: string; data: Record<string, unknown> }) {
  const Icon = sectionIcon[title] ?? SettingsIcon;
  return (
    <Card className="glass-card border-white/[0.06]">
      <CardHeader className="pb-3 border-b border-white/5">
        <CardTitle className="text-xs flex items-center gap-2">
          <div className="p-1.5 rounded-md bg-primary/10 border border-primary/10">
            <Icon className="w-3 h-3 text-primary" />
          </div>
          <span className="uppercase tracking-widest text-muted-foreground/70 font-bold">{title}</span>
        </CardTitle>
      </CardHeader>
      <CardContent className="pt-3 space-y-1.5">
        {Object.entries(data).map(([key, value]) => (
          <div key={key} className="flex justify-between items-center text-xs py-1 border-b border-white/[0.03] last:border-0">
            <span className="text-muted-foreground font-medium">{key}</span>
            <span className="font-mono text-right max-w-[55%] truncate text-foreground/70 bg-white/5 px-2 py-0.5 rounded border border-white/5">
              {typeof value === "object" ? JSON.stringify(value) : String(value)}
            </span>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function PasswordForm({ onSuccess }: { onSuccess?: () => void }) {
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
      setCurrent(""); setNewPw(""); setConfirm("");
      setTimeout(() => onSuccess?.(), 1500);
    } catch (err: unknown) {
      setMessage({ type: "error", text: err instanceof Error ? err.message : "Failed" });
    } finally {
      setLoading(false);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      {[
        { id: "current-pw", label: "Current Password", value: current, onChange: setCurrent },
        { id: "new-pw", label: "New Password", value: newPw, onChange: setNewPw },
        { id: "confirm-pw", label: "Confirm New Password", value: confirm, onChange: setConfirm },
      ].map(({ id, label, value, onChange }) => (
        <div key={id} className="space-y-1">
          <Label htmlFor={id} className="text-xs text-muted-foreground">{label}</Label>
          <Input
            id={id}
            type="password"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            required
            className="bg-white/5 border-white/10 focus:border-primary/40"
          />
        </div>
      ))}
      {message && (
        <div className={`text-sm rounded-lg px-3 py-2 border ${
          message.type === "success"
            ? "bg-success/10 text-success border-success/20"
            : "bg-destructive/10 text-destructive border-destructive/20"
        }`}>
          {message.text}
        </div>
      )}
      <Button type="submit" disabled={loading} size="sm" className="w-full">
        {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Check className="w-4 h-4" />}
        Update Password
      </Button>
    </form>
  );
}

export default function SettingsPage() {
  const mustChange = typeof window !== "undefined" && getMustChangePassword();
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [version, setVersion] = useState<VersionInfo | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);

  useEffect(() => {
    api.config().then(setConfig).catch(() => {});
    api.version().then(setVersion).catch(() => {});
  }, []);

  return (
    <div className="space-y-6 relative max-w-3xl">
      <div className="absolute top-0 right-0 w-[400px] h-[200px] blob-bg opacity-15 pointer-events-none -z-10 mix-blend-screen" />

      <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
        <h1 className="text-3xl font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-b from-white to-white/60">
          Settings
        </h1>
        <p className="text-muted-foreground mt-1 text-sm">System configuration and security</p>
      </motion.div>

      <Tabs defaultValue={mustChange ? "security" : "config"}>
        <TabsList className="bg-white/[0.04] border border-white/[0.06]">
          <TabsTrigger value="config" className="gap-2 text-xs">
            <SettingsIcon className="w-3.5 h-3.5" />
            Configuration
          </TabsTrigger>
          <TabsTrigger value="security" className="gap-2 text-xs">
            <Shield className="w-3.5 h-3.5" />
            Security
          </TabsTrigger>
          <TabsTrigger value="about" className="gap-2 text-xs">
            <Info className="w-3.5 h-3.5" />
            About
          </TabsTrigger>
        </TabsList>

        <TabsContent value="config" className="space-y-3 mt-4">
          {config ? (
            <>
              <ConfigSection title="Raft" data={config.raft as Record<string, unknown>} />
              <ConfigSection title="Worker" data={config.worker as Record<string, unknown>} />
              <ConfigSection title="LLM" data={config.llm as Record<string, unknown>} />
              <ConfigSection title="Storage" data={config.storage as Record<string, unknown>} />
            </>
          ) : (
            <div className="space-y-3">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="glass-card rounded-xl h-24 opacity-20" />
              ))}
            </div>
          )}
        </TabsContent>

        <TabsContent value="security" className="space-y-4 mt-4">
          {mustChange && (
            <div className="glass-card rounded-xl border border-warning/30 p-4 flex items-start gap-3">
              <AlertTriangle className="w-4 h-4 text-warning shrink-0 mt-0.5" />
              <p className="text-sm text-warning">
                You are using the default password. Please change it before continuing.
              </p>
            </div>
          )}

          <Card className="glass-card border-white/[0.06]">
            <CardHeader className="pb-3 border-b border-white/5">
              <CardTitle className="text-xs flex items-center gap-2">
                <div className="p-1.5 rounded-md bg-primary/10 border border-primary/10">
                  <Shield className="w-3 h-3 text-primary" />
                </div>
                <span className="uppercase tracking-widest text-muted-foreground/70 font-bold">Password Management</span>
              </CardTitle>
            </CardHeader>
            <CardContent className="pt-4">
              <p className="text-sm text-muted-foreground mb-4">
                Update your account password to keep your account secure.
              </p>
              <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
                <DialogTrigger asChild>
                  <Button variant="outline" size="sm" className="border-white/10 hover:bg-white/5">
                    <Key className="w-4 h-4 mr-2" />
                    Change Password
                  </Button>
                </DialogTrigger>
                <DialogContent className="sm:max-w-[425px]">
                  <DialogHeader>
                    <DialogTitle>Change Password</DialogTitle>
                    <DialogDescription>
                      Enter your current password and choose a new one. Minimum 8 characters.
                    </DialogDescription>
                  </DialogHeader>
                  <PasswordForm onSuccess={() => setDialogOpen(false)} />
                </DialogContent>
              </Dialog>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="about" className="mt-4">
          {version && (
            <Card className="glass-card border-white/[0.06]">
              <CardHeader className="pb-3 border-b border-white/5">
                <CardTitle className="text-xs flex items-center gap-2">
                  <div className="p-1.5 rounded-md bg-primary/10 border border-primary/10">
                    <Info className="w-3 h-3 text-primary" />
                  </div>
                  <span className="uppercase tracking-widest text-muted-foreground/70 font-bold">Memorose</span>
                </CardTitle>
              </CardHeader>
              <CardContent className="pt-4 space-y-3">
                {[
                  { label: "Version", value: version.version },
                  { label: "Build Time", value: version.build_time },
                ].map(({ label, value }) => (
                  <div key={label} className="flex justify-between items-center text-sm border-b border-white/[0.03] pb-2 last:border-0 last:pb-0">
                    <span className="text-muted-foreground">{label}</span>
                    <span className="font-mono text-foreground/70 bg-white/5 px-2 py-0.5 rounded border border-white/5 text-xs">{value}</span>
                  </div>
                ))}
                <div>
                  <span className="text-sm text-muted-foreground">Features</span>
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    {version.features.map((f) => (
                      <Badge key={f} variant="outline" className="text-xs bg-primary/5 border-primary/20 text-primary/80">{f}</Badge>
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
