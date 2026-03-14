"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import { getMustChangePassword, setMustChangePassword } from "@/lib/auth";
import type { AppConfig } from "@/lib/types";
import {
  Settings as SettingsIcon,
  Shield,
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
import { Card } from "@/components/ui/card";
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

function ConfigSection({ title, data, delay }: { title: string; data: Record<string, unknown>; delay: number }) {
  const Icon = sectionIcon[title] ?? SettingsIcon;
  return (
    <motion.div
      initial={{ opacity: 0, y: 15 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay }}
    >
      <div className="mb-8">
        <div className="flex items-center gap-2 mb-4">
          <Icon className="w-4 h-4 text-muted-foreground" />
          <h2 className="text-[13px] font-semibold text-foreground tracking-tight">{title} Configuration</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {Object.entries(data).map(([key, value]) => (
            <Card key={key} className="glass-card flex flex-col p-4 justify-between h-24 hover:bg-muted/30 transition-colors">
              <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground truncate">
                {key.replace(/_/g, ' ')}
              </span>
              <span className="font-mono text-sm text-foreground truncate mt-2">
                {typeof value === "object" ? JSON.stringify(value) : String(value)}
              </span>
            </Card>
          ))}
        </div>
      </div>
    </motion.div>
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
    <form onSubmit={handleSubmit} className="space-y-4 py-2">
      {[
        { id: "current-pw", label: "Current Password", value: current, onChange: setCurrent },
        { id: "new-pw", label: "New Password", value: newPw, onChange: setNewPw },
        { id: "confirm-pw", label: "Confirm New Password", value: confirm, onChange: setConfirm },
      ].map(({ id, label, value, onChange }) => (
        <div key={id} className="space-y-1.5">
          <Label htmlFor={id} className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{label}</Label>
          <Input
            id={id}
            type="password"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            required
            className="bg-background border-border h-9"
          />
        </div>
      ))}
      {message && (
        <motion.div 
          initial={{ opacity: 0, scale: 0.98 }} animate={{ opacity: 1, scale: 1 }}
          className={`text-xs font-medium rounded-md px-3 py-2 border flex items-center gap-2 ${
          message.type === "success"
            ? "bg-success/10 text-success border-success/20"
            : "bg-destructive/10 text-destructive border-destructive/20"
        }`}>
          {message.type === "success" ? <Check className="w-3.5 h-3.5" /> : <AlertTriangle className="w-3.5 h-3.5" />}
          {message.text}
        </motion.div>
      )}
      <div className="pt-2">
        <Button type="submit" disabled={loading} className="w-full h-9">
          {loading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Shield className="w-4 h-4 mr-2" />}
          Update Password
        </Button>
      </div>
    </form>
  );
}

export default function SettingsPage() {
  const mustChange = typeof window !== "undefined" && getMustChangePassword();
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);

  useEffect(() => {
    api.config().then(setConfig).catch(() => {});
  }, []);

  return (
    <div className="space-y-8 relative max-w-4xl mx-auto pb-12">
      <div className="absolute top-0 right-0 w-[500px] h-[300px] blob-bg opacity-15 pointer-events-none -z-10" />

      <motion.div 
        initial={{ opacity: 0, y: 10 }} 
        animate={{ opacity: 1, y: 0 }} 
        transition={{ duration: 0.5 }}
        className="flex flex-col gap-2"
      >
        <div className="flex items-center gap-3">
          <div className="p-2.5 bg-card rounded-xl border border-border">
            <SettingsIcon className="w-6 h-6 text-foreground/80" />
          </div>
          <div>
            <h1 className="text-3xl font-extrabold tracking-tight bg-clip-text text-transparent">
              Settings
            </h1>
            <p className="text-muted-foreground mt-1 text-sm font-medium">Manage system configuration and security credentials.</p>
          </div>
        </div>
      </motion.div>

      {mustChange && (
        <motion.div initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }} className="glass-card rounded-xl border-warning/30 p-5 flex items-start gap-4 bg-warning/5">
          <div className="p-2 bg-warning/20 rounded-lg">
            <AlertTriangle className="w-5 h-5 text-warning" />
          </div>
          <div className="space-y-1">
            <h4 className="font-semibold text-warning">Action Required</h4>
            <p className="text-sm text-warning/80">
              You are currently using the default installation password. For security reasons, please change your password immediately.
            </p>
          </div>
        </motion.div>
      )}

      <div className="space-y-6">
        <div className="flex items-center gap-3 px-1">
          <div className="w-1 h-4 bg-primary/40 rounded-full" />
          <h2 className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Configuration</h2>
        </div>
        
        <div className="space-y-5">
          {config ? (
            <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
              <ConfigSection title="Raft" data={config.raft as Record<string, unknown>} delay={0.1} />
              <ConfigSection title="Worker" data={config.worker as Record<string, unknown>} delay={0.2} />
              <ConfigSection title="LLM" data={config.llm as Record<string, unknown>} delay={0.3} />
              <ConfigSection title="Storage" data={config.storage as Record<string, unknown>} delay={0.4} />
            </div>
          ) : (
            <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="glass-card rounded-2xl h-[280px] opacity-20 animate-pulse" />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Low-profile Security Section at the bottom */}
      <motion.div 
        initial={{ opacity: 0 }} 
        animate={{ opacity: 1 }} 
        transition={{ delay: 0.5 }}
        className="mt-12 pt-6 border-t border-border flex items-center justify-between"
      >
        <div>
          <h3 className="text-[11px] uppercase tracking-widest font-bold text-foreground/80 flex items-center gap-2">
            <Shield className="w-4 h-4 text-muted-foreground/60" />
            Reset Password
          </h3>
          <p className="mt-1 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Authentication Management</p>
        </div>
        
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger asChild>
            <Button variant="outline" size="sm" className="bg-transparent border-border hover:bg-card h-9 px-4 relative text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
              <Key className="w-3.5 h-3.5 mr-2 opacity-50" />
              Change Credentials
              {mustChange && <span className="absolute -top-1 -right-1 flex h-2.5 w-2.5 rounded-full bg-warning animate-pulse" />}
            </Button>
          </DialogTrigger>
          <DialogContent className="sm:max-w-[450px] glass-card">
            <DialogHeader className="pb-4 border-b border-border">
              <DialogTitle className="flex items-center gap-2 text-xl">
                <Key className="w-5 h-5 text-primary" />
                Update Password
              </DialogTitle>
              <DialogDescription className="pt-2">
                Enter your current password and choose a new secure password. Minimum 8 characters required.
              </DialogDescription>
            </DialogHeader>
            <PasswordForm onSuccess={() => setDialogOpen(false)} />
          </DialogContent>
        </Dialog>
      </motion.div>

    </div>
  );
}
