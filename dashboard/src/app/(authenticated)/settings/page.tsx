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
  Terminal,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
      <Card className="glass-card border-white/[0.04] overflow-hidden group hover:border-white/10 transition-all duration-500 shadow-2xl">
        <CardHeader className="pb-4 border-b border-white/[0.03] bg-white/[0.01]">
          <CardTitle className="text-xs flex items-center gap-3">
            <div className="p-2 rounded-lg bg-primary/5 border border-white/[0.03] group-hover:scale-110 transition-transform duration-500">
              <Icon className="w-4 h-4 text-primary opacity-60" />
            </div>
            <span className="uppercase tracking-[0.2em] text-muted-foreground/40 font-bold">{title}</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="p-6 grid grid-cols-1 sm:grid-cols-2 gap-5">
          {Object.entries(data).map(([key, value]) => (
            <div key={key} className="flex flex-col gap-1.5 p-4 rounded-xl bg-white/[0.01] border border-white/[0.02] hover:bg-white/[0.03] hover:border-white/[0.05] transition-all duration-300 group/item">
              <span className="text-[9px] text-muted-foreground/40 uppercase tracking-widest font-bold flex items-center gap-2">
                <Terminal className="w-3 h-3 opacity-20 group-hover/item:text-primary group-hover/item:opacity-40 transition-all" />
                {key.replace(/_/g, ' ')}
              </span>
              <span className="font-mono text-[13px] text-foreground/70 truncate max-w-full pl-5">
                {typeof value === "object" ? JSON.stringify(value) : String(value)}
              </span>
            </div>
          ))}
        </CardContent>
      </Card>
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
    <form onSubmit={handleSubmit} className="space-y-5 py-2">
      {[
        { id: "current-pw", label: "Current Password", value: current, onChange: setCurrent },
        { id: "new-pw", label: "New Password", value: newPw, onChange: setNewPw },
        { id: "confirm-pw", label: "Confirm New Password", value: confirm, onChange: setConfirm },
      ].map(({ id, label, value, onChange }) => (
        <div key={id} className="space-y-2">
          <Label htmlFor={id} className="text-[10px] uppercase tracking-widest text-muted-foreground/50 font-bold px-1">{label}</Label>
          <Input
            id={id}
            type="password"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            required
            className="bg-black/20 border-white/5 h-11 text-base"
          />
        </div>
      ))}
      {message && (
        <motion.div 
          initial={{ opacity: 0, scale: 0.98 }} animate={{ opacity: 1, scale: 1 }}
          className={`text-xs font-bold uppercase tracking-widest rounded-xl px-5 py-4 border flex items-center gap-3 ${
          message.type === "success"
            ? "bg-success/5 text-success border-success/20"
            : "bg-destructive/5 text-destructive border-destructive/20"
        }`}>
          {message.type === "success" ? <Check className="w-4 h-4" /> : <AlertTriangle className="w-4 h-4" />}
          {message.text}
        </motion.div>
      )}
      <Button type="submit" disabled={loading} className="w-full h-12 mt-2 font-bold uppercase tracking-[0.2em] text-xs rounded-xl shadow-xl">
        {loading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Shield className="w-4 h-4 mr-2" />}
        Update Password
      </Button>
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
          <div className="p-2.5 bg-white/5 rounded-xl border border-white/10 backdrop-blur-md">
            <SettingsIcon className="w-6 h-6 text-foreground/80" />
          </div>
          <div>
            <h1 className="text-3xl font-extrabold tracking-tight bg-clip-text text-transparent bg-gradient-to-br from-white via-white to-white/50">
              Settings
            </h1>
            <p className="text-muted-foreground mt-1 text-sm font-medium">Manage system configuration and security credentials.</p>
          </div>
        </div>
      </motion.div>

      {mustChange && (
        <motion.div initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }} className="glass-card rounded-xl border border-warning/30 p-5 flex items-start gap-4 bg-warning/5 shadow-lg shadow-warning/5">
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
          <h2 className="text-[10px] uppercase tracking-[0.3em] font-bold text-muted-foreground/40">Configuration</h2>
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
                <div key={i} className="glass-card rounded-2xl h-[280px] opacity-20 animate-pulse border-white/5" />
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
        className="mt-12 pt-6 border-t border-white/[0.06] flex items-center justify-between"
      >
        <div>
          <h3 className="text-[11px] uppercase tracking-widest font-bold text-foreground/80 flex items-center gap-2">
            <Shield className="w-4 h-4 text-muted-foreground/60" />
            Reset Password
          </h3>
          <p className="text-[10px] uppercase tracking-widest text-muted-foreground/30 font-bold mt-1">Authentication Management</p>
        </div>
        
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger asChild>
            <Button variant="outline" size="sm" className="bg-transparent border-white/10 hover:bg-white/5 text-[10px] uppercase tracking-widest font-bold h-9 px-4 relative">
              <Key className="w-3.5 h-3.5 mr-2 opacity-50" />
              Change Credentials
              {mustChange && <span className="absolute -top-1 -right-1 flex h-2.5 w-2.5 rounded-full bg-warning animate-pulse" />}
            </Button>
          </DialogTrigger>
          <DialogContent className="sm:max-w-[450px] glass-card border-white/10 shadow-2xl">
            <DialogHeader className="pb-4 border-b border-white/5">
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
