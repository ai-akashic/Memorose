"use client";

import { useEffect, useMemo, useState } from "react";
import { api } from "@/lib/api";
import { getMustChangePassword, setMustChangePassword } from "@/lib/auth";
import { useApiKeys, useOrganizations } from "@/lib/hooks";
import type { CreatedApiKey, RuntimeConfig } from "@/lib/types";
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
  Plus,
  Copy,
  Ban,
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { motion } from "framer-motion";
import { useTranslations } from "next-intl";
import { DashboardHero, DashboardStatRail } from "@/components/dashboard-chrome";

const sectionIcon: Record<string, React.ElementType> = {
  Raft: Server,
  Worker: Cpu,
  LLM: Brain,
  Storage: HardDrive,
};

function ConfigSection({ title, data, delay }: { title: string; data: Record<string, unknown>; delay: number }) {
  const t = useTranslations("Settings");
  const Icon = sectionIcon[title] ?? SettingsIcon;
  return (
    <motion.div
      initial={{ opacity: 0, y: 15 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay }}
    >
      <div className="mb-8">
        <div className="mb-4 flex items-center gap-2">
          <Icon className="h-4 w-4 text-muted-foreground" />
          <h2 className="text-[13px] font-semibold tracking-tight text-foreground">{t("sections.configTitle", { title })}</h2>
        </div>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-3">
          {Object.entries(data).map(([key, value]) => (
            <Card key={key} className="glass-card flex h-24 flex-col justify-between p-4 transition-colors hover:bg-muted/30">
              <span className="truncate text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
                {key.replace(/_/g, " ")}
              </span>
              <span className="mt-2 truncate font-mono text-sm text-foreground">
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
  const t = useTranslations("Settings");
  const [current, setCurrent] = useState("");
  const [newPw, setNewPw] = useState("");
  const [confirm, setConfirm] = useState("");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<{ type: "success" | "error"; text: string } | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setMessage(null);
    if (newPw !== confirm) {
      setMessage({ type: "error", text: t("security.errorMismatch") });
      return;
    }
    if (newPw.length < 8) {
      setMessage({ type: "error", text: t("security.errorTooShort") });
      return;
    }
    setLoading(true);
    try {
      await api.changePassword(current, newPw);
      setMustChangePassword(false);
      setMessage({ type: "success", text: t("security.success") });
      setCurrent("");
      setNewPw("");
      setConfirm("");
      setTimeout(() => onSuccess?.(), 1500);
    } catch (err: unknown) {
      setMessage({ type: "error", text: err instanceof Error ? err.message : t("security.errorFailed") });
    } finally {
      setLoading(false);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4 py-2">
      {[
        { id: "current-pw", label: t("security.currentPassword"), value: current, onChange: setCurrent },
        { id: "new-pw", label: t("security.newPassword"), value: newPw, onChange: setNewPw },
        { id: "confirm-pw", label: t("security.confirmPassword"), value: confirm, onChange: setConfirm },
      ].map(({ id, label, value, onChange }) => (
        <div key={id} className="space-y-1.5">
          <Label htmlFor={id} className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
            {label}
          </Label>
          <Input
            id={id}
            type="password"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            required
            className="h-9 border-border bg-background"
          />
        </div>
      ))}
      {message && (
        <motion.div
          initial={{ opacity: 0, scale: 0.98 }}
          animate={{ opacity: 1, scale: 1 }}
          className={`flex items-center gap-2 rounded-md border px-3 py-2 text-xs font-medium ${
            message.type === "success"
              ? "border-success/20 bg-success/10 text-success"
              : "border-destructive/20 bg-destructive/10 text-destructive"
          }`}
        >
          {message.type === "success" ? <Check className="h-3.5 w-3.5" /> : <AlertTriangle className="h-3.5 w-3.5" />}
          {message.text}
        </motion.div>
      )}
      <div className="pt-2">
        <Button type="submit" disabled={loading} className="h-9 w-full">
          {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Shield className="mr-2 h-4 w-4" />}
          {t("security.submit")}
        </Button>
      </div>
    </form>
  );
}

export default function SettingsPage() {
  const t = useTranslations("Settings");
  const mustChange = typeof window !== "undefined" && getMustChangePassword();
  const [config, setConfig] = useState<RuntimeConfig | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [apiKeyDialogOpen, setApiKeyDialogOpen] = useState(false);
  const [configLoading, setConfigLoading] = useState(true);
  const [createOrgId, setCreateOrgId] = useState("");
  const [createKeyName, setCreateKeyName] = useState("");
  const [creatingApiKey, setCreatingApiKey] = useState(false);
  const [revokingKeyId, setRevokingKeyId] = useState<string | null>(null);
  const [apiKeyMessage, setApiKeyMessage] = useState<{ type: "success" | "error"; text: string } | null>(null);
  const [createdApiKey, setCreatedApiKey] = useState<CreatedApiKey | null>(null);
  const [copiedCreatedKey, setCopiedCreatedKey] = useState(false);

  const { data: organizationsData } = useOrganizations();
  const { data: apiKeysData, mutate: mutateApiKeys, isLoading: apiKeysLoading } = useApiKeys();
  const organizations = useMemo(() => organizationsData?.organizations ?? [], [organizationsData]);
  const apiKeys = apiKeysData?.api_keys ?? [];

  useEffect(() => {
    let active = true;
    setConfigLoading(true);
    api.runtimeConfig()
      .then((value) => {
        if (active) {
          setConfig(value);
        }
      })
      .catch(() => {})
      .finally(() => {
        if (active) {
          setConfigLoading(false);
        }
      });
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!createOrgId && organizations.length > 0) {
      setCreateOrgId(organizations[0].org_id);
    }
  }, [createOrgId, organizations]);

  async function handleCreateApiKey(e: React.FormEvent) {
    e.preventDefault();
    setApiKeyMessage(null);
    if (!createOrgId) {
      setApiKeyMessage({ type: "error", text: t("apiKeys.createOrgRequired") });
      return;
    }

    setCreatingApiKey(true);
    try {
      const result = await api.createApiKey({
        org_id: createOrgId,
        name: createKeyName.trim() || undefined,
      });
      setCreatedApiKey(result);
      setCopiedCreatedKey(false);
      setCreateKeyName("");
      setApiKeyDialogOpen(false);
      setApiKeyMessage({ type: "success", text: t("apiKeys.createSuccess") });
      await mutateApiKeys();
    } catch (error) {
      setApiKeyMessage({
        type: "error",
        text: error instanceof Error ? error.message : t("apiKeys.errorFailed"),
      });
    } finally {
      setCreatingApiKey(false);
    }
  }

  async function handleRevokeApiKey(keyId: string) {
    setApiKeyMessage(null);
    setRevokingKeyId(keyId);
    try {
      await api.revokeApiKey(keyId);
      setApiKeyMessage({ type: "success", text: t("apiKeys.revokeSuccess") });
      await mutateApiKeys();
    } catch (error) {
      setApiKeyMessage({
        type: "error",
        text: error instanceof Error ? error.message : t("apiKeys.errorFailed"),
      });
    } finally {
      setRevokingKeyId(null);
    }
  }

  async function handleCopyCreatedKey() {
    if (!createdApiKey?.key) {
      return;
    }
    await navigator.clipboard.writeText(createdApiKey.key);
    setCopiedCreatedKey(true);
    window.setTimeout(() => setCopiedCreatedKey(false), 1600);
  }

  return (
    <div className="relative mx-auto max-w-4xl space-y-8 pb-12">
      <div className="blob-bg pointer-events-none absolute top-0 right-0 -z-10 h-[300px] w-[500px] opacity-15" />

      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
      >
        <DashboardHero
          icon={SettingsIcon}
          kicker={t("title")}
          title={t("title")}
          description={t("subtitle")}
        >
          <DashboardStatRail
            items={[
              { label: t("configuration"), value: configLoading ? "…" : "Ready", tone: "primary" },
              { label: t("apiKeys.title"), value: apiKeys.length, tone: "success" },
              { label: t("security.title"), value: mustChange ? "Alert" : "Nominal", tone: mustChange ? "warning" : "neutral" },
            ]}
          />
        </DashboardHero>
      </motion.div>

      {mustChange && (
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          className="glass-card flex items-start gap-4 rounded-xl border-warning/30 bg-warning/5 p-5"
        >
          <div className="rounded-lg bg-warning/20 p-2">
            <AlertTriangle className="h-5 w-5 text-warning" />
          </div>
          <div className="space-y-1">
            <h4 className="font-semibold text-warning">{t("mustChange.title")}</h4>
            <p className="text-sm text-warning/80">{t("mustChange.description")}</p>
          </div>
        </motion.div>
      )}

      <div className="space-y-6">
        <div className="flex items-center gap-3 px-1">
          <div className="h-4 w-1 rounded-full bg-primary/40" />
          <h2 className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("configuration")}</h2>
        </div>

        <div className="space-y-5">
          {!configLoading && config ? (
            <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
              <ConfigSection title="Raft" data={config.raft as Record<string, unknown>} delay={0.1} />
              <ConfigSection title="Worker" data={config.worker as Record<string, unknown>} delay={0.2} />
              <ConfigSection title="LLM" data={config.llm as Record<string, unknown>} delay={0.3} />
              <ConfigSection title="Storage" data={config.storage as Record<string, unknown>} delay={0.4} />
            </div>
          ) : (
            <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="glass-card h-[280px] animate-pulse rounded-2xl opacity-20" />
              ))}
            </div>
          )}
        </div>
      </div>

      <motion.section
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2 }}
        className="space-y-4"
      >
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="flex items-center gap-2 text-[11px] font-bold uppercase tracking-widest text-foreground/80">
              <Key className="h-4 w-4 text-muted-foreground/60" />
              {t("apiKeys.title")}
            </h2>
            <p className="mt-1 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
              {t("apiKeys.subtitle")}
            </p>
          </div>

          <Dialog open={apiKeyDialogOpen} onOpenChange={setApiKeyDialogOpen}>
            <DialogTrigger asChild>
              <Button variant="outline" size="sm" className="h-9 border-border bg-transparent px-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground hover:bg-card">
                <Plus className="mr-2 h-3.5 w-3.5 opacity-60" />
                {t("apiKeys.createButton")}
              </Button>
            </DialogTrigger>
            <DialogContent className="glass-card sm:max-w-[460px]">
              <DialogHeader className="border-b border-border pb-4">
                <DialogTitle className="flex items-center gap-2 text-xl">
                  <Key className="h-5 w-5 text-primary" />
                  {t("apiKeys.dialogTitle")}
                </DialogTitle>
                <DialogDescription className="pt-2">
                  {t("apiKeys.dialogDescription")}
                </DialogDescription>
              </DialogHeader>

              <form onSubmit={handleCreateApiKey} className="space-y-4 py-2">
                <div className="space-y-1.5">
                  <Label className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
                    {t("apiKeys.orgLabel")}
                  </Label>
                  <Select value={createOrgId} onValueChange={setCreateOrgId}>
                    <SelectTrigger className="h-9 border-border bg-background">
                      <SelectValue placeholder={t("apiKeys.orgPlaceholder")} />
                    </SelectTrigger>
                    <SelectContent>
                      {organizations.map((organization) => (
                        <SelectItem key={organization.org_id} value={organization.org_id}>
                          {organization.name} ({organization.org_id})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-1.5">
                  <Label htmlFor="api-key-name" className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">
                    {t("apiKeys.nameLabel")}
                  </Label>
                  <Input
                    id="api-key-name"
                    value={createKeyName}
                    onChange={(e) => setCreateKeyName(e.target.value)}
                    placeholder={t("apiKeys.namePlaceholder")}
                    className="h-9 border-border bg-background"
                  />
                </div>

                <p className="text-xs leading-6 text-muted-foreground">
                  {t("apiKeys.dialogFootnote")}
                </p>

                <Button type="submit" disabled={creatingApiKey || organizations.length === 0} className="h-9 w-full">
                  {creatingApiKey ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Plus className="mr-2 h-4 w-4" />}
                  {t("apiKeys.createSubmit")}
                </Button>
              </form>
            </DialogContent>
          </Dialog>
        </div>

        {apiKeyMessage && (
          <div
            className={`flex items-center gap-2 rounded-md border px-3 py-2 text-xs font-medium ${
              apiKeyMessage.type === "success"
                ? "border-success/20 bg-success/10 text-success"
                : "border-destructive/20 bg-destructive/10 text-destructive"
            }`}
          >
            {apiKeyMessage.type === "success" ? <Check className="h-3.5 w-3.5" /> : <AlertTriangle className="h-3.5 w-3.5" />}
            {apiKeyMessage.text}
          </div>
        )}

        {createdApiKey && (
          <Card className="glass-card border-primary/20 bg-primary/5 p-5">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div className="space-y-2">
                <div className="text-[11px] font-bold uppercase tracking-widest text-primary">
                  {t("apiKeys.createdTitle")}
                </div>
                <p className="text-sm text-muted-foreground">{t("apiKeys.createdDescription")}</p>
                <div className="grid gap-2 text-sm text-foreground sm:grid-cols-2">
                  <div>
                    <div className="text-[11px] uppercase tracking-widest text-muted-foreground">{t("apiKeys.orgLabel")}</div>
                    <div className="mt-1 font-medium">{createdApiKey.org_id}</div>
                  </div>
                  <div>
                    <div className="text-[11px] uppercase tracking-widest text-muted-foreground">{t("apiKeys.nameLabel")}</div>
                    <div className="mt-1 font-medium">{createdApiKey.name}</div>
                  </div>
                </div>
              </div>
              <Button variant="outline" onClick={handleCopyCreatedKey} className="h-9 border-border bg-background/70">
                {copiedCreatedKey ? <Check className="mr-2 h-4 w-4" /> : <Copy className="mr-2 h-4 w-4" />}
                {copiedCreatedKey ? t("apiKeys.copied") : t("apiKeys.copy")}
              </Button>
            </div>

            <div className="mt-4 rounded-xl border border-border bg-background/80 p-4">
              <div className="mb-2 text-[11px] uppercase tracking-widest text-muted-foreground">{t("apiKeys.rawKeyLabel")}</div>
              <code className="block break-all font-mono text-sm text-foreground">{createdApiKey.key}</code>
            </div>
          </Card>
        )}

        <div className="grid gap-3">
          {apiKeysLoading && apiKeys.length === 0 ? (
            [1, 2].map((index) => (
              <div key={index} className="glass-card h-28 animate-pulse rounded-2xl opacity-20" />
            ))
          ) : apiKeys.length === 0 ? (
            <Card className="glass-card border-dashed border-border/80 p-5 text-sm text-muted-foreground">
              {t("apiKeys.empty")}
            </Card>
          ) : (
            apiKeys.map((apiKey) => (
              <Card key={apiKey.key_id} className="glass-card p-5">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                  <div className="space-y-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="font-semibold text-foreground">{apiKey.name}</div>
                      <span className={`rounded-full px-2.5 py-1 text-[10px] font-bold uppercase tracking-widest ${
                        apiKey.active
                          ? "bg-success/10 text-success"
                          : "bg-muted text-muted-foreground"
                      }`}>
                        {apiKey.active ? t("apiKeys.active") : t("apiKeys.revoked")}
                      </span>
                    </div>
                    <div className="grid gap-3 text-sm text-muted-foreground sm:grid-cols-3">
                      <div>
                        <div className="text-[11px] uppercase tracking-widest">{t("apiKeys.prefixLabel")}</div>
                        <div className="mt-1 font-mono text-foreground">{apiKey.key_prefix}</div>
                      </div>
                      <div>
                        <div className="text-[11px] uppercase tracking-widest">{t("apiKeys.orgLabel")}</div>
                        <div className="mt-1 text-foreground">{apiKey.org_id}</div>
                      </div>
                      <div>
                        <div className="text-[11px] uppercase tracking-widest">{t("apiKeys.createdAtLabel")}</div>
                        <div className="mt-1 text-foreground">{new Date(apiKey.created_at).toLocaleString()}</div>
                      </div>
                    </div>
                  </div>

                  <Button
                    variant="outline"
                    size="sm"
                    disabled={!apiKey.active || revokingKeyId === apiKey.key_id}
                    onClick={() => handleRevokeApiKey(apiKey.key_id)}
                    className="h-9 border-border bg-transparent text-[11px] font-medium uppercase tracking-widest text-muted-foreground hover:bg-card"
                  >
                    {revokingKeyId === apiKey.key_id ? (
                      <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Ban className="mr-2 h-3.5 w-3.5 opacity-60" />
                    )}
                    {apiKey.active ? t("apiKeys.revokeButton") : t("apiKeys.revoked")}
                  </Button>
                </div>
              </Card>
            ))
          )}
        </div>
      </motion.section>

      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.5 }}
        className="mt-12 flex items-center justify-between border-t border-border pt-6"
      >
        <div>
          <h3 className="flex items-center gap-2 text-[11px] font-bold uppercase tracking-widest text-foreground/80">
            <Shield className="h-4 w-4 text-muted-foreground/60" />
            {t("security.title")}
          </h3>
          <p className="mt-1 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{t("security.subtitle")}</p>
        </div>

        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger asChild>
            <Button variant="outline" size="sm" className="relative h-9 border-border bg-transparent px-4 text-[11px] font-medium uppercase tracking-widest text-muted-foreground hover:bg-card">
              <Key className="mr-2 h-3.5 w-3.5 opacity-50" />
              {t("security.button")}
              {mustChange && <span className="absolute -top-1 -right-1 flex h-2.5 w-2.5 animate-pulse rounded-full bg-warning" />}
            </Button>
          </DialogTrigger>
          <DialogContent className="glass-card sm:max-w-[450px]">
            <DialogHeader className="border-b border-border pb-4">
              <DialogTitle className="flex items-center gap-2 text-xl">
                <Key className="h-5 w-5 text-primary" />
                {t("security.dialogTitle")}
              </DialogTitle>
              <DialogDescription className="pt-2">{t("security.dialogDescription")}</DialogDescription>
            </DialogHeader>
            <PasswordForm onSuccess={() => setDialogOpen(false)} />
          </DialogContent>
        </Dialog>
      </motion.div>
    </div>
  );
}
