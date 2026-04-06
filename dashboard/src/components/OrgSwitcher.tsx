"use client";

import { useMemo, useState } from "react";
import { useTranslations } from "next-intl";
import { Building2, Check, ChevronsUpDown, Loader2, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api";
import { useOrganizations } from "@/lib/hooks";
import { DEFAULT_ORG_ID, useOrgScope } from "@/lib/org-scope";

export function OrgSwitcher({ collapsed }: { collapsed?: boolean }) {
  const t = useTranslations("Organizations");
  const { orgId, setOrgId } = useOrgScope();
  const { data, mutate } = useOrganizations();
  const [open, setOpen] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createId, setCreateId] = useState("");
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const organizations = useMemo(() => {
    const items = data?.organizations ?? [];
    return items.length > 0
      ? items
      : [{ org_id: DEFAULT_ORG_ID, name: t("defaultName"), created_at: new Date(0).toISOString() }];
  }, [data, t]);

  const activeOrg =
    organizations.find((organization) => organization.org_id === orgId) ??
    organizations.find((organization) => organization.org_id === DEFAULT_ORG_ID) ??
    organizations[0];

  async function handleCreate() {
    const normalizedOrgId = createId.trim();
    if (!normalizedOrgId) {
      setError(t("switcher.errorOrgIdRequired"));
      return;
    }

    setCreating(true);
    setError(null);

    try {
      const created = await api.createOrganization({
        org_id: normalizedOrgId,
        name: createName.trim() || undefined,
      });
      await mutate();
      setOrgId(created.org_id);
      setCreateId("");
      setCreateName("");
      setShowCreateForm(false);
      setOpen(false);
    } catch (createError) {
      setError(createError instanceof Error ? createError.message : t("switcher.errorCreateFailed"));
    } finally {
      setCreating(false);
    }
  }

  const handleOpenChange = (val: boolean) => {
    setOpen(val);
    if (!val) {
      setShowCreateForm(false);
      setError(null);
    }
  };

  const trigger = collapsed ? (
    <button
      type="button"
      className="flex h-7 w-7 items-center justify-center rounded-md border border-primary/20 bg-primary/10 text-[10px] font-bold text-primary"
      aria-label={t("switcher.ariaLabel")}
    >
      {activeOrg.org_id.charAt(0).toUpperCase()}
    </button>
  ) : (
    <Button variant="ghost" className="h-9 w-full justify-between px-1.5 hover:bg-muted/70">
      <div className="flex min-w-0 items-center gap-2">
        <div className="flex h-6 w-6 items-center justify-center rounded-md border border-primary/20 bg-primary/10 text-primary">
          <Building2 className="h-3 w-3" />
        </div>
        <div className="min-w-0 text-left">
          <p className="truncate text-[12px] font-semibold leading-none">{activeOrg.name}</p>
        </div>
      </div>
      <ChevronsUpDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
    </Button>
  );

  return (
    <div className={collapsed ? "flex justify-center border-b border-border py-2.5" : "flex shrink-0 items-center border-b border-border px-2.5 py-2.5"}>
      <Dialog open={open} onOpenChange={handleOpenChange}>
        <DialogTrigger asChild>{trigger}</DialogTrigger>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-base">
              <Building2 className="h-4 w-4 text-primary" />
              {t("panel.title")}
            </DialogTitle>
          </DialogHeader>

          <div className="space-y-5">
            <div className="space-y-2">
              <p className="text-sm text-muted-foreground">
                {t("switcher.description")}
              </p>
              <div className="grid gap-2">
                {organizations.map((organization) => {
                  const selected = organization.org_id === activeOrg.org_id;
                  return (
                    <button
                      key={organization.org_id}
                      type="button"
                      onClick={() => {
                        setOrgId(organization.org_id);
                        setOpen(false);
                      }}
                      className={`flex min-h-12 items-center justify-between rounded-xl border px-3 py-3 text-left transition-colors ${
                        selected
                          ? "border-primary/30 bg-primary/5"
                          : "border-border/70 bg-background/60 hover:bg-muted/60"
                      }`}
                    >
                      <div className="min-w-0">
                        <p className="truncate text-sm font-semibold">{organization.name}</p>
                        <p className="truncate font-mono text-[11px] text-muted-foreground">
                          {organization.org_id}
                        </p>
                      </div>
                      {selected ? <Check className="h-4 w-4 shrink-0 text-primary" /> : null}
                    </button>
                  );
                })}
              </div>
            </div>

            {!showCreateForm ? (
              <Button 
                variant="outline" 
                className="w-full border-dashed border-border/70 text-muted-foreground hover:text-foreground h-12 rounded-xl" 
                onClick={() => setShowCreateForm(true)}
              >
                <Plus className="mr-2 h-4 w-4" />
                {t("switcher.createTitle")}
              </Button>
            ) : (
              <div className="space-y-3 rounded-2xl border border-border/70 bg-background/50 p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                    <Plus className="h-3.5 w-3.5" />
                    {t("switcher.createTitle")}
                  </div>
                </div>
                <div className="grid gap-3 md:grid-cols-2">
                  <Input
                    value={createId}
                    onChange={(event) => setCreateId(event.target.value)}
                    placeholder={t("createOrg.orgIdPlaceholder")}
                    className="font-mono"
                  />
                  <Input
                    value={createName}
                    onChange={(event) => setCreateName(event.target.value)}
                    placeholder={t("createOrg.displayName")}
                  />
                </div>
                {error ? (
                  <div className="rounded-lg border border-destructive/20 bg-destructive/5 px-3 py-2 text-sm text-destructive">
                    {error}
                  </div>
                ) : null}
                <div className="flex gap-2">
                  <Button type="button" onClick={() => setShowCreateForm(false)} variant="outline" className="w-1/3">
                    {t("switcher.cancel", { fallback: "Cancel" })}
                  </Button>
                  <Button type="button" onClick={handleCreate} disabled={creating} className="flex-1">
                    {creating ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Plus className="mr-2 h-4 w-4" />}
                    {t("switcher.createAndSwitch")}
                  </Button>
                </div>
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
