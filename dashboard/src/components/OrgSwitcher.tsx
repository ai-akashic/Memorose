"use client";

import { useMemo, useState } from "react";
import { Building2, Check, ChevronsUpDown, Loader2, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api";
import { useOrganizations } from "@/lib/hooks";
import { DEFAULT_ORG_ID, useOrgScope } from "@/lib/org-scope";

export function OrgSwitcher({ collapsed }: { collapsed?: boolean }) {
  const { orgId, setOrgId } = useOrgScope();
  const { data, mutate } = useOrganizations();
  const [open, setOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createId, setCreateId] = useState("");
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const organizations = useMemo(() => {
    const items = data?.organizations ?? [];
    return items.length > 0
      ? items
      : [{ org_id: DEFAULT_ORG_ID, name: "Default", created_at: new Date(0).toISOString() }];
  }, [data]);

  const activeOrg =
    organizations.find((organization) => organization.org_id === orgId) ??
    organizations.find((organization) => organization.org_id === DEFAULT_ORG_ID) ??
    organizations[0];

  async function handleCreate() {
    const normalizedOrgId = createId.trim();
    if (!normalizedOrgId) {
      setError("org_id is required");
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
      setOpen(false);
    } catch (createError) {
      setError(createError instanceof Error ? createError.message : "Failed to create organization");
    } finally {
      setCreating(false);
    }
  }

  const trigger = collapsed ? (
    <button
      type="button"
      className="flex h-8 w-8 items-center justify-center rounded-md border border-primary/20 bg-primary/10 text-xs font-bold text-primary"
      aria-label="Switch organization"
    >
      {activeOrg.org_id.charAt(0).toUpperCase()}
    </button>
  ) : (
    <Button variant="ghost" className="h-11 w-full justify-between px-2 hover:bg-muted">
      <div className="flex min-w-0 items-center gap-3">
        <div className="flex h-7 w-7 items-center justify-center rounded-md border border-primary/20 bg-primary/10 text-primary">
          <Building2 className="h-3.5 w-3.5" />
        </div>
        <div className="min-w-0 text-left">
          <p className="truncate text-[13px] font-semibold leading-none">{activeOrg.name}</p>
          <p className="mt-1 truncate font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            {activeOrg.org_id}
          </p>
        </div>
      </div>
      <ChevronsUpDown className="h-4 w-4 shrink-0 text-muted-foreground" />
    </Button>
  );

  return (
    <div className={collapsed ? "flex justify-center border-b border-border py-4" : "flex shrink-0 items-center border-b border-border px-4 py-4"}>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogTrigger asChild>{trigger}</DialogTrigger>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-base">
              <Building2 className="h-4 w-4 text-primary" />
              Organizations
            </DialogTitle>
          </DialogHeader>

          <div className="space-y-5">
            <div className="space-y-2">
              <p className="text-sm text-muted-foreground">
                The active organization controls which apps and organization memory you are viewing.
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

            <div className="space-y-3 rounded-2xl border border-border/70 bg-background/50 p-4">
              <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
                <Plus className="h-3.5 w-3.5" />
                Create Organization
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <Input
                  value={createId}
                  onChange={(event) => setCreateId(event.target.value)}
                  placeholder="org_id"
                  className="font-mono"
                />
                <Input
                  value={createName}
                  onChange={(event) => setCreateName(event.target.value)}
                  placeholder="Display name"
                />
              </div>
              {error ? (
                <div className="rounded-lg border border-destructive/20 bg-destructive/5 px-3 py-2 text-sm text-destructive">
                  {error}
                </div>
              ) : null}
              <Button type="button" onClick={handleCreate} disabled={creating} className="w-full">
                {creating ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Plus className="mr-2 h-4 w-4" />}
                Create And Switch
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
