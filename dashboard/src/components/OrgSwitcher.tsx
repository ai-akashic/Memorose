import { useState } from "react";
import { Check, ChevronsUpDown, Building2, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

const MOCK_ORGS = [
  { id: "org_1", name: "Acme Corp", slug: "acme" },
  { id: "org_2", name: "Personal Workspace", slug: "personal" },
];

export function OrgSwitcher({ collapsed }: { collapsed?: boolean }) {
  const [activeOrg, setActiveOrg] = useState(MOCK_ORGS[0]);

  if (collapsed) {
    return (
      <div className="flex justify-center py-4 border-b border-border shrink-0">
        <div className="w-8 h-8 rounded-md bg-primary/10 border border-primary/20 flex items-center justify-center text-primary font-bold text-xs">
          {activeOrg.name.charAt(0)}
        </div>
      </div>
    );
  }

  return (
    <div className="px-4 py-4 border-b border-border shrink-0 flex items-center">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            variant="ghost"
            className="w-full justify-between px-2 h-10 hover:bg-muted"
          >
            <div className="flex items-center gap-2 truncate">
              <div className="w-6 h-6 rounded flex items-center justify-center bg-primary/10 border border-primary/20 text-primary shrink-0">
                <Building2 className="w-3.5 h-3.5" />
              </div>
              <div className="flex flex-col items-start truncate">
                <span className="text-[13px] font-medium leading-none truncate">
                  {activeOrg.name}
                </span>
                <span className="text-[10px] text-muted-foreground mt-1 uppercase tracking-widest font-medium">
                  Organization
                </span>
              </div>
            </div>
            <ChevronsUpDown className="w-4 h-4 text-muted-foreground shrink-0" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent className="w-[200px] bg-popover border-border" align="start">
          <DropdownMenuLabel className="text-[11px] uppercase tracking-widest text-muted-foreground font-medium">
            Organizations
          </DropdownMenuLabel>
          {MOCK_ORGS.map((org) => (
            <DropdownMenuItem
              key={org.id}
              onClick={() => setActiveOrg(org)}
              className="text-[13px] cursor-pointer focus:bg-muted"
            >
              <Building2 className="w-4 h-4 mr-2 text-muted-foreground" />
              <span className="flex-1 truncate">{org.name}</span>
              {activeOrg.id === org.id && <Check className="w-4 h-4 text-primary" />}
            </DropdownMenuItem>
          ))}
          <DropdownMenuSeparator className="bg-border" />
          <DropdownMenuItem className="text-[13px] cursor-pointer focus:bg-muted text-primary">
            <Plus className="w-4 h-4 mr-2" />
            Create Organization
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}
