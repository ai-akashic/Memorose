"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Command } from "cmdk";
import { Search, Monitor, Database, Settings, LogOut } from "lucide-react";

export function CommandPalette() {
  const [open, setOpen] = React.useState(false);
  const router = useRouter();

  React.useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((open) => !open);
      }
    };

    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, []);

  const runCommand = React.useCallback(
    (command: () => unknown) => {
      setOpen(false);
      command();
    },
    []
  );

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-[20vh] bg-background/50 backdrop-blur-sm" onClick={() => setOpen(false)}>
      <Command 
        className="w-[600px] max-w-full rounded-xl glass-card overflow-hidden shadow-2xl border border-white/[0.05] bg-card/80 animate-in fade-in zoom-in-95 duration-200" 
        onClick={(e) => e.stopPropagation()}
        loop
      >
        <div className="flex items-center px-3 border-b border-white/[0.05]">
          <Search className="mr-2 h-4 w-4 shrink-0 opacity-50 text-muted-foreground" />
          <Command.Input 
            placeholder="Type a command or search..." 
            className="flex h-12 w-full rounded-md bg-transparent py-3 text-sm outline-none placeholder:text-muted-foreground disabled:cursor-not-allowed disabled:opacity-50 border-none ring-0 focus:ring-0 text-foreground"
            autoFocus
          />
        </div>
        
        <Command.List className="max-h-[300px] overflow-y-auto overflow-x-hidden p-2 text-sm text-foreground">
          <Command.Empty className="py-6 text-center text-sm text-muted-foreground">
            No results found.
          </Command.Empty>
          
          <Command.Group heading="Navigation" className="px-2 py-1.5 text-xs font-medium text-muted-foreground [&_[cmdk-group-heading]]:px-2 [&_[cmdk-group-heading]]:py-1.5 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:font-medium [&_[cmdk-group-heading]]:text-muted-foreground">
            <Command.Item 
              onSelect={() => runCommand(() => router.push("/metrics"))}
              className="relative flex cursor-default select-none items-center rounded-sm px-2 py-2 text-sm outline-none aria-selected:bg-white/[0.08] aria-selected:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50 hover:bg-white/[0.08] transition-colors"
            >
              <Monitor className="mr-2 h-4 w-4 text-muted-foreground" />
              <span>Go to Metrics</span>
            </Command.Item>
            <Command.Item 
              onSelect={() => runCommand(() => router.push("/memories"))}
              className="relative flex cursor-default select-none items-center rounded-sm px-2 py-2 text-sm outline-none aria-selected:bg-white/[0.08] aria-selected:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50 hover:bg-white/[0.08] transition-colors"
            >
              <Database className="mr-2 h-4 w-4 text-muted-foreground" />
              <span>Explore Memories</span>
            </Command.Item>
            <Command.Item 
              onSelect={() => runCommand(() => router.push("/settings"))}
              className="relative flex cursor-default select-none items-center rounded-sm px-2 py-2 text-sm outline-none aria-selected:bg-white/[0.08] aria-selected:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50 hover:bg-white/[0.08] transition-colors"
            >
              <Settings className="mr-2 h-4 w-4 text-muted-foreground" />
              <span>System Settings</span>
            </Command.Item>
          </Command.Group>
          <Command.Separator className="-mx-1 h-px bg-white/[0.05] my-1" />
          <Command.Group heading="Actions" className="px-2 py-1.5 text-xs font-medium text-muted-foreground [&_[cmdk-group-heading]]:px-2 [&_[cmdk-group-heading]]:py-1.5 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:font-medium [&_[cmdk-group-heading]]:text-muted-foreground">
             <Command.Item 
              onSelect={() => runCommand(() => window.dispatchEvent(new Event('logout')))}
              className="relative flex cursor-default select-none items-center rounded-sm px-2 py-2 text-sm outline-none aria-selected:bg-destructive/20 aria-selected:text-destructive data-[disabled]:pointer-events-none data-[disabled]:opacity-50 hover:bg-destructive/20 text-destructive transition-colors"
            >
              <LogOut className="mr-2 h-4 w-4" />
              <span>Logout</span>
            </Command.Item>
          </Command.Group>
        </Command.List>
      </Command>
    </div>
  );
}
