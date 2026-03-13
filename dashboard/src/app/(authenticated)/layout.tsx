"use client";

import { createContext, useContext, useEffect, useState } from "react";
import { useRouter, usePathname } from "next/navigation";
import { isAuthenticated, clearToken } from "@/lib/auth";
import {
  LayoutDashboard,
  Database,
  BarChart3,
  Settings,
  LogOut,
  ChevronLeft,
  MessageSquare,
  Package,
  Bot,
  CheckSquare,
  UserRound,
} from "lucide-react";
import Link from "next/link";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { CommandPalette } from "@/components/CommandPalette";
import { MemoroseLogo } from "@/components/haku-logo";
import { Input } from "@/components/ui/input";

const UserFilterContext = createContext<{
  userId: string;
  setUserId: (v: string) => void;
}>({ userId: "", setUserId: () => {} });

export function useUserFilter() {
  return useContext(UserFilterContext);
}

const navItems = [
  { href: "/cluster/", label: "Cluster", icon: LayoutDashboard },
  { href: "/memories/", label: "Memories", icon: Database },
  { href: "/playground/", label: "Playground", icon: MessageSquare },
  { href: "/apps/", label: "Apps", icon: Package },
  { href: "/agents/", label: "Agents", icon: Bot },
  { href: "/tasks/", label: "Tasks", icon: CheckSquare },
  { href: "/metrics/", label: "Metrics", icon: BarChart3 },
  { href: "/settings/", label: "Settings", icon: Settings },
];

export default function AuthenticatedLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);
  const [mounted, setMounted] = useState(false);
  const [userId, setUserId] = useState("");

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setMounted(true);
    const handleLogout = () => {
      clearToken();
      router.push("/login/");
    };
    window.addEventListener("logout", handleLogout);
    return () => window.removeEventListener("logout", handleLogout);
  }, [router]);

  useEffect(() => {
    if (mounted && !isAuthenticated()) {
      router.push("/login/");
    }
  }, [mounted, router]);

  if (!mounted || !isAuthenticated()) {
    return (
      <div className="h-screen flex items-center justify-center bg-background">
        <div className="animate-pulse text-muted-foreground text-sm">Loading...</div>
      </div>
    );
  }

  return (
    <UserFilterContext.Provider value={{ userId, setUserId }}>
      <TooltipProvider delayDuration={0}>
        <div className="h-screen flex bg-background overflow-hidden relative">
          <CommandPalette />
          <aside
            className={cn(
              "flex flex-col shrink-0 border-r border-border bg-card transition-all duration-300 z-30 h-full overflow-hidden",
              collapsed ? "w-[72px]" : "w-[220px]"
            )}
          >
            <div className="flex items-center gap-3 px-4 h-[52px] border-b border-border shrink-0">
              <div className="relative z-10">
                <MemoroseLogo size={24} />
              </div>
              {!collapsed && (
                <div className="flex flex-col">
                  <span className="font-bold text-[15px] tracking-tight leading-none text-text-strong">
                    Memorose
                  </span>
                  <span className="text-[9px] font-medium text-muted-foreground uppercase tracking-widest mt-0.5">
                    Dashboard
                  </span>
                </div>
              )}
            </div>

            <nav className="flex-1 py-4 space-y-1 px-3 overflow-y-auto">
              {navItems.map((item) => {
                const isActive = pathname === item.href || pathname?.startsWith(item.href.replace(/\/$/, ""));
                const link = (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      "relative flex items-center gap-3 px-3 py-2 rounded-md text-[13.5px] transition-colors duration-200 group overflow-hidden",
                      isActive
                        ? "text-primary-foreground font-medium bg-primary/10 border border-primary/20"
                        : "text-muted-foreground hover:text-foreground hover:bg-muted"
                    )}
                  >
                    <item.icon className={cn("w-4 h-4 shrink-0", isActive ? "text-primary" : "opacity-70 group-hover:opacity-100")} />
                    {!collapsed && <span className="truncate">{item.label}</span>}
                  </Link>
                );

                if (collapsed) {
                  return (
                    <Tooltip key={item.href}>
                      <TooltipTrigger asChild>{link}</TooltipTrigger>
                      <TooltipContent side="right" className="text-xs bg-popover border-border text-popover-foreground">{item.label}</TooltipContent>
                    </Tooltip>
                  );
                }


                return link;
              })}
            </nav>

            <div className="border-t border-border px-3 py-4">
              {collapsed ? (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <div className="flex justify-center py-1 cursor-default">
                      <UserRound className={cn("w-4 h-4 shrink-0", userId ? "text-primary opacity-80" : "text-muted-foreground")} />
                    </div>
                  </TooltipTrigger>
                  <TooltipContent side="right" className="text-xs bg-popover border-border text-popover-foreground">
                    {userId || "NO FILTER"}
                  </TooltipContent>
                </Tooltip>
              ) : (
                <div className="space-y-2 px-1">
                  <div className="flex items-center gap-2">
                    <UserRound className="w-3.5 h-3.5 text-muted-foreground" />
                    <span className="text-[11px] font-medium uppercase tracking-widest text-muted-foreground">Scope</span>
                  </div>
                  <Input
                    value={userId}
                    onChange={(e) => setUserId(e.target.value)}
                    placeholder="USER_ID"
                    className="h-8 text-[12px] font-mono bg-muted border-border focus:border-primary placeholder:text-muted-foreground/50"
                  />
                </div>
              )}
            </div>

            <div className="border-t border-border p-3 space-y-1 bg-background/50">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setCollapsed(!collapsed)}
                className={cn(
                  "w-full h-8 justify-start gap-3 text-muted-foreground hover:text-foreground hover:bg-muted",
                  collapsed && "justify-center px-0"
                )}
              >
                <ChevronLeft
                  className={cn(
                    "w-4 h-4 shrink-0 transition-transform duration-300",
                    collapsed && "rotate-180"
                  )}
                />
                {!collapsed && <span className="text-[13px] font-medium">Collapse</span>}
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => {
                  clearToken();
                  router.push("/login/");
                }}
                className={cn(
                  "w-full h-8 justify-start gap-3 text-muted-foreground hover:text-destructive hover:bg-destructive/10 group",
                  collapsed && "justify-center px-0"
                )}
              >
                <LogOut className="w-4 h-4 shrink-0 group-hover:-translate-x-0.5 transition-transform" />
                {!collapsed && <span className="text-[13px] font-medium">Logout</span>}
              </Button>
            </div>
          </aside>

          <main className="flex-1 overflow-auto h-full allow-select bg-background">
            <div className="p-6 max-w-7xl h-full openclaw-dashboard-enter">{children}</div>
          </main>
        </div>
      </TooltipProvider>
    </UserFilterContext.Provider>
  );
}
