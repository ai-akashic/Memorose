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
import { MemoroseLogo } from "@/components/haku-logo";

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
    setMounted(true);
  }, []);

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
        <div className="h-screen flex bg-background overflow-hidden">
          <aside
            className={cn(
              "flex flex-col shrink-0 border-r border-border bg-card/80 backdrop-blur-sm transition-all duration-200 z-30 h-full overflow-hidden",
              collapsed ? "w-14" : "w-52"
            )}
          >
            <div className="flex items-center gap-2 px-3 h-12 border-b border-border">
              <MemoroseLogo size={32} />
              {!collapsed && (
                <span className="font-semibold text-sm tracking-tight">Memorose</span>
              )}
            </div>

            <nav className="flex-1 py-2 space-y-0.5 px-2">
              {navItems.map((item) => {
                const isActive = pathname === item.href || pathname?.startsWith(item.href.replace(/\/$/, ""));
                const link = (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      "flex items-center gap-2.5 px-2.5 py-1.5 rounded-md text-[13px] transition-colors",
                      isActive
                        ? "bg-primary/10 text-primary font-medium"
                        : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                    )}
                  >
                    <item.icon className="w-3.5 h-3.5 shrink-0" />
                    {!collapsed && item.label}
                  </Link>
                );

                if (collapsed) {
                  return (
                    <Tooltip key={item.href}>
                      <TooltipTrigger asChild>{link}</TooltipTrigger>
                      <TooltipContent side="right" className="text-xs">{item.label}</TooltipContent>
                    </Tooltip>
                  );
                }

                return link;
              })}
            </nav>

            <div className="border-t border-border p-1.5 space-y-0.5">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setCollapsed(!collapsed)}
                className={cn(
                  "w-full h-7 justify-start gap-2.5 text-xs text-muted-foreground hover:text-foreground",
                  collapsed && "justify-center"
                )}
              >
                <ChevronLeft
                  className={cn(
                    "w-3.5 h-3.5 shrink-0 transition-transform",
                    collapsed && "rotate-180"
                  )}
                />
                {!collapsed && "Collapse"}
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => {
                  clearToken();
                  router.push("/login/");
                }}
                className={cn(
                  "w-full h-7 justify-start gap-2.5 text-xs text-muted-foreground hover:text-destructive hover:bg-destructive/10",
                  collapsed && "justify-center"
                )}
              >
                <LogOut className="w-3.5 h-3.5 shrink-0" />
                {!collapsed && "Logout"}
              </Button>
            </div>
          </aside>

          <main className="flex-1 overflow-auto h-full allow-select">
            <div className="p-6 max-w-7xl h-full">{children}</div>
          </main>
        </div>
      </TooltipProvider>
    </UserFilterContext.Provider>
  );
}
