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
import { CommandPalette } from "@/components/CommandPalette";
import { MemoroseLogo } from "@/components/haku-logo";
import { motion } from "framer-motion";

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
              "flex flex-col shrink-0 border-r border-white/5 bg-black/40 backdrop-blur-xl transition-all duration-300 z-30 h-full overflow-hidden shadow-[inset_-1px_0_0_rgba(255,255,255,0.02)]",
              collapsed ? "w-16" : "w-60"
            )}
          >
            <div className="flex items-center gap-3 px-4 h-16 border-b border-white/5 relative">
              <div className="absolute inset-0 bg-gradient-to-r from-primary/10 to-transparent opacity-50" />
              <div className="relative z-10 drop-shadow-[0_0_10px_rgba(255,255,255,0.3)]">
                <MemoroseLogo size={32} />
              </div>
              {!collapsed && (
                <span className="font-bold text-base tracking-tight relative z-10 bg-clip-text text-transparent bg-gradient-to-r from-white to-white/70">
                  Memorose
                </span>
              )}
            </div>

            <nav className="flex-1 py-4 space-y-1.5 px-3 overflow-y-auto">
              {navItems.map((item) => {
                const isActive = pathname === item.href || pathname?.startsWith(item.href.replace(/\/$/, ""));
                const link = (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      "relative flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-all duration-200 group overflow-hidden",
                      isActive
                        ? "text-primary-foreground font-medium"
                        : "text-muted-foreground hover:text-foreground"
                    )}
                  >
                    {isActive && (
                      <motion.div
                        layoutId="active-nav-bg"
                        className="absolute inset-0 bg-gradient-to-r from-primary/80 to-primary/40 rounded-lg -z-10 shadow-[0_0_15px_rgba(56,125,255,0.3)]"
                        initial={false}
                        transition={{ type: "spring", stiffness: 350, damping: 30 }}
                      />
                    )}
                    {!isActive && (
                       <div className="absolute inset-0 bg-white/5 opacity-0 group-hover:opacity-100 transition-opacity rounded-lg -z-10" />
                    )}
                    <item.icon className={cn("w-4 h-4 shrink-0 transition-transform group-hover:scale-110", isActive ? "opacity-100" : "opacity-70")} />
                    {!collapsed && <span className="truncate">{item.label}</span>}
                  </Link>
                );

                if (collapsed) {
                  return (
                    <Tooltip key={item.href}>
                      <TooltipTrigger asChild>{link}</TooltipTrigger>
                      <TooltipContent side="right" className="text-xs border-white/10 glass-card bg-black/80">{item.label}</TooltipContent>
                    </Tooltip>
                  );
                }

                return link;
              })}
            </nav>

            <div className="border-t border-white/5 p-3 space-y-1 bg-black/20">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setCollapsed(!collapsed)}
                className={cn(
                  "w-full h-9 justify-start gap-3 text-sm text-muted-foreground hover:text-foreground hover:bg-white/5",
                  collapsed && "justify-center"
                )}
              >
                <ChevronLeft
                  className={cn(
                    "w-4 h-4 shrink-0 transition-transform duration-300",
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
                  "w-full h-9 justify-start gap-3 text-sm text-muted-foreground hover:text-destructive hover:bg-destructive/10 group",
                  collapsed && "justify-center"
                )}
              >
                <LogOut className="w-4 h-4 shrink-0 group-hover:-translate-x-1 transition-transform" />
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
