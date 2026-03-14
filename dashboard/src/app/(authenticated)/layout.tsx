"use client";

import { useEffect, useState } from "react";
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
  Building2,
} from "lucide-react";
import Link from "next/link";
import { useTranslations } from "next-intl";
import { cn } from "@/lib/utils";
import { OrgScopeProvider } from "@/lib/org-scope";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { CommandPalette } from "@/components/CommandPalette";
import { LocaleSwitcher } from "@/components/LocaleSwitcher";
import { OrgSwitcher } from "@/components/OrgSwitcher";

export default function AuthenticatedLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const t = useTranslations("Navigation");
  const [collapsed, setCollapsed] = useState(false);
  const [mounted, setMounted] = useState(false);

  const navItems = [
    { href: "/cluster/", label: t("cluster"), icon: LayoutDashboard },
    { href: "/organizations/", label: "Organizations", icon: Building2 },
    { href: "/apps/", label: t("apps"), icon: Package },
    { href: "/memories/", label: t("memories"), icon: Database },
    { href: "/playground/", label: "Playground", icon: MessageSquare },
    { href: "/agents/", label: t("agents"), icon: Bot },
    { href: "/tasks/", label: t("tasks"), icon: CheckSquare },
    { href: "/metrics/", label: "Metrics", icon: BarChart3 },
    { href: "/settings/", label: t("settings"), icon: Settings },
  ];

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
    <OrgScopeProvider>
      <TooltipProvider delayDuration={0}>
        <div className="h-screen flex bg-background overflow-hidden relative">
          <CommandPalette />
          <aside
            className={cn(
              "flex flex-col shrink-0 border-r border-border bg-card transition-all duration-300 z-30 h-full overflow-hidden",
              collapsed ? "w-[72px]" : "w-[220px]"
            )}
          >
            <OrgSwitcher collapsed={collapsed} />

            <nav className="flex-1 py-4 space-y-1 px-3 overflow-y-auto">
              {navItems.map((item) => {
                const isActive = pathname === item.href || pathname?.startsWith(item.href);
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

            <div className="border-t border-border p-3 space-y-1 bg-background/50 flex flex-col items-center">
              {!collapsed ? (
                <div className="w-full flex justify-between items-center mb-1">
                  <LocaleSwitcher />
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setCollapsed(!collapsed)}
                    className="h-8 w-8 p-0 text-muted-foreground hover:text-foreground hover:bg-muted"
                  >
                    <ChevronLeft className="w-4 h-4 shrink-0 transition-transform duration-300" />
                  </Button>
                </div>
              ) : (
                <>
                  <LocaleSwitcher />
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setCollapsed(!collapsed)}
                    className="w-full h-8 justify-center px-0 text-muted-foreground hover:text-foreground hover:bg-muted"
                  >
                    <ChevronLeft className="w-4 h-4 shrink-0 transition-transform duration-300 rotate-180" />
                  </Button>
                </>
              )}

              <Button
                variant="ghost"
                size="sm"
                onClick={() => {
                  clearToken();
                  router.push("/login/");
                }}
                className={cn(
                  "w-full h-8 justify-start gap-3 text-muted-foreground hover:text-destructive hover:bg-destructive/10 group mt-1",
                  collapsed && "justify-center px-0"
                )}
              >
                <LogOut className="w-4 h-4 shrink-0 group-hover:-translate-x-0.5 transition-transform" />
                {!collapsed && <span className="text-[13px] font-medium">Logout</span>}
              </Button>
            </div>
          </aside>

          <main className="flex-1 overflow-auto h-full allow-select bg-background">
            <div className="mx-auto max-w-7xl p-6 h-full openclaw-dashboard-enter">{children}</div>
          </main>
        </div>
      </TooltipProvider>
    </OrgScopeProvider>
  );
}
