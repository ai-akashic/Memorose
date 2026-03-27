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
  Bot,
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
  const tLayout = useTranslations("Layout");
  const [collapsed, setCollapsed] = useState(false);
  const [mounted, setMounted] = useState(false);

  const navItems = [
    { href: "/cluster/", label: t("cluster"), icon: LayoutDashboard },
    { href: "/organizations/", label: t("organizations"), icon: Building2 },
    { href: "/memories/", label: t("memories"), icon: Database },
    { href: "/playground/", label: t("playground"), icon: MessageSquare },
    { href: "/agents/", label: t("agents"), icon: Bot },
    { href: "/metrics/", label: t("metrics"), icon: BarChart3 },
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
        <div className="animate-pulse text-muted-foreground text-sm">{tLayout("loading")}</div>
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
              "flex flex-col shrink-0 border-r border-white/5 bg-background/80 backdrop-blur-xl shadow-[4px_0_24px_rgba(0,0,0,0.2)] transition-all duration-300 z-30 h-full overflow-hidden",
              collapsed ? "w-[72px]" : "w-[220px]"
            )}
          >
            <OrgSwitcher collapsed={collapsed} />

            <nav className="flex-1 py-4 space-y-1.5 px-3 overflow-y-auto">
              {navItems.map((item) => {
                const isActive = pathname === item.href || pathname?.startsWith(item.href);
                const link = (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      "relative flex items-center gap-3 px-3 py-2.5 rounded-lg text-[13.5px] transition-all duration-200 group overflow-hidden",
                      isActive
                        ? "text-primary font-medium bg-primary/10 border border-primary/20 shadow-[inset_0_1px_0_rgba(255,255,255,0.1)]"
                        : "text-muted-foreground hover:text-foreground hover:bg-white/5 border border-transparent hover:border-white/10 hover:shadow-sm"
                    )}
                  >
                    {isActive && (
                      <span className="absolute left-0 top-1/2 -translate-y-1/2 w-1 h-5 bg-primary rounded-r-full shadow-[0_0_10px_rgba(255,92,92,0.8)]" />
                    )}
                    <item.icon className={cn("w-4 h-4 shrink-0", isActive ? "text-primary" : "opacity-50 group-hover:opacity-100")} />
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
                {!collapsed && <span className="text-[13px] font-medium">{t("logout")}</span>}
              </Button>
            </div>
          </aside>

          <main className="flex-1 overflow-y-auto overflow-x-hidden h-full allow-select bg-background relative z-0 flex flex-col">
            <div className="absolute inset-0 z-[-1] bg-grid-white/[0.02] bg-[size:50px_50px] pointer-events-none" />
            <div className="absolute inset-0 z-[-1] bg-gradient-to-tr from-background via-background/90 to-background/20 pointer-events-none" />
            <div className="mx-auto max-w-7xl p-6 md:p-8 flex-1 flex flex-col w-full openclaw-dashboard-enter relative">{children}</div>
          </main>
        </div>
      </TooltipProvider>
    </OrgScopeProvider>
  );
}
