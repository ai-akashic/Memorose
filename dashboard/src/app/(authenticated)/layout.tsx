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
  Command,
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
import { MemoroseLogo } from "@/components/memorose-logo";

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
              "flex flex-col shrink-0 border-r border-white/8 bg-[linear-gradient(180deg,rgba(34,20,18,0.94),rgba(18,12,12,0.94))] backdrop-blur-xl shadow-[10px_0_42px_rgba(0,0,0,0.28)] transition-all duration-300 z-30 h-full overflow-hidden",
              collapsed ? "w-[84px]" : "w-[286px]"
            )}
          >
            <div className={cn("border-b border-white/8", collapsed ? "px-3 py-4" : "px-4 py-5")}>
              {collapsed ? (
                <div className="flex justify-center">
                  <div className="flex h-12 w-12 items-center justify-center rounded-[1.25rem] border border-white/10 bg-white/[0.04] shadow-[0_10px_30px_rgba(0,0,0,0.2)]">
                    <MemoroseLogo size={28} />
                  </div>
                </div>
              ) : (
                <div className="dashboard-panel rounded-[1.5rem] px-4 py-4">
                  <div className="flex items-center gap-3">
                    <div className="flex h-12 w-12 items-center justify-center rounded-[1.2rem] border border-white/10 bg-white/[0.04]">
                      <MemoroseLogo size={30} />
                    </div>
                    <div className="min-w-0">
                      <p className="text-[11px] font-medium uppercase tracking-[0.24em] text-muted-foreground">
                        Memorose
                      </p>
                      <p className="truncate text-[1.05rem] font-semibold tracking-[-0.03em] text-foreground">
                        Control Plane
                      </p>
                    </div>
                  </div>
                  <div className="mt-4 flex items-center justify-between rounded-[1rem] border border-white/8 bg-white/[0.03] px-3 py-2">
                    <div>
                      <p className="text-[10px] font-medium uppercase tracking-[0.22em] text-muted-foreground">
                        Shortcuts
                      </p>
                      <p className="mt-1 text-xs text-foreground/80">Command palette</p>
                    </div>
                    <div className="flex items-center gap-1 rounded-full border border-white/8 bg-background/80 px-2 py-1 text-[11px] text-muted-foreground">
                      <Command className="h-3 w-3" />
                      K
                    </div>
                  </div>
                </div>
              )}
            </div>

            <OrgSwitcher collapsed={collapsed} />

            <nav className="flex-1 py-5 space-y-2 px-3 overflow-y-auto">
              {navItems.map((item) => {
                const isActive = pathname === item.href || pathname?.startsWith(item.href);
                const link = (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      "relative flex items-center gap-3 rounded-[1.1rem] border px-3.5 py-3 text-[13.5px] transition-all duration-200 group overflow-hidden",
                      isActive
                        ? "border-primary/25 bg-primary/10 text-foreground shadow-[inset_0_1px_0_rgba(255,255,255,0.08),0_10px_25px_rgba(255,122,87,0.12)]"
                        : "border-transparent text-muted-foreground hover:border-white/10 hover:bg-white/[0.04] hover:text-foreground"
                    )}
                  >
                    {isActive && (
                      <span className="absolute inset-y-3 left-0 w-1 rounded-r-full bg-primary shadow-[0_0_18px_rgba(255,122,87,0.8)]" />
                    )}
                    <div
                      className={cn(
                        "flex h-9 w-9 shrink-0 items-center justify-center rounded-[0.95rem] border transition-colors",
                        isActive
                          ? "border-primary/20 bg-primary/12 text-primary"
                          : "border-white/8 bg-white/[0.03] text-muted-foreground group-hover:text-foreground"
                      )}
                    >
                      <item.icon className="h-4 w-4" />
                    </div>
                    {!collapsed && (
                      <div className="min-w-0">
                        <span className="block truncate font-medium">{item.label}</span>
                        <span className="block truncate text-[11px] uppercase tracking-[0.2em] text-muted-foreground/80">
                          Memorose
                        </span>
                      </div>
                    )}
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

            <div className="border-t border-white/8 p-3 space-y-2 bg-background/50 flex flex-col items-center">
              {!collapsed ? (
                <div className="w-full flex justify-between items-center">
                  <LocaleSwitcher />
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setCollapsed(!collapsed)}
                    className="h-9 w-9 p-0 text-muted-foreground hover:text-foreground hover:bg-white/[0.06]"
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
                    className="w-full h-9 justify-center px-0 text-muted-foreground hover:text-foreground hover:bg-white/[0.06]"
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
                  "w-full h-10 justify-start gap-3 rounded-[1rem] text-muted-foreground hover:text-destructive hover:bg-destructive/10 group",
                  collapsed && "justify-center px-0"
                )}
              >
                <LogOut className="w-4 h-4 shrink-0 group-hover:-translate-x-0.5 transition-transform" />
                {!collapsed && <span className="text-[13px] font-medium">{t("logout")}</span>}
              </Button>
            </div>
          </aside>

          <main className="flex-1 overflow-y-auto overflow-x-hidden h-full allow-select bg-transparent relative z-0 flex flex-col">
            <div className="absolute inset-0 z-[-1] bg-grid-white/[0.02] bg-[size:50px_50px] pointer-events-none opacity-60" />
            <div className="absolute left-[-10%] top-[-8%] z-[-1] h-[28rem] w-[28rem] rounded-full bg-[radial-gradient(circle,rgba(255,128,92,0.18),transparent_65%)] blur-3xl pointer-events-none" />
            <div className="absolute right-[-8%] top-[10%] z-[-1] h-[24rem] w-[24rem] rounded-full bg-[radial-gradient(circle,rgba(255,188,110,0.12),transparent_65%)] blur-3xl pointer-events-none" />
            <div className="absolute inset-0 z-[-1] bg-gradient-to-b from-transparent via-background/24 to-background/72 pointer-events-none" />
            <div className="mx-auto flex w-full max-w-[92rem] flex-1 flex-col px-5 py-5 md:px-7 md:py-7 openclaw-dashboard-enter relative">
              {children}
            </div>
          </main>
        </div>
      </TooltipProvider>
    </OrgScopeProvider>
  );
}
