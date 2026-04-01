"use client";

import { Button } from "@/components/ui/button";
import { useTranslations } from "next-intl";
import { Languages } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  DASHBOARD_LOCALE_COOKIE_KEY,
  DASHBOARD_LOCALE_STORAGE_KEY,
  type DashboardLocale,
} from "@/lib/locale";

export function LocaleSwitcher() {
  const t = useTranslations("LocaleSwitcher");

  const switchLocale = (locale: DashboardLocale) => {
    localStorage.setItem(DASHBOARD_LOCALE_STORAGE_KEY, locale);
    document.cookie = `${DASHBOARD_LOCALE_COOKIE_KEY}=${locale}; Path=/; Max-Age=31536000; SameSite=Lax`;
    window.location.reload();
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon" className="h-8 w-8 text-muted-foreground hover:text-foreground">
          <Languages className="w-4 h-4" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="bg-popover border-border">
        <DropdownMenuItem onClick={() => switchLocale("en")} className="text-xs cursor-pointer focus:bg-muted">
          {t("en")}
        </DropdownMenuItem>
        <DropdownMenuItem onClick={() => switchLocale("zh")} className="text-xs cursor-pointer focus:bg-muted">
          {t("zh")}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
