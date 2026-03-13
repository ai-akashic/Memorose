"use client";

import { useLocale, useTranslations } from "next-intl";
import { usePathname, useRouter } from "@/i18n/routing";
import { Button } from "@/components/ui/button";
import { Globe } from "lucide-react";

export function LocaleSwitcher() {
  const t = useTranslations("LocaleSwitcher");
  const locale = useLocale();
  const router = useRouter();
  const pathname = usePathname();

  function toggleLocale() {
    const nextLocale = locale === "en" ? "zh" : "en";
    router.replace(pathname, { locale: nextLocale });
  }

  return (
    <Button
      variant="ghost"
      size="sm"
      onClick={toggleLocale}
      className="gap-2 text-muted-foreground hover:text-foreground"
    >
      <Globe className="w-4 h-4 shrink-0" />
      <span className="text-[13px] font-medium">{t(locale === "en" ? "zh" : "en")}</span>
    </Button>
  );
}
