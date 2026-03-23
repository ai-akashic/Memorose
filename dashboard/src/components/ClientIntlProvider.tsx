"use client";

import { NextIntlClientProvider } from "next-intl";
import { useEffect, useState } from "react";
import enMessages from "../../messages/en.json";
import zhMessages from "../../messages/zh.json";
import {
  DASHBOARD_LOCALE_STORAGE_KEY,
  type DashboardLocale,
  normalizeDashboardLocale,
} from "@/lib/locale";

export function ClientIntlProvider({ children }: { children: React.ReactNode }) {
  const [locale] = useState<DashboardLocale>(() =>
    normalizeDashboardLocale(
      typeof window === "undefined"
        ? "en"
        : window.localStorage.getItem(DASHBOARD_LOCALE_STORAGE_KEY)
    )
  );

  useEffect(() => {
    document.documentElement.lang = locale;
  }, [locale]);

  if (!locale) {
    return <div className="h-screen w-screen bg-background" />;
  }

  const messages = locale === "zh" ? zhMessages : enMessages;

  return (
    <NextIntlClientProvider locale={locale} messages={messages}>
      {children}
    </NextIntlClientProvider>
  );
}
