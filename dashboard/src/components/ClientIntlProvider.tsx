import { NextIntlClientProvider } from "next-intl";
import enMessages from "../../messages/en.json";
import zhMessages from "../../messages/zh.json";
import { type DashboardLocale, normalizeDashboardLocale } from "@/lib/locale";

export function ClientIntlProvider({
  children,
  locale,
}: {
  children: React.ReactNode;
  locale?: DashboardLocale | null;
}) {
  const normalizedLocale = normalizeDashboardLocale(locale);
  const messages = normalizedLocale === "zh" ? zhMessages : enMessages;

  return (
    <NextIntlClientProvider locale={normalizedLocale} messages={messages}>
      {children}
    </NextIntlClientProvider>
  );
}
