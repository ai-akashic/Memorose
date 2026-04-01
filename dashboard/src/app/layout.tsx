import type { Metadata } from "next";
import { cookies } from "next/headers";
import { GeistSans } from "geist/font/sans";
import { GeistMono } from "geist/font/mono";
import "./globals.css";
import { ClientIntlProvider } from "@/components/ClientIntlProvider";
import {
  DASHBOARD_LOCALE_COOKIE_KEY,
  type DashboardLocale,
  normalizeDashboardLocale,
} from "@/lib/locale";

export const metadata: Metadata = {
  title: "Memorose Control Plane",
  description: "Inspect cluster state, memories, tasks, and organization knowledge in Memorose.",
};

export default async function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const cookieStore = await cookies();
  const locale = normalizeDashboardLocale(
    cookieStore.get(DASHBOARD_LOCALE_COOKIE_KEY)?.value as DashboardLocale | undefined
  );

  return (
    <html lang={locale} className="dark" suppressHydrationWarning>
      <body
        className={`${GeistSans.variable} ${GeistMono.variable} antialiased h-screen overflow-hidden`}
      >
        <ClientIntlProvider locale={locale}>
          {children}
        </ClientIntlProvider>
      </body>
    </html>
  );
}
