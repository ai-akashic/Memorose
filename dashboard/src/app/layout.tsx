import type { Metadata } from "next";
import { GeistSans } from "geist/font/sans";
import { GeistMono } from "geist/font/mono";
import "./globals.css";
import { ClientIntlProvider } from "@/components/ClientIntlProvider";

export const metadata: Metadata = {
  title: "Memorose Control Plane",
  description: "Inspect cluster state, memories, tasks, and organization knowledge in Memorose.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark" suppressHydrationWarning>
      <body
        className={`${GeistSans.variable} ${GeistMono.variable} antialiased h-screen overflow-hidden`}
      >
        <ClientIntlProvider>
          {children}
        </ClientIntlProvider>
      </body>
    </html>
  );
}
