import type { Metadata } from "next";
import { GeistSans } from "geist/font/sans";
import { GeistMono } from "geist/font/mono";
import { Navbar } from "@/components/Navbar";
import { Footer } from "@/components/Footer";
import "./globals.css";

export const metadata: Metadata = {
  title: "Memorose — Long-Term Memory for AI Agents",
  description:
    "Open-source, self-hosted memory layer for AI agents. Hybrid vector + graph search, multi-tenant, Raft-replicated. Give your agents perfect recall.",
  keywords: [
    "AI memory",
    "agent memory",
    "vector database",
    "knowledge graph",
    "LLM memory",
    "Memorose",
    "open source",
  ],
  openGraph: {
    title: "Memorose — Long-Term Memory for AI Agents",
    description:
      "Open-source, self-hosted memory layer for AI agents. Hybrid vector + graph search, multi-tenant, Raft-replicated.",
    type: "website",
    url: "https://memorose.dev",
  },
  twitter: {
    card: "summary_large_image",
    title: "Memorose — Long-Term Memory for AI Agents",
    description:
      "Open-source, self-hosted memory layer for AI agents. Hybrid search, multi-tenant, Raft-replicated.",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body
        className={`${GeistSans.variable} ${GeistMono.variable} antialiased`}
      >
        <Navbar />
        <main className="min-h-screen">{children}</main>
        <Footer />
      </body>
    </html>
  );
}
