import AppDetailClient from "./client";

// Required for static export with dynamic routes
// Provide at least one path to satisfy Next.js static export
export async function generateStaticParams() {
  return [{ app_id: "placeholder" }];
}

export default function AppDetailPage() {
  return <AppDetailClient />;
}
