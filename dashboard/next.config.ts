import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  basePath: "/dashboard",
  trailingSlash: true,
  images: { unoptimized: true },
  async redirects() {
    return [
      {
        source: "/",
        destination: "/login/",
        permanent: false,
      },
      {
        source: "/tasks/",
        destination: "/memories/?tab=tasks",
        permanent: false,
      },
    ];
  },
  async rewrites() {
    const apiOrigin = process.env.DASHBOARD_API_ORIGIN || "http://127.0.0.1:3000";
    return [
      {
        source: "/v1/:path*",
        destination: `${apiOrigin}/v1/:path*`,
      },
    ];
  },
};

export default nextConfig;
