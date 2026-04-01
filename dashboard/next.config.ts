import type { NextConfig } from "next";

const apiOrigin = (process.env.DASHBOARD_API_ORIGIN || "http://127.0.0.1:3000").replace(/\/+$/, "");

const nextConfig: NextConfig = {
  output: "standalone",
  basePath: "/dashboard",
  trailingSlash: true,
  images: { unoptimized: true },
  async rewrites() {
    return {
      beforeFiles: [
        {
          source: "/v1/:path*",
          destination: `${apiOrigin}/v1/:path*`,
          basePath: false,
        },
        {
          source: "/dashboard/v1/:path*",
          destination: `${apiOrigin}/v1/:path*`,
        },
      ],
    };
  },
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
};

export default nextConfig;
