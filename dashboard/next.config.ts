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
};

export default nextConfig;
