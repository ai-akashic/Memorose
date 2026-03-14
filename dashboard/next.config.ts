import createNextIntlPlugin from 'next-intl/plugin';
import type { NextConfig } from "next";

const withNextIntl = createNextIntlPlugin();

const nextConfig: NextConfig = {
  output: 'export',
  basePath: "/dashboard",
  trailingSlash: true,
  images: { unoptimized: true },
  async rewrites() {
    return [
      {
        source: "/v1/:path*",
        destination: "http://127.0.0.1:3000/v1/:path*",
      },
    ];
  },
};

export default withNextIntl(nextConfig);