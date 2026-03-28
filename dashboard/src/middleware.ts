import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(request: NextRequest) {
  const { pathname, search } = request.nextUrl;

  if (pathname.startsWith("/v1/")) {
    const apiOrigin = process.env.DASHBOARD_API_ORIGIN || "http://127.0.0.1:3000";
    
    // Construct the destination URL
    // e.g. pathname="/v1/dashboard/auth/login" -> "http://backend:3000/v1/dashboard/auth/login"
    const targetUrl = new URL(pathname + search, apiOrigin);
    
    return NextResponse.rewrite(targetUrl);
  }
}

export const config = {
  // Matches /v1/* under the basePath
  matcher: ["/v1/:path*"],
};
