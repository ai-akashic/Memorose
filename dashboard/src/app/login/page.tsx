"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { LoginForm } from "@/components/login-form";
import { isAuthenticated } from "@/lib/auth";
import { MemoroseLogo } from "@/components/haku-logo";

export default function LoginPage() {
  const router = useRouter();

  useEffect(() => {
    if (isAuthenticated()) {
      router.push("/cluster/");
    }
  }, [router]);

  return (
    <div className="relative flex min-h-screen flex-col items-center justify-center bg-gradient-to-br from-background via-background to-primary/5 p-6 md:p-10">
      {/* Background decoration */}
      <div className="absolute inset-0 bg-grid-white/[0.02] bg-[size:50px_50px]" />
      <div className="absolute inset-0 flex items-center justify-center">
        <div className="h-[800px] w-[800px] rounded-full bg-primary/10 blur-3xl" />
      </div>

      <div className="relative w-full max-w-sm">
        {/* Logo */}
        <div className="mb-12 flex flex-col items-center">
          <div className="flex h-24 w-24 items-center justify-center rounded-3xl bg-gradient-to-br from-primary/20 to-primary/5 backdrop-blur-sm border border-primary/30 shadow-2xl">
            <MemoroseLogo size={48} />
          </div>
          <h1 className="text-3xl font-bold tracking-tight mt-6">Memorose</h1>
        </div>

        <LoginForm />
      </div>
    </div>
  );
}
