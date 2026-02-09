"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { LoginForm } from "@/components/login-form";
import { isAuthenticated } from "@/lib/auth";

export default function LoginPage() {
  const router = useRouter();

  useEffect(() => {
    if (isAuthenticated()) {
      router.push("/cluster");
    }
  }, [router]);

  return (
    <div className="flex min-h-screen flex-col items-center justify-center bg-white p-6 md:p-10">
      <div className="w-full max-w-[12rem]">
        <LoginForm />
      </div>
    </div>
  );
}
