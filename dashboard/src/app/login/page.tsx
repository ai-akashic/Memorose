"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { LoginForm } from "@/components/login-form";
import { isAuthenticated } from "@/lib/auth";
import { MemoroseLogo } from "@/components/haku-logo";
import { motion } from "framer-motion";

export default function LoginPage() {
  const router = useRouter();

  useEffect(() => {
    if (isAuthenticated()) {
      router.push("/cluster/");
    }
  }, [router]);

  return (
    <div className="relative flex min-h-screen flex-col items-center justify-center bg-background p-6 md:p-10 overflow-hidden">
      {/* Dynamic Backgrounds */}
      <div className="absolute inset-0 bg-grid-white/[0.02] bg-[size:40px_40px]" />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[800px] blob-bg opacity-30 pointer-events-none mix-blend-screen" />
      <div className="absolute bottom-0 left-0 right-0 h-1/3 bg-gradient-to-t from-background to-transparent z-0" />

      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.8, ease: [0.16, 1, 0.3, 1] }}
        className="relative z-10 w-full max-w-[380px]"
      >
        {/* Logo Section */}
        <div className="mb-10 flex flex-col items-center">
          <motion.div 
            whileHover={{ scale: 1.05, rotate: 5 }}
            transition={{ type: "spring", stiffness: 300, damping: 20 }}
            className="relative flex h-24 w-24 items-center justify-center rounded-3xl bg-white/[0.03] backdrop-blur-xl border border-white/10 shadow-[inset_0_1px_0_rgba(255,255,255,0.2),0_8px_32px_rgba(0,0,0,0.5)] cursor-pointer group"
          >
            <div className="absolute inset-0 rounded-3xl bg-gradient-to-br from-primary/20 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
            <MemoroseLogo size={48} />
          </motion.div>
          <h1 className="text-4xl font-bold tracking-tighter mt-8 bg-clip-text text-transparent bg-gradient-to-b from-white to-white/50">
            Memorose
          </h1>
          <p className="text-muted-foreground mt-2 text-sm font-medium tracking-wide">
            Cognitive Infrastructure for AI
          </p>
        </div>

        <LoginForm />
      </motion.div>
    </div>
  );
}
