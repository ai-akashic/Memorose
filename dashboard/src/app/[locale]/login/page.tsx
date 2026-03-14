"use client";

import { useEffect } from "react";
import { useRouter } from "@/i18n/routing";
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
      {/* Centered violet glow */}
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-[55%] w-[600px] h-[600px] rounded-full bg-violet-600/10 blur-[120px] pointer-events-none" />
      {/* Top-right accent */}
      <div className="absolute top-0 right-0 w-[400px] h-[400px] rounded-full bg-indigo-500/5 blur-[100px] pointer-events-none" />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[800px] blob-bg opacity-20 pointer-events-none mix-blend-screen" />
      <div className="absolute bottom-0 left-0 right-0 h-1/2 bg-gradient-to-t from-background to-transparent z-0" />

      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.8, ease: [0.16, 1, 0.3, 1] }}
        className="relative z-10 w-full max-w-[380px]"
      >
        {/* Logo Section */}
        <div className="mb-12 flex flex-col items-center gap-1">
          <motion.div
            whileHover={{ scale: 1.08 }}
            whileTap={{ scale: 0.96 }}
            transition={{ type: "spring", stiffness: 260, damping: 22 }}
            className="relative cursor-pointer"
          >
            {/* Ambient glow */}
            <div className="absolute inset-0 -z-10 blur-3xl opacity-50 bg-violet-500/30 rounded-full scale-[1.8] pointer-events-none" />
            <MemoroseLogo size={72} />
          </motion.div>

          <h1 className="text-[2.6rem] font-bold tracking-tighter mt-7 bg-clip-text text-transparent bg-gradient-to-b from-white via-white/90 to-white/40 leading-none">
            Memorose
          </h1>
          <p className="text-muted-foreground/70 mt-2 text-[0.8rem] font-medium tracking-[0.2em] uppercase">
            Cognitive Infrastructure for AI
          </p>
        </div>

        <LoginForm />
      </motion.div>
    </div>
  );
}
