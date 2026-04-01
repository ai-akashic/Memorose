"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { useTranslations } from "next-intl";
import { LoginForm } from "@/components/login-form";
import { isAuthenticated } from "@/lib/auth";
import { MemoroseLogo } from "@/components/memorose-logo";
import { motion } from "framer-motion";

export default function LoginPage() {
  const router = useRouter();
  const t = useTranslations("Playground");

  useEffect(() => {
    if (isAuthenticated()) {
      router.push("/cluster/");
    }
  }, [router]);

  return (
    <div className="relative flex min-h-screen flex-col items-center justify-center overflow-hidden px-6 py-10 md:px-10">
      <div className="absolute inset-0 bg-grid-white/[0.02] bg-[size:44px_44px]" />
      <div className="absolute left-[-8%] top-[-5%] h-[28rem] w-[28rem] rounded-full bg-[radial-gradient(circle,rgba(255,124,87,0.18),transparent_62%)] blur-3xl" />
      <div className="absolute right-[-6%] top-[8%] h-[24rem] w-[24rem] rounded-full bg-[radial-gradient(circle,rgba(255,194,120,0.16),transparent_58%)] blur-3xl" />
      <div className="absolute bottom-[-10%] left-1/2 h-[34rem] w-[34rem] -translate-x-1/2 rounded-full blob-bg opacity-30" />
      <div className="absolute inset-x-0 bottom-0 h-40 bg-gradient-to-t from-background to-transparent z-0" />

      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.8, ease: [0.16, 1, 0.3, 1] }}
        className="relative z-10 w-full max-w-[430px]"
      >
        <div className="mb-8 rounded-[2rem] border border-white/8 bg-white/[0.03] px-5 py-4 backdrop-blur-xl">
          <div className="flex items-center gap-3">
            <div className="flex h-12 w-12 items-center justify-center rounded-[1.2rem] border border-white/10 bg-white/[0.05]">
              <MemoroseLogo size={28} />
            </div>
            <div>
              <p className="text-[11px] font-medium uppercase tracking-[0.24em] text-muted-foreground">
                Memorose
              </p>
              <p className="text-sm text-foreground/80">Operational dashboard access</p>
            </div>
          </div>
        </div>

        <div className="mb-12 flex flex-col items-center gap-1">
          <motion.div
            whileHover={{ scale: 1.08 }}
            whileTap={{ scale: 0.96 }}
            transition={{ type: "spring", stiffness: 260, damping: 22 }}
            className="relative cursor-pointer"
          >
            <div className="absolute inset-0 -z-10 scale-[1.8] rounded-full bg-primary/25 blur-3xl pointer-events-none" />
            <MemoroseLogo size={72} />
          </motion.div>

          <h1 className="mt-7 text-[2.8rem] font-semibold leading-none tracking-[-0.06em] text-foreground">
            Memorose
          </h1>
          <p className="mt-2 text-center text-[0.8rem] font-medium uppercase tracking-[0.2em] text-muted-foreground/80">
            {t("tagline")}
          </p>
        </div>

        <LoginForm />
      </motion.div>
    </div>
  );
}
