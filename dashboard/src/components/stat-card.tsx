"use client";

import { useEffect, useState } from "react";
import { formatNumber } from "@/lib/utils";
import { Card, CardContent } from "@/components/ui/card";
import { motion } from "framer-motion";

const COLOR_GRADIENTS: Record<string, string> = {
  "text-primary": "from-primary/[0.04] to-transparent",
  "text-success": "from-success/[0.04] to-transparent",
  "text-warning": "from-warning/[0.04] to-transparent",
  "text-destructive": "from-destructive/[0.04] to-transparent",
  "text-accent": "from-accent/[0.04] to-transparent",
};

function NumberTicker({ value }: { value: number }) {
  const [displayValue, setDisplayValue] = useState(0);

  useEffect(() => {
    let startTimestamp: number | null = null;
    const duration = 1000;
    const startValue = displayValue;

    const step = (timestamp: number) => {
      if (!startTimestamp) startTimestamp = timestamp;
      const progress = Math.min((timestamp - startTimestamp) / duration, 1);
      const easeOutQuart = 1 - Math.pow(1 - progress, 4);
      const current = Math.floor(startValue + (value - startValue) * easeOutQuart);
      setDisplayValue(current);
      if (progress < 1) {
        window.requestAnimationFrame(step);
      }
    };
    window.requestAnimationFrame(step);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value]);

  return <span>{formatNumber(displayValue)}</span>;
}

export function StatCard({
  label,
  value,
  icon: Icon,
  color = "text-primary",
  className = "",
  delay = 0,
  compact = false,
}: {
  label: string;
  value: string | number;
  icon: React.ElementType;
  color?: string;
  className?: string;
  delay?: number;
  compact?: boolean;
}) {
  const gradient = COLOR_GRADIENTS[color] || COLOR_GRADIENTS["text-primary"];

  return (
    <motion.div
      initial={{ opacity: 0, y: 15 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay, ease: "easeOut" }}
      className={`h-full ${className}`}
    >
      <Card className={`glass-card group relative overflow-hidden transition-all duration-500 h-full bg-gradient-to-br ${gradient}`}>
        <div className="absolute inset-0 bg-[linear-gradient(110deg,transparent_30%,rgba(255,255,255,0.03)_50%,transparent_70%)] translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700 pointer-events-none" />
        <CardContent className={`relative z-10 flex h-full flex-col ${compact ? "gap-2.5 p-3.5" : "gap-4 p-5"}`}>
          <div className={`flex items-center ${compact ? "gap-1.5" : "gap-2"}`}>
            <Icon className={`${compact ? "h-3.5 w-3.5" : "h-4 w-4"} ${color} opacity-60 group-hover:opacity-100 transition-opacity shrink-0`} />
            <span className={`${compact ? "text-[9px]" : "text-[10px]"} font-semibold uppercase tracking-wider text-muted-foreground truncate`}>
              {label}
            </span>
          </div>
          <div className={`${compact ? "text-2xl xl:text-[1.65rem]" : "text-3xl"} font-bold tracking-tighter font-mono text-foreground/90 transition-colors group-hover:text-white`}>
            {typeof value === "number" ? <NumberTicker value={value} /> : value}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  );
}
