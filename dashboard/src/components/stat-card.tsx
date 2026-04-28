"use client";

import { useEffect, useState } from "react";
import { formatNumber } from "@/lib/utils";
import { Card, CardContent } from "@/components/ui/card";
import { motion } from "framer-motion";

const COLOR_GRADIENTS: Record<string, string> = {
  "text-primary": "from-transparent to-transparent",
  "text-success": "from-transparent to-transparent",
  "text-warning": "from-transparent to-transparent",
  "text-destructive": "from-transparent to-transparent",
  "text-accent": "from-transparent to-transparent",
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
      transition={{ duration: 0.3, delay, ease: "easeOut" }}
      className={`h-full ${className}`}
    >
      <Card className={`glass-card group relative overflow-hidden h-full bg-gradient-to-br ${gradient}`}>
        <CardContent className={`relative z-10 flex h-full flex-col ${compact ? "gap-2.5 p-3.5" : "gap-4 p-5"}`}>
          <div className={`flex items-center ${compact ? "gap-1.5" : "gap-2"}`}>
            <Icon className={`${compact ? "h-3.5 w-3.5" : "h-4 w-4"} ${color} opacity-55 shrink-0`} />
            <span className={`${compact ? "text-[9px]" : "text-[10px]"} font-semibold uppercase tracking-wide text-muted-foreground/75 truncate`}>
              {label}
            </span>
          </div>
          <div className={`${compact ? "text-2xl xl:text-[1.65rem]" : "text-3xl"} font-semibold tracking-tight text-foreground/90`}>
            {typeof value === "number" ? <NumberTicker value={value} /> : value}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  );
}
