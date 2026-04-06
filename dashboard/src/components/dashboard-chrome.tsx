import type { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export function DashboardHero({
  actions,
  children,
  className,
}: {
  icon?: LucideIcon;
  kicker?: string;
  title?: string;
  description?: string;
  actions?: React.ReactNode;
  children?: React.ReactNode;
  className?: string;
}) {
  if (!actions && !children) return null;
  
  return (
    <div className={cn("flex flex-col sm:flex-row sm:items-center sm:justify-end mb-2 z-10 relative", className)}>
      {actions ? <div className="flex shrink-0 items-center gap-3">{actions}</div> : null}
      {children}
    </div>
  );
}


