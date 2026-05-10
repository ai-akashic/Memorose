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
  const hasContent = actions || children;
  if (!hasContent) return null;

  return (
    <div className={cn("z-10 relative mb-3", className)}>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-end">
        {children}
        {actions ? <div className="flex shrink-0 items-center gap-3">{actions}</div> : null}
      </div>
    </div>
  );
}
