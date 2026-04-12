import type { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export function DashboardHero({
  title,
  description,
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
  const hasContent = title || description || actions || children;
  if (!hasContent) return null;

  return (
    <div className={cn("flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between mb-2 z-10 relative", className)}>
      {(title || description) && (
        <div className="mr-auto min-w-0">
          {title && <h1 className="text-2xl font-bold tracking-tight text-foreground">{title}</h1>}
          {description && <p className="mt-1 text-sm text-muted-foreground">{description}</p>}
        </div>
      )}
      {children}
      {actions ? <div className="flex shrink-0 items-center gap-3">{actions}</div> : null}
    </div>
  );
}
