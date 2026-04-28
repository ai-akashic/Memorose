import type { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export function DashboardHero({
  icon: Icon,
  kicker,
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
    <div className={cn("z-10 relative mb-3", className)}>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-3 mr-auto min-w-0">
          {Icon && (
            <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-white/[0.035] text-primary/80">
              <Icon className="h-[16px] w-[16px]" />
            </div>
          )}
          <div className="min-w-0">
            {kicker && (
              <span className="text-[10px] font-semibold uppercase tracking-wide text-primary/65">{kicker}</span>
            )}
            {title && <h1 className="text-xl font-semibold tracking-tight text-foreground">{title}</h1>}
            {description && <p className="sr-only">{description}</p>}
          </div>
        </div>
        {children}
        {actions ? <div className="flex shrink-0 items-center gap-3">{actions}</div> : null}
      </div>
    </div>
  );
}
