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
    <div className={cn("z-10 relative mb-2", className)}>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-4 mr-auto min-w-0">
          {Icon && (
            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-primary/10 border border-primary/20 shadow-[0_0_24px_rgba(255,122,87,0.15)]">
              <Icon className="h-[18px] w-[18px] text-primary" />
            </div>
          )}
          <div className="min-w-0">
            {kicker && (
              <span className="text-[10px] font-bold uppercase tracking-widest text-primary/70">{kicker}</span>
            )}
            {title && <h1 className="text-2xl font-bold tracking-tight text-foreground">{title}</h1>}
            {description && <p className="mt-0.5 text-sm text-muted-foreground">{description}</p>}
          </div>
        </div>
        {children}
        {actions ? <div className="flex shrink-0 items-center gap-3">{actions}</div> : null}
      </div>
      <div className="mt-4 h-px bg-gradient-to-r from-transparent via-primary/20 to-transparent" />
    </div>
  );
}
