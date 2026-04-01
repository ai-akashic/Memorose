import type { LucideIcon } from "lucide-react";
import { cn, formatNumber } from "@/lib/utils";

type DashboardStatTone = "primary" | "success" | "warning" | "neutral";

const toneClassName: Record<DashboardStatTone, string> = {
  primary: "text-primary",
  success: "text-success",
  warning: "text-warning",
  neutral: "text-foreground",
};

export function DashboardHero({
  icon: Icon,
  kicker,
  title,
  description,
  actions,
  children,
  className,
}: {
  icon: LucideIcon;
  kicker: string;
  title: string;
  description?: string;
  actions?: React.ReactNode;
  children?: React.ReactNode;
  className?: string;
}) {
  return (
    <section
      className={cn(
        "dashboard-panel flex flex-col gap-6 px-6 py-6 sm:px-7 sm:py-7 lg:flex-row lg:items-end lg:justify-between",
        className
      )}
    >
      <div className="space-y-4">
        <div className="dashboard-kicker">
          <Icon className="h-3.5 w-3.5" />
          <span>{kicker}</span>
        </div>
        <div className="space-y-2">
          <h1 className="dashboard-title">{title}</h1>
          {description ? <p className="dashboard-copy">{description}</p> : null}
        </div>
        {children}
      </div>
      {actions ? <div className="flex shrink-0 items-center gap-3">{actions}</div> : null}
    </section>
  );
}

export function DashboardStatRail({
  items,
  className,
}: {
  items: Array<{
    label: string;
    value: React.ReactNode;
    tone?: DashboardStatTone;
  }>;
  className?: string;
}) {
  return (
    <div className={cn("flex flex-wrap gap-3", className)}>
      {items.map((item) => (
        <div key={item.label} className="dashboard-stat-pill">
          <span className="dashboard-stat-label">{item.label}</span>
          <span className={cn("dashboard-stat-value", toneClassName[item.tone ?? "neutral"])}>
            {typeof item.value === "number" ? formatNumber(item.value) : item.value}
          </span>
        </div>
      ))}
    </div>
  );
}
