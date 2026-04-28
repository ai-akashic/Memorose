import type { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export function EmptyState({
  icon: Icon,
  title,
  description,
  action,
  className,
}: {
  icon: LucideIcon;
  title: string;
  description?: string;
  action?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("flex flex-col items-center justify-center py-14 px-6 text-center", className)}>
      <div className="mb-3 flex h-11 w-11 items-center justify-center rounded-xl bg-white/[0.035]">
        <Icon className="h-5 w-5 text-primary/55" />
      </div>
      <p className="text-sm font-medium text-foreground/70">{title}</p>
      {description && (
        <p className="mt-1 max-w-xs text-xs text-muted-foreground/55">{description}</p>
      )}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}
