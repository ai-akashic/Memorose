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
    <div className={cn("flex flex-col items-center justify-center py-16 px-6 text-center", className)}>
      <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-primary/8 border border-primary/15 shadow-[0_0_24px_rgba(255,122,87,0.1)] mb-4">
        <Icon className="h-6 w-6 text-primary/50" />
      </div>
      <p className="text-sm font-medium text-foreground/70">{title}</p>
      {description && (
        <p className="mt-1.5 max-w-xs text-xs text-muted-foreground/60">{description}</p>
      )}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}
