import { cn } from "@/lib/utils";

const STATUS_STYLES = {
  healthy: "bg-success shadow-[0_0_8px_rgba(34,197,94,0.5)]",
  warning: "bg-warning shadow-[0_0_8px_rgba(245,158,11,0.5)]",
  error: "bg-destructive shadow-[0_0_8px_rgba(220,38,38,0.5)]",
};

const SIZE_STYLES = {
  sm: "w-1.5 h-1.5",
  md: "w-2 h-2",
};

export function StatusDot({
  status,
  size = "md",
  className,
}: {
  status: "healthy" | "warning" | "error";
  size?: "sm" | "md";
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-block rounded-full animate-[glow-pulse_2s_ease-in-out_infinite]",
        STATUS_STYLES[status],
        SIZE_STYLES[size],
        className
      )}
    />
  );
}
