import { ReactNode } from "react";

export function Card({
  children,
  className = "",
  hover = false,
}: {
  children: ReactNode;
  className?: string;
  hover?: boolean;
}) {
  return (
    <div
      className={`bg-card border border-border rounded-xl p-6 ${
        hover ? "hover:border-primary/40 hover:shadow-lg hover:shadow-primary/5 transition-all duration-300" : ""
      } ${className}`}
    >
      {children}
    </div>
  );
}
