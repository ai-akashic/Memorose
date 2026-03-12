import { Card } from "@/components/ui/Card";
import { benchmarks } from "@/data/benchmarks";

export function Benchmarks() {
  return (
    <section className="py-20 lg:py-28 border-t border-border">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-14">
          <h2 className="text-3xl sm:text-4xl font-bold">
            Built for Speed
          </h2>
          <p className="mt-4 text-muted-foreground max-w-2xl mx-auto">
            Rust-powered performance. No garbage collector, no interpreter overhead.
          </p>
        </div>

        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5 max-w-4xl mx-auto">
          {benchmarks.map((b) => (
            <Card key={b.label} className="text-center">
              <div className="text-3xl font-bold text-primary mb-1">
                {b.value}
              </div>
              <div className="text-sm font-medium text-muted-foreground mb-2">
                {b.unit}
              </div>
              <div className="text-xs text-muted-foreground/70">
                {b.description}
              </div>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
}
