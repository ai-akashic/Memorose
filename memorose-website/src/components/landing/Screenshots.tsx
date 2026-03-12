import { Card } from "@/components/ui/Card";

const screenshots = [
  {
    title: "Memory Explorer",
    description: "Browse and search all stored memories with rich metadata",
  },
  {
    title: "Knowledge Graph",
    description: "Visualize entity relationships extracted from memories",
  },
  {
    title: "Agent Activity",
    description: "Monitor agent memory operations in real time",
  },
  {
    title: "Cluster Health",
    description: "Track Raft consensus and node status across your cluster",
  },
];

export function Screenshots() {
  return (
    <section className="py-20 lg:py-28 border-t border-border">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-14">
          <h2 className="text-3xl sm:text-4xl font-bold">
            Built-in Dashboard
          </h2>
          <p className="mt-4 text-muted-foreground max-w-2xl mx-auto">
            Monitor, explore, and debug your memory layer with the included web
            dashboard. No extra tooling required.
          </p>
        </div>

        <div className="grid sm:grid-cols-2 gap-5 max-w-4xl mx-auto">
          {screenshots.map((s) => (
            <Card key={s.title} hover>
              {/* Placeholder for screenshot */}
              <div className="aspect-video bg-secondary/50 rounded-lg border border-border mb-4 flex items-center justify-center">
                <span className="text-xs text-muted-foreground">
                  Screenshot: {s.title}
                </span>
              </div>
              <h3 className="font-semibold mb-1">{s.title}</h3>
              <p className="text-sm text-muted-foreground">{s.description}</p>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
}
