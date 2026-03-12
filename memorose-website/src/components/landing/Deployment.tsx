import { Card } from "@/components/ui/Card";
import { Button } from "@/components/ui/Button";
import { Container, Binary, Cloud } from "lucide-react";

const deployOptions = [
  {
    icon: Container,
    title: "Docker",
    description:
      "Single-command deployment with Docker Compose. Includes dashboard and all dependencies.",
    command: "docker compose up -d",
  },
  {
    icon: Binary,
    title: "Binary",
    description:
      "Download a single static binary for Linux or macOS. Zero dependencies, just run it.",
    command: "curl -fsSL https://memorose.dev/install.sh | sh",
  },
  {
    icon: Cloud,
    title: "Memorose Cloud",
    description:
      "Managed hosting with automatic scaling, backups, and monitoring. Coming soon.",
    command: null,
  },
];

export function Deployment() {
  return (
    <section className="py-20 lg:py-28 border-t border-border">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-14">
          <h2 className="text-3xl sm:text-4xl font-bold">
            Deploy in 60 Seconds
          </h2>
          <p className="mt-4 text-muted-foreground max-w-2xl mx-auto">
            Get Memorose running however you prefer.
          </p>
        </div>

        <div className="grid sm:grid-cols-3 gap-5 max-w-4xl mx-auto">
          {deployOptions.map((opt) => (
            <Card key={opt.title} hover>
              <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center mb-4">
                <opt.icon className="w-5 h-5 text-primary" />
              </div>
              <h3 className="font-semibold mb-2">{opt.title}</h3>
              <p className="text-sm text-muted-foreground mb-4">
                {opt.description}
              </p>
              {opt.command ? (
                <code className="block text-xs bg-secondary px-3 py-2 rounded-md font-mono text-muted-foreground overflow-x-auto">
                  {opt.command}
                </code>
              ) : (
                <Button variant="secondary" size="sm" className="w-full" href="#">
                  Join Waitlist
                </Button>
              )}
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
}
