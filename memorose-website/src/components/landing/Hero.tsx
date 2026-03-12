import { Button } from "@/components/ui/Button";
import { Badge } from "@/components/ui/Badge";
import { Github, ArrowRight } from "lucide-react";

export function Hero() {
  return (
    <section className="relative pt-32 pb-20 lg:pt-44 lg:pb-32 overflow-hidden">
      {/* Background effects */}
      <div className="absolute inset-0 pointer-events-none">
        <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[600px] rounded-full bg-primary/8 blur-[120px] animate-pulse-glow" />
        <div className="absolute bottom-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-border to-transparent" />
      </div>

      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
        <div className="animate-fade-in">
          <Badge className="mb-6">
            Now in Public Beta
          </Badge>
        </div>

        <h1 className="animate-fade-in text-4xl sm:text-5xl lg:text-7xl font-bold tracking-tight leading-[1.1] max-w-4xl mx-auto">
          Long-Term Memory{" "}
          <span className="bg-gradient-to-r from-primary via-violet-400 to-fuchsia-400 bg-clip-text text-transparent animate-gradient">
            for AI Agents
          </span>
        </h1>

        <p className="animate-fade-in-up mt-6 text-lg sm:text-xl text-muted-foreground max-w-2xl mx-auto leading-relaxed [animation-delay:100ms] opacity-0">
          Open-source, self-hosted memory layer with hybrid vector + graph search,
          multi-tenant isolation, and Raft replication. Written in Rust.
        </p>

        <div className="animate-fade-in-up flex flex-col sm:flex-row items-center justify-center gap-4 mt-10 [animation-delay:200ms] opacity-0">
          <Button size="lg" href="/docs/getting-started">
            Get Started
            <ArrowRight className="w-4 h-4 ml-2" />
          </Button>
          <Button
            variant="secondary"
            size="lg"
            href="https://github.com/memorose/memorose"
            target="_blank"
            rel="noopener noreferrer"
          >
            <Github className="w-5 h-5 mr-2" />
            View on GitHub
          </Button>
        </div>

        {/* Tech badges */}
        <div className="animate-fade-in-up flex flex-wrap items-center justify-center gap-3 mt-12 [animation-delay:300ms] opacity-0">
          {["Rust", "LanceDB", "Tantivy", "Raft", "Axum", "Next.js"].map(
            (tech) => (
              <span
                key={tech}
                className="px-3 py-1 text-xs rounded-full bg-secondary text-muted-foreground border border-border"
              >
                {tech}
              </span>
            )
          )}
        </div>
      </div>
    </section>
  );
}
