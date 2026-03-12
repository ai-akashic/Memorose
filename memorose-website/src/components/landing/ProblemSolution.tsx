import { Card } from "@/components/ui/Card";
import { AlertTriangle, Sparkles } from "lucide-react";

export function ProblemSolution() {
  return (
    <section className="py-20 lg:py-28">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <h2 className="text-3xl sm:text-4xl font-bold">
            The Problem with AI Memory
          </h2>
          <p className="mt-4 text-muted-foreground max-w-2xl mx-auto">
            Today&apos;s AI agents forget everything between sessions. Your users
            repeat themselves. Your agents lose context. Memorose fixes this.
          </p>
        </div>

        <div className="grid md:grid-cols-2 gap-6 max-w-4xl mx-auto">
          {/* Problem */}
          <Card className="relative overflow-hidden border-red-500/20 bg-red-500/[0.03]">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 rounded-lg bg-red-500/10 flex items-center justify-center">
                <AlertTriangle className="w-5 h-5 text-red-400" />
              </div>
              <h3 className="text-lg font-semibold text-red-400">
                Without Memorose
              </h3>
            </div>
            <ul className="space-y-3 text-sm text-muted-foreground">
              <li className="flex items-start gap-2">
                <span className="text-red-400 mt-0.5">&#x2717;</span>
                Agents forget user preferences every session
              </li>
              <li className="flex items-start gap-2">
                <span className="text-red-400 mt-0.5">&#x2717;</span>
                No long-term knowledge accumulation
              </li>
              <li className="flex items-start gap-2">
                <span className="text-red-400 mt-0.5">&#x2717;</span>
                Context window stuffing wastes tokens
              </li>
              <li className="flex items-start gap-2">
                <span className="text-red-400 mt-0.5">&#x2717;</span>
                No multi-agent memory sharing
              </li>
              <li className="flex items-start gap-2">
                <span className="text-red-400 mt-0.5">&#x2717;</span>
                Vendor lock-in with cloud-only solutions
              </li>
            </ul>
          </Card>

          {/* Solution */}
          <Card className="relative overflow-hidden border-primary/20 bg-primary/[0.03]">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
                <Sparkles className="w-5 h-5 text-primary" />
              </div>
              <h3 className="text-lg font-semibold text-primary">
                With Memorose
              </h3>
            </div>
            <ul className="space-y-3 text-sm text-muted-foreground">
              <li className="flex items-start gap-2">
                <span className="text-green-400 mt-0.5">&#x2713;</span>
                Persistent memories across sessions and agents
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-400 mt-0.5">&#x2713;</span>
                Knowledge graph builds over time automatically
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-400 mt-0.5">&#x2713;</span>
                Hybrid search retrieves exactly what&apos;s needed
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-400 mt-0.5">&#x2713;</span>
                Multi-tenant isolation for SaaS platforms
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-400 mt-0.5">&#x2713;</span>
                Self-hosted, open-source, your data stays yours
              </li>
            </ul>
          </Card>
        </div>
      </div>
    </section>
  );
}
