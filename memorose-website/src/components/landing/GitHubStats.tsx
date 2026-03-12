import { Button } from "@/components/ui/Button";
import { Github, Star, ArrowRight } from "lucide-react";

export function GitHubStats() {
  return (
    <section className="py-20 lg:py-28 border-t border-border">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
        <div className="max-w-2xl mx-auto">
          <div className="w-16 h-16 rounded-2xl bg-primary/10 flex items-center justify-center mx-auto mb-6">
            <Github className="w-8 h-8 text-primary" />
          </div>
          <h2 className="text-3xl sm:text-4xl font-bold mb-4">
            Open Source &amp; Community Driven
          </h2>
          <p className="text-muted-foreground mb-8 leading-relaxed">
            Memorose is Apache-2.0 licensed. Star us on GitHub to support the
            project, report issues, or contribute.
          </p>

          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <Button
              size="lg"
              href="https://github.com/memorose/memorose"
              target="_blank"
              rel="noopener noreferrer"
            >
              <Star className="w-5 h-5 mr-2" />
              Star on GitHub
            </Button>
            <Button variant="secondary" size="lg" href="/docs/getting-started">
              Read the Docs
              <ArrowRight className="w-4 h-4 ml-2" />
            </Button>
          </div>
        </div>
      </div>
    </section>
  );
}
