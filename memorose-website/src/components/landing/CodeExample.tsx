"use client";

import { useState } from "react";
import { Check, Copy } from "lucide-react";

const codeSnippet = `from memorose import Memorose

client = Memorose("http://localhost:3000")

# Store a memory
client.add(
    content="User prefers dark mode and Python",
    user_id="alice",
    app_id="my-app",
    metadata={"source": "preferences"}
)

# Retrieve relevant memories
results = client.search(
    query="What does the user prefer?",
    user_id="alice",
    limit=5
)

for memory in results:
    print(f"{memory.content} (score: {memory.score:.2f})")`;

export function CodeExample() {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(codeSnippet);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <section className="py-20 lg:py-28 border-t border-border">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="max-w-3xl mx-auto">
          <div className="text-center mb-10">
            <h2 className="text-3xl sm:text-4xl font-bold">
              5 Lines to Get Started
            </h2>
            <p className="mt-4 text-muted-foreground">
              Add persistent memory to your AI agent in minutes, not days.
            </p>
          </div>

          <div className="relative group">
            <div className="absolute -inset-px bg-gradient-to-b from-primary/20 to-transparent rounded-xl opacity-0 group-hover:opacity-100 transition-opacity" />
            <div className="relative bg-card border border-border rounded-xl overflow-hidden">
              {/* Title bar */}
              <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-secondary/30">
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 rounded-full bg-red-500/60" />
                  <div className="w-3 h-3 rounded-full bg-yellow-500/60" />
                  <div className="w-3 h-3 rounded-full bg-green-500/60" />
                  <span className="ml-2 text-xs text-muted-foreground">
                    quickstart.py
                  </span>
                </div>
                <button
                  onClick={handleCopy}
                  className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors px-2 py-1 rounded-md hover:bg-secondary"
                >
                  {copied ? (
                    <>
                      <Check className="w-3.5 h-3.5" />
                      Copied
                    </>
                  ) : (
                    <>
                      <Copy className="w-3.5 h-3.5" />
                      Copy
                    </>
                  )}
                </button>
              </div>

              {/* Code */}
              <pre className="p-5 overflow-x-auto text-sm leading-relaxed">
                <code className="font-mono text-muted-foreground">
                  {codeSnippet.split("\n").map((line, i) => (
                    <div key={i} className="table-row">
                      <span className="table-cell pr-4 text-right text-muted-foreground/30 select-none w-8">
                        {i + 1}
                      </span>
                      <span
                        className={
                          line.startsWith("#")
                            ? "text-green-400/70"
                            : line.includes("from ") || line.includes("import ")
                              ? "text-violet-400"
                              : line.includes('"')
                                ? "text-amber-300/80"
                                : ""
                        }
                      >
                        {line || "\n"}
                      </span>
                    </div>
                  ))}
                </code>
              </pre>
            </div>
          </div>

          <p className="mt-4 text-center text-sm text-muted-foreground">
            pip install memorose &middot; Works with any LLM framework
          </p>
        </div>
      </div>
    </section>
  );
}
