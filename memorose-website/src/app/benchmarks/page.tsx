import { Card } from "@/components/ui/Card";
import { benchmarks } from "@/data/benchmarks";

export default function BenchmarksPage() {
  return (
    <div className="pt-24 pb-20">
      <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-14">
          <h1 className="text-4xl font-bold mb-4">Benchmarks</h1>
          <p className="text-muted-foreground max-w-2xl mx-auto">
            Performance measurements on a single 8-core node with 1M stored
            memories. All numbers are p99 unless noted.
          </p>
        </div>

        <div className="grid sm:grid-cols-2 gap-6 mb-16">
          {benchmarks.map((b) => (
            <Card key={b.label}>
              <div className="text-4xl font-bold text-primary mb-1">
                {b.value}
              </div>
              <div className="text-sm font-medium mb-2">{b.unit}</div>
              <div className="text-sm text-muted-foreground">
                {b.description}
              </div>
            </Card>
          ))}
        </div>

        <Card>
          <h2 className="text-xl font-semibold mb-4">Methodology</h2>
          <div className="text-sm text-muted-foreground space-y-3">
            <p>
              Benchmarks were run on a single node with an 8-core AMD EPYC
              processor, 32 GB RAM, and NVMe SSD storage. The dataset consists
              of 1M memories with 384-dimensional embeddings.
            </p>
            <p>
              Search latency measures end-to-end hybrid search (vector + BM25
              fusion) including network overhead. Write throughput measures
              sustained ingestion with embedding generation disabled (pre-computed
              embeddings).
            </p>
            <p>
              Detailed benchmark scripts and reproducible configurations are
              available in the{" "}
              <a
                href="https://github.com/memorose/memorose/tree/main/benchmarks"
                className="text-primary underline underline-offset-4"
                target="_blank"
                rel="noopener noreferrer"
              >
                benchmarks/
              </a>{" "}
              directory.
            </p>
          </div>
        </Card>
      </div>
    </div>
  );
}
