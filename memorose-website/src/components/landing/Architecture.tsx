export function Architecture() {
  const tiers = [
    {
      level: "L0",
      name: "Hot Cache",
      description: "In-memory LRU for sub-ms reads",
      color: "from-red-500 to-orange-500",
      items: ["LRU Cache", "Recent queries", "Active sessions"],
    },
    {
      level: "L1",
      name: "Warm Index",
      description: "LanceDB vectors + Tantivy full-text",
      color: "from-primary to-violet-500",
      items: ["Vector embeddings", "BM25 index", "Knowledge graph"],
    },
    {
      level: "L2",
      name: "Cold Storage",
      description: "Compressed archive with bitemporal versioning",
      color: "from-blue-500 to-cyan-500",
      items: ["Bitemporal history", "Compressed data", "Raft-replicated"],
    },
  ];

  return (
    <section className="py-20 lg:py-28 border-t border-border">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-14">
          <h2 className="text-3xl sm:text-4xl font-bold">
            Tiered Storage Architecture
          </h2>
          <p className="mt-4 text-muted-foreground max-w-2xl mx-auto">
            Memories flow from hot cache to warm index to cold storage.
            Each tier optimized for its access pattern.
          </p>
        </div>

        <div className="max-w-3xl mx-auto space-y-4">
          {tiers.map((tier, i) => (
            <div
              key={tier.level}
              className="group relative"
              style={{ animationDelay: `${i * 150}ms` }}
            >
              <div className="relative bg-card border border-border rounded-xl p-6 hover:border-primary/30 transition-all duration-300">
                <div className="flex items-start gap-5">
                  {/* Level badge */}
                  <div
                    className={`shrink-0 w-14 h-14 rounded-xl bg-gradient-to-br ${tier.color} flex items-center justify-center`}
                  >
                    <span className="text-white font-bold text-lg">
                      {tier.level}
                    </span>
                  </div>

                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-3 mb-1">
                      <h3 className="text-lg font-semibold">{tier.name}</h3>
                    </div>
                    <p className="text-sm text-muted-foreground mb-3">
                      {tier.description}
                    </p>
                    <div className="flex flex-wrap gap-2">
                      {tier.items.map((item) => (
                        <span
                          key={item}
                          className="px-2.5 py-1 text-xs bg-secondary rounded-md text-muted-foreground border border-border"
                        >
                          {item}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
              </div>

              {/* Connector line */}
              {i < tiers.length - 1 && (
                <div className="flex justify-center py-1">
                  <div className="w-px h-4 bg-border" />
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
