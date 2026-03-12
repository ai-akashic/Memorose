import { comparisonData } from "@/data/comparison";
import { Check, X, Minus } from "lucide-react";

function CellValue({ value }: { value: string | boolean }) {
  if (value === true)
    return <Check className="w-4 h-4 text-green-400 mx-auto" />;
  if (value === false)
    return <X className="w-4 h-4 text-red-400/60 mx-auto" />;
  return (
    <span className="text-sm text-muted-foreground">{value}</span>
  );
}

export function ComparisonTable() {
  return (
    <section className="py-20 lg:py-28 border-t border-border">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-14">
          <h2 className="text-3xl sm:text-4xl font-bold">
            How Memorose Compares
          </h2>
          <p className="mt-4 text-muted-foreground max-w-2xl mx-auto">
            The most complete open-source memory layer for AI agents.
          </p>
        </div>

        <div className="max-w-4xl mx-auto overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border">
                <th className="text-left py-3 px-4 font-medium text-muted-foreground">
                  Feature
                </th>
                <th className="py-3 px-4 font-semibold text-primary text-center bg-primary/5 rounded-t-lg">
                  Memorose
                </th>
                <th className="py-3 px-4 font-medium text-muted-foreground text-center">
                  Mem0
                </th>
                <th className="py-3 px-4 font-medium text-muted-foreground text-center">
                  Zep
                </th>
                <th className="py-3 px-4 font-medium text-muted-foreground text-center">
                  ChromaDB
                </th>
              </tr>
            </thead>
            <tbody>
              {comparisonData.map((row) => (
                <tr key={row.feature} className="border-b border-border/50">
                  <td className="py-3 px-4 font-medium">{row.feature}</td>
                  <td className="py-3 px-4 text-center bg-primary/5">
                    <CellValue value={row.memorose} />
                  </td>
                  <td className="py-3 px-4 text-center">
                    <CellValue value={row.mem0} />
                  </td>
                  <td className="py-3 px-4 text-center">
                    <CellValue value={row.zep} />
                  </td>
                  <td className="py-3 px-4 text-center">
                    <CellValue value={row.chromadb} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
