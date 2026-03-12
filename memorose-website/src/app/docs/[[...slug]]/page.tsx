import { readFile, readdir } from "fs/promises";
import path from "path";
import { MDXRemote } from "next-mdx-remote/rsc";
import Link from "next/link";
import { ChevronRight } from "lucide-react";

const DOCS_DIR = path.join(process.cwd(), "content", "docs");

const docNav = [
  { slug: "getting-started", title: "Getting Started" },
  { slug: "architecture", title: "Architecture" },
  { slug: "api-reference", title: "API Reference" },
  { slug: "python-sdk", title: "Python SDK" },
  { slug: "deployment", title: "Deployment" },
];

async function getDocContent(slug: string) {
  try {
    const filePath = path.join(DOCS_DIR, `${slug}.mdx`);
    const source = await readFile(filePath, "utf-8");
    return source;
  } catch {
    return null;
  }
}

export async function generateStaticParams() {
  try {
    const files = await readdir(DOCS_DIR);
    const slugs = files
      .filter((f) => f.endsWith(".mdx"))
      .map((f) => ({ slug: [f.replace(".mdx", "")] }));
    return [{ slug: [] }, ...slugs];
  } catch {
    return [{ slug: [] }];
  }
}

export default async function DocsPage({
  params,
}: {
  params: Promise<{ slug?: string[] }>;
}) {
  const { slug } = await params;
  const currentSlug = slug?.[0] || "getting-started";
  const content = await getDocContent(currentSlug);

  return (
    <div className="pt-16 min-h-screen">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
        <div className="flex gap-10">
          {/* Sidebar */}
          <aside className="hidden lg:block w-56 shrink-0">
            <nav className="sticky top-28 space-y-1">
              <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
                Documentation
              </h3>
              {docNav.map((item) => (
                <Link
                  key={item.slug}
                  href={`/docs/${item.slug}`}
                  className={`flex items-center gap-2 px-3 py-2 text-sm rounded-md transition-colors ${
                    currentSlug === item.slug
                      ? "bg-primary/10 text-primary font-medium"
                      : "text-muted-foreground hover:text-foreground hover:bg-secondary/50"
                  }`}
                >
                  <ChevronRight
                    className={`w-3.5 h-3.5 ${
                      currentSlug === item.slug
                        ? "text-primary"
                        : "text-muted-foreground/40"
                    }`}
                  />
                  {item.title}
                </Link>
              ))}
            </nav>
          </aside>

          {/* Content */}
          <article className="flex-1 min-w-0 prose">
            {content ? (
              <MDXRemote source={content} />
            ) : (
              <div>
                <h1>Page Not Found</h1>
                <p>
                  The documentation page &quot;{currentSlug}&quot; does not exist
                  yet.
                </p>
              </div>
            )}
          </article>
        </div>
      </div>
    </div>
  );
}
