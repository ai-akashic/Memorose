import { Card } from "@/components/ui/Card";

const posts = [
  {
    title: "Introducing Memorose: Long-Term Memory for AI Agents",
    excerpt:
      "We're excited to announce the public beta of Memorose — an open-source, self-hosted memory layer purpose-built for AI agents.",
    date: "March 2026",
    tag: "Announcement",
  },
  {
    title: "Why AI Agents Need a Dedicated Memory Layer",
    excerpt:
      "Context windows are getting bigger, but they're not the answer. Here's why purpose-built memory infrastructure matters.",
    date: "Coming Soon",
    tag: "Engineering",
  },
  {
    title: "Hybrid Search: Combining Vectors and BM25 for Better Recall",
    excerpt:
      "How Memorose fuses vector similarity and full-text search for more accurate and complete memory retrieval.",
    date: "Coming Soon",
    tag: "Deep Dive",
  },
];

export default function BlogPage() {
  return (
    <div className="pt-24 pb-20">
      <div className="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="mb-14">
          <h1 className="text-4xl font-bold mb-4">Blog</h1>
          <p className="text-muted-foreground">
            Engineering deep dives, announcements, and updates from the Memorose
            team.
          </p>
        </div>

        <div className="space-y-6">
          {posts.map((post) => (
            <Card key={post.title} hover className="cursor-pointer">
              <div className="flex items-center gap-3 mb-2">
                <span className="text-xs px-2 py-0.5 bg-primary/10 text-primary rounded-full">
                  {post.tag}
                </span>
                <span className="text-xs text-muted-foreground">
                  {post.date}
                </span>
              </div>
              <h2 className="text-lg font-semibold mb-2">{post.title}</h2>
              <p className="text-sm text-muted-foreground">{post.excerpt}</p>
            </Card>
          ))}
        </div>
      </div>
    </div>
  );
}
