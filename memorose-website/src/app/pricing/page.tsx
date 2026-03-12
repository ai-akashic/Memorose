import { Card } from "@/components/ui/Card";
import { Button } from "@/components/ui/Button";
import { Badge } from "@/components/ui/Badge";
import { Check } from "lucide-react";

const plans = [
  {
    name: "Community",
    price: "Free",
    description: "Self-hosted, open-source, forever free.",
    badge: "Open Source",
    features: [
      "Unlimited memories",
      "Hybrid search (vector + BM25)",
      "Knowledge graph",
      "Multi-tenant isolation",
      "Built-in dashboard",
      "Community support",
    ],
    cta: "Get Started",
    ctaHref: "/docs/getting-started",
    highlighted: false,
  },
  {
    name: "Pro",
    price: "$49",
    period: "/mo per node",
    description: "Priority support and advanced features for production.",
    badge: "Most Popular",
    features: [
      "Everything in Community",
      "Raft multi-node replication",
      "Priority email support",
      "Advanced analytics",
      "Custom embedding models",
      "SSO / SAML",
    ],
    cta: "Coming Soon",
    ctaHref: "#",
    highlighted: true,
  },
  {
    name: "Enterprise",
    price: "Custom",
    description: "Dedicated support, SLAs, and custom deployment.",
    features: [
      "Everything in Pro",
      "Dedicated support engineer",
      "99.99% SLA",
      "Custom integrations",
      "On-premise deployment",
      "Security audit & compliance",
    ],
    cta: "Contact Us",
    ctaHref: "#",
    highlighted: false,
  },
];

export default function PricingPage() {
  return (
    <div className="pt-24 pb-20">
      <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-14">
          <h1 className="text-4xl font-bold mb-4">Pricing</h1>
          <p className="text-muted-foreground max-w-2xl mx-auto">
            Start free with the open-source community edition. Scale with
            commercial support when you&apos;re ready.
          </p>
        </div>

        <div className="grid md:grid-cols-3 gap-6">
          {plans.map((plan) => (
            <Card
              key={plan.name}
              className={
                plan.highlighted
                  ? "border-primary/40 shadow-lg shadow-primary/10 relative"
                  : ""
              }
            >
              {plan.badge && (
                <Badge className="mb-4">{plan.badge}</Badge>
              )}
              <h2 className="text-xl font-bold mb-1">{plan.name}</h2>
              <div className="flex items-baseline gap-1 mb-2">
                <span className="text-3xl font-bold">{plan.price}</span>
                {plan.period && (
                  <span className="text-sm text-muted-foreground">
                    {plan.period}
                  </span>
                )}
              </div>
              <p className="text-sm text-muted-foreground mb-6">
                {plan.description}
              </p>

              <ul className="space-y-2.5 mb-8">
                {plan.features.map((feature) => (
                  <li
                    key={feature}
                    className="flex items-start gap-2 text-sm text-muted-foreground"
                  >
                    <Check className="w-4 h-4 text-green-400 mt-0.5 shrink-0" />
                    {feature}
                  </li>
                ))}
              </ul>

              <Button
                variant={plan.highlighted ? "primary" : "secondary"}
                href={plan.ctaHref}
                className="w-full"
              >
                {plan.cta}
              </Button>
            </Card>
          ))}
        </div>
      </div>
    </div>
  );
}
