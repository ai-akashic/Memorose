import { Hero } from "@/components/landing/Hero";
import { ProblemSolution } from "@/components/landing/ProblemSolution";
import { Architecture } from "@/components/landing/Architecture";
import { CodeExample } from "@/components/landing/CodeExample";
import { FeatureGrid } from "@/components/landing/FeatureGrid";
import { ComparisonTable } from "@/components/landing/ComparisonTable";
import { Screenshots } from "@/components/landing/Screenshots";
import { Benchmarks } from "@/components/landing/Benchmarks";
import { Deployment } from "@/components/landing/Deployment";
import { GitHubStats } from "@/components/landing/GitHubStats";

export default function Home() {
  return (
    <>
      <Hero />
      <ProblemSolution />
      <Architecture />
      <CodeExample />
      <FeatureGrid />
      <ComparisonTable />
      <Screenshots />
      <Benchmarks />
      <Deployment />
      <GitHubStats />
    </>
  );
}
