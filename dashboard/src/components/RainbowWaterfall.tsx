"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatNumber } from "@/lib/utils";
import { BarChart3 } from "lucide-react";

interface RainbowWaterfallProps {
  stats: {
    total_events: number;
    pending_events: number;
    memory_by_level: {
      l1: number;
      l2: number;
    };
  };
}

export function RainbowWaterfall({ stats }: RainbowWaterfallProps) {
  const steps = [
    { label: "Events", count: stats.total_events, color: "#3b82f6", width: 100 }, // Blue
    { label: "Pending (L0)", count: stats.pending_events, color: "#f59e0b", width: 80 }, // Amber
    { label: "L1 Units", count: stats.memory_by_level.l1, color: "#22c55e", width: 60 }, // Green
    { label: "L2 Insights", count: stats.memory_by_level.l2, color: "#a855f7", width: 40 }, // Purple
  ];

  const svgWidth = 600;
  const stepHeight = 50;
  const stepGap = 60; // Increased gap for visible flow
  const totalHeight = steps.length * stepHeight + (steps.length - 1) * stepGap + 20;

  return (
    <Card className="overflow-hidden">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium flex items-center gap-2">
          <BarChart3 className="w-4 h-4 text-primary" />
          Memory Pipeline Flow
        </CardTitle>
      </CardHeader>
      <CardContent className="flex justify-center p-6">
        <div className="relative w-full max-w-lg">
          <svg
            viewBox={`0 0 ${svgWidth} ${totalHeight}`}
            className="w-full h-auto drop-shadow-sm"
            style={{ overflow: 'visible' }}
          >
            <defs>
              {/* Gradients for flows */}
              {steps.slice(0, -1).map((step, i) => (
                <linearGradient key={`grad-${i}`} id={`flowGradient${i}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={step.color} stopOpacity="0.4" />
                  <stop offset="100%" stopColor={steps[i + 1].color} stopOpacity="0.4" />
                </linearGradient>
              ))}

              {/* Animated Pattern for Flow Effect */}
              <pattern id="flowPattern" x="0" y="0" width="20" height="20" patternUnits="userSpaceOnUse">
                <circle cx="2" cy="2" r="1.5" fill="white" opacity="0.5" />
                <circle cx="12" cy="12" r="1.5" fill="white" opacity="0.3" />
                <animateTransform 
                  attributeName="patternTransform" 
                  type="translate" 
                  from="0 0" 
                  to="0 20" 
                  dur="1s" 
                  repeatCount="indefinite" 
                />
              </pattern>
            </defs>

            {/* Draw Flows first (background) */}
            {steps.map((step, i) => {
              if (i >= steps.length - 1) return null;
              
              const nextStep = steps[i + 1];
              const centerX = svgWidth / 2;
              
              const blockWidth = (step.width / 100) * svgWidth;
              const nextBlockWidth = (nextStep.width / 100) * svgWidth;
              
              const x = centerX - blockWidth / 2;
              const nextX = centerX - nextBlockWidth / 2;
              
              const y = i * (stepHeight + stepGap);
              const nextY = (i + 1) * (stepHeight + stepGap);
              
              const startY = y + stepHeight;
              const endY = nextY;

              const c1y = startY + stepGap * 0.5;
              const c2y = endY - stepGap * 0.5;

              // Path connecting bottom of current to top of next
              const pathD = `
                M ${x + 10} ${startY}
                C ${x + 10} ${c1y}, ${nextX + 10} ${c2y}, ${nextX + 10} ${endY}
                L ${nextX + nextBlockWidth - 10} ${endY}
                C ${nextX + nextBlockWidth - 10} ${c2y}, ${x + blockWidth - 10} ${c1y}, ${x + blockWidth - 10} ${startY}
                Z
              `;

              return (
                <g key={`flow-${i}`}>
                  {/* Gradient Background */}
                  <path d={pathD} fill={`url(#flowGradient${i})`} />
                  {/* Animated Overlay */}
                  <path d={pathD} fill="url(#flowPattern)" style={{ mixBlendMode: 'overlay' }} />
                </g>
              );
            })}

            {/* Draw Blocks (foreground) */}
            {steps.map((step, i) => {
              const centerX = svgWidth / 2;
              const blockWidth = (step.width / 100) * svgWidth;
              const x = centerX - blockWidth / 2;
              const y = i * (stepHeight + stepGap);

              return (
                <g key={step.label}>
                  {/* Block */}
                  <rect
                    x={x}
                    y={y}
                    width={blockWidth}
                    height={stepHeight}
                    rx="8"
                    fill={step.color}
                    filter="drop-shadow(0px 4px 6px rgba(0,0,0,0.1))"
                  />
                  
                  {/* Label */}
                  <text
                    x={centerX}
                    y={y + stepHeight / 2}
                    dy="0.3em"
                    textAnchor="middle"
                    fill="white"
                    className="text-sm font-bold pointer-events-none"
                    style={{ textShadow: "0px 1px 2px rgba(0,0,0,0.3)" }}
                  >
                    {step.label}
                  </text>
                  
                  {/* Count Pill */}
                  <g transform={`translate(${centerX}, ${y - 12})`}>
                    <rect x="-40" y="0" width="80" height="24" rx="12" fill="white" className="shadow-sm" />
                    <text 
                      x="0" 
                      y="16" 
                      textAnchor="middle" 
                      className="text-xs font-bold" 
                      fill="#1f2937"
                    >
                      {formatNumber(step.count)}
                    </text>
                  </g>
                </g>
              );
            })}
          </svg>
        </div>
      </CardContent>
    </Card>
  );
}
