"use client";

import { formatNumber } from "@/lib/utils";

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
    { label: "L0 Stream", count: stats.total_events, color: "hsl(220 70% 50%)", width: 85 }, // Reduced block width to make room for text
    { label: "Queue", count: stats.pending_events, color: "hsl(38 92% 50%)", width: 70 },
    { label: "L1 Memory", count: stats.memory_by_level.l1, color: "hsl(142 76% 36%)", width: 55 },
    { label: "L2 Insights", count: stats.memory_by_level.l2, color: "hsl(280 65% 60%)", width: 40 },
  ];

  // We increase svgWidth to 550 and keep blocks aligned towards the left/center 
  // to ensure numbers on the right have plenty of space.
  const svgWidth = 550;
  const centerX = 200; // Offset center to the left
  const stepHeight = 44;
  const stepGap = 48; 
  const totalHeight = steps.length * stepHeight + (steps.length - 1) * stepGap;

  return (
    <div className="w-full h-full flex items-center justify-center p-2">
      <svg
        viewBox={`0 0 ${svgWidth} ${totalHeight}`}
        className="max-h-full w-full drop-shadow-2xl"
        preserveAspectRatio="xMidYMid meet"
        style={{ overflow: 'visible' }}
      >
        <defs>
          {/* Gradients for flows */}
          {steps.slice(0, -1).map((step, i) => (
            <linearGradient key={`grad-${i}`} id={`flowGradient${i}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={step.color} stopOpacity="0.3" />
              <stop offset="100%" stopColor={steps[i + 1].color} stopOpacity="0.3" />
            </linearGradient>
          ))}

          {/* Animated Pattern for Flow Effect */}
          <pattern id="flowPattern" x="0" y="0" width="20" height="20" patternUnits="userSpaceOnUse">
            <circle cx="2" cy="2" r="1" fill="white" opacity="0.2" />
            <animateTransform 
              attributeName="patternTransform" 
              type="translate" 
              from="0 0" 
              to="0 20" 
              dur="2s" 
              repeatCount="indefinite" 
            />
          </pattern>
        </defs>

        {/* Draw Flows */}
        {steps.map((step, i) => {
          if (i >= steps.length - 1) return null;
          
          const nextStep = steps[i + 1];
          const blockWidth = (step.width / 100) * 400; // Use 400 as reference width
          const nextBlockWidth = (nextStep.width / 100) * 400;
          
          const x = centerX - blockWidth / 2;
          const nextX = centerX - nextBlockWidth / 2;
          
          const y = i * (stepHeight + stepGap);
          const nextY = (i + 1) * (stepHeight + stepGap);
          
          const startY = y + stepHeight;
          const endY = nextY;

          const c1y = startY + stepGap * 0.4;
          const c2y = endY - stepGap * 0.4;

          const pathD = `
            M ${x + 5} ${startY}
            C ${x + 5} ${c1y}, ${nextX + 5} ${c2y}, ${nextX + 5} ${endY}
            L ${nextX + nextBlockWidth - 5} ${endY}
            C ${nextX + nextBlockWidth - 5} ${c2y}, ${x + blockWidth - 5} ${c1y}, ${x + blockWidth - 5} ${startY}
            Z
          `;

          return (
            <g key={`flow-${i}`}>
              <path d={pathD} fill={`url(#flowGradient${i})`} />
              <path d={pathD} fill="url(#flowPattern)" style={{ mixBlendMode: 'overlay' }} />
            </g>
          );
        })}

        {/* Draw Blocks */}
        {steps.map((step, i) => {
          const blockWidth = (step.width / 100) * 400;
          const x = centerX - blockWidth / 2;
          const y = i * (stepHeight + stepGap);

          return (
            <g key={step.label}>
              {/* Glow */}
              <rect
                x={x}
                y={y}
                width={blockWidth}
                height={stepHeight}
                rx="8"
                fill={step.color}
                opacity="0.25"
                style={{ filter: 'blur(10px)' }}
              />
              {/* Block */}
              <rect
                x={x}
                y={y}
                width={blockWidth}
                height={stepHeight}
                rx="8"
                fill={step.color}
                className="stroke-white/20"
                strokeWidth="1.5"
              />
              
              {/* Label - Increased to 15px */}
              <text
                x={centerX}
                y={y + stepHeight / 2}
                dy="0.3em"
                textAnchor="middle"
                fill="white"
                className="font-sans font-extrabold pointer-events-none uppercase tracking-widest"
                style={{ fontSize: '15px', textShadow: "0px 2px 4px rgba(0,0,0,0.6)" }}
              >
                {step.label}
              </text>
              
              {/* Count - Increased to 16px and moved to avoid clipping */}
              <text 
                x={centerX + blockWidth/2 + 18} 
                y={y + stepHeight/2} 
                dy="0.3em"
                textAnchor="start" 
                className="font-mono text-[16px] font-black fill-white"
                style={{ filter: "drop-shadow(0 0 10px rgba(255,255,255,0.4))" }}
              >
                {formatNumber(step.count)}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
