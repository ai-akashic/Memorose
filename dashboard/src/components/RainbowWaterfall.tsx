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
    { label: "L0 Stream", count: stats.total_events, color: "hsl(220 70% 50%)", width: 85 },
    { label: "Queue", count: stats.pending_events, color: "hsl(38 92% 50%)", width: 70 },
    { label: "L1 Memory", count: stats.memory_by_level.l1, color: "hsl(142 76% 36%)", width: 55 },
    { label: "L2 Insights", count: stats.memory_by_level.l2, color: "hsl(280 65% 60%)", width: 40 },
  ];

  const svgWidth = 700;
  const svgHeight = 320; // Trimmed vertical empty space
  const centerY = 160;   // Perfectly centered in 320
  const stepWidth = 130; // Wider blocks
  const stepGap = 40;    // Tighter gaps
  const totalWidth = steps.length * stepWidth + (steps.length - 1) * stepGap;
  const startOffsetX = (svgWidth - totalWidth) / 2;

  return (
    <div className="w-full h-full flex items-center justify-center">
      <svg
        viewBox={`0 0 ${svgWidth} ${svgHeight}`}
        className="w-full h-full drop-shadow-2xl"
        preserveAspectRatio="xMidYMid meet"
        style={{ overflow: 'visible' }}
      >
        <defs>
          <linearGradient id="flowPatternGrad" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="white" stopOpacity="0.1" />
            <stop offset="50%" stopColor="white" stopOpacity="0.3" />
            <stop offset="100%" stopColor="white" stopOpacity="0.1" />
          </linearGradient>

          {/* Gradients for flows */}
          {steps.slice(0, -1).map((step, i) => (
            <linearGradient key={`grad-${i}`} id={`flowGradient${i}`} x1="0" y1="0" x2="1" y2="0">
              <stop offset="0%" stopColor={step.color} stopOpacity="0.3" />
              <stop offset="100%" stopColor={steps[i + 1].color} stopOpacity="0.3" />
            </linearGradient>
          ))}

          {/* Animated Pattern for Flow Effect */}
          <pattern id="flowPattern" x="0" y="0" width="40" height="40" patternUnits="userSpaceOnUse">
             <rect width="2" height="40" fill="url(#flowPatternGrad)" />
            <animateTransform 
              attributeName="patternTransform" 
              type="translate" 
              from="0 0" 
              to="40 0" 
              dur="3s" 
              repeatCount="indefinite" 
            />
          </pattern>
        </defs>

        {/* Draw Flows */}
        {steps.map((step, i) => {
          if (i >= steps.length - 1) return null;
          
          const nextStep = steps[i + 1];
          const blockHeight = (step.width / 100) * 300; // Scaled up height
          const nextBlockHeight = (nextStep.width / 100) * 300;
          
          const x = startOffsetX + i * (stepWidth + stepGap);
          const nextX = startOffsetX + (i + 1) * (stepWidth + stepGap);
          
          const y = centerY - blockHeight / 2;
          const nextY = centerY - nextBlockHeight / 2;
          
          const startX = x + stepWidth;
          const endX = nextX;

          const yTop1 = y;
          const yBottom1 = y + blockHeight;
          const yTop2 = nextY;
          const yBottom2 = nextY + nextBlockHeight;

          const c1x = startX + stepGap * 0.5;
          const c2x = endX - stepGap * 0.5;

          const pathD = `
            M ${startX} ${yTop1 + 4}
            C ${c1x} ${yTop1 + 4}, ${c2x} ${yTop2 + 4}, ${endX} ${yTop2 + 4}
            L ${endX} ${yBottom2 - 4}
            C ${c2x} ${yBottom2 - 4}, ${c1x} ${yBottom1 - 4}, ${startX} ${yBottom1 - 4}
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
          const blockHeight = (step.width / 100) * 300;
          const x = startOffsetX + i * (stepWidth + stepGap);
          const y = centerY - blockHeight / 2;

          return (
            <g key={step.label}>
              {/* Glow */}
              <rect
                x={x}
                y={y}
                width={stepWidth}
                height={blockHeight}
                rx="6"
                fill={step.color}
                opacity="0.2"
                style={{ filter: 'blur(8px)' }}
              />
              {/* Block */}
              <rect
                x={x}
                y={y}
                width={stepWidth}
                height={blockHeight}
                rx="6"
                fill={step.color}
                className="stroke-white/10"
                strokeWidth="1"
              />
              
              {/* Label - Rotated or split to fit narrow block */}
              <g transform={`translate(${x + stepWidth / 2}, ${centerY})`}>
                <text
                  y="-12"
                  textAnchor="middle"
                  fill="white"
                  className="font-sans font-black pointer-events-none uppercase tracking-tighter"
                  style={{ fontSize: '13px', textShadow: "0px 1px 3px rgba(0,0,0,0.8)" }}
                >
                  {step.label.split(' ')[0]}
                </text>
                <text
                  y="6"
                  textAnchor="middle"
                  fill="white"
                  className="font-sans font-black pointer-events-none uppercase tracking-tighter"
                  style={{ fontSize: '13px', textShadow: "0px 1px 3px rgba(0,0,0,0.8)" }}
                >
                  {step.label.split(' ')[1] || ''}
                </text>
                
                {/* Count */}
                <text 
                  y="30" 
                  textAnchor="middle" 
                  className="font-mono text-[16px] font-bold fill-white/90"
                >
                  {formatNumber(step.count)}
                </text>
              </g>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
