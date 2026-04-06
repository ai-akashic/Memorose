"use client";

import { useId, useMemo } from "react";
import { useReducedMotion } from "framer-motion";
import type { Stats } from "@/lib/types";
import { formatNumber } from "@/lib/utils";

interface RainbowWaterfallProps {
  stats: Pick<
    Stats,
    | "total_events"
    | "pending_events"
    | "total_memory_units"
    | "total_edges"
    | "memory_by_scope"
    | "memory_by_domain"
    | "memory_by_level"
    | "memory_by_level_and_scope"
    | "rac_metrics"
  >;
}

type FlowNode = {
  key: string;
  label: string;
  value: number;
  x: number;
  y: number;
  width: number;
  height: number;
  fill: string;
  stroke: string;
};

type FlowLink = {
  from: string;
  to: string;
  color: string;
  opacity?: number;
};

const VIEWBOX_WIDTH = 1180;
const VIEWBOX_HEIGHT = 220;
const NODE_WIDTH = 190;
const NODE_HEIGHT = 36;

function pathBetween(source: FlowNode, target: FlowNode) {
  const startX = source.x + source.width;
  const startY = source.y + source.height / 2;
  const endX = target.x;
  const endY = target.y + target.height / 2;
  const deltaX = Math.max(44, (endX - startX) * 0.45);

  return `M ${startX} ${startY} C ${startX + deltaX} ${startY}, ${endX - deltaX} ${endY}, ${endX} ${endY}`;
}

function valueRatio(value: number, peak: number) {
  if (peak <= 0) return 0;
  return Math.max(0.12, Math.min(1, value / peak));
}

export function RainbowWaterfall({ stats }: RainbowWaterfallProps) {
  const reduceMotion = useReducedMotion();
  const id = useId().replace(/:/g, "");

  const forgetCount =
    (stats.rac_metrics?.tombstone_total ?? 0) +
    (stats.rac_metrics?.correction_action_obsolete_total ?? 0);
  const denoiseCount =
    (stats.rac_metrics?.correction_action_ignore_total ?? 0) +
    (stats.rac_metrics?.correction_action_contradicts_total ?? 0);
  const alignCount = Math.max(
    stats.total_edges,
    stats.rac_metrics?.fact_extraction_success_total ?? 0,
  );
  const communityCount = Math.max(
    stats.memory_by_scope.shared,
    stats.memory_by_level_and_scope.shared.l1 + stats.memory_by_level_and_scope.shared.l2,
  );

  const nodes = useMemo<FlowNode[]>(
    () => [
      { key: "ingest", label: "摄取事件", value: stats.total_events, x: 52, y: 56, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(57,120,255,0.18)", stroke: "rgb(87,144,255)" },
      { key: "queue", label: "待处理", value: stats.pending_events, x: 52, y: 126, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(255,174,54,0.18)", stroke: "rgb(255,190,84)" },

      { key: "user", label: "用户记忆", value: stats.memory_by_domain.user, x: 318, y: 24, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(76,201,240,0.16)", stroke: "rgb(100,220,255)" },
      { key: "agent", label: "Agent 记忆", value: stats.memory_by_domain.agent, x: 318, y: 92, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(71,190,142,0.16)", stroke: "rgb(87,222,160)" },
      { key: "org", label: "组织记忆", value: stats.memory_by_domain.organization, x: 318, y: 160, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(173,120,255,0.16)", stroke: "rgb(193,147,255)" },

      { key: "align", label: "对齐压缩", value: alignCount, x: 584, y: 12, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(92,125,255,0.16)", stroke: "rgb(130,157,255)" },
      { key: "community", label: "社区发现", value: communityCount, x: 584, y: 64, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(53,198,179,0.16)", stroke: "rgb(78,227,208)" },
      { key: "denoise", label: "降噪", value: denoiseCount, x: 584, y: 116, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(255,145,84,0.16)", stroke: "rgb(255,176,119)" },
      { key: "forget", label: "记忆遗忘", value: forgetCount, x: 584, y: 168, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(255,90,128,0.16)", stroke: "rgb(255,126,159)" },

      { key: "local", label: "本地记忆", value: stats.memory_by_scope.local, x: 850, y: 40, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(57,120,255,0.16)", stroke: "rgb(105,156,255)" },
      { key: "shared", label: "共享记忆", value: stats.memory_by_scope.shared, x: 850, y: 92, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(170,120,255,0.16)", stroke: "rgb(200,157,255)" },
      { key: "insight", label: "洞察 L2", value: stats.memory_by_level.l2, x: 850, y: 144, width: NODE_WIDTH, height: NODE_HEIGHT, fill: "rgba(255,198,72,0.16)", stroke: "rgb(255,216,101)" },
    ],
    [alignCount, communityCount, denoiseCount, forgetCount, stats],
  );

  const links = useMemo<FlowLink[]>(
    () => [
      { from: "ingest", to: "user", color: "rgba(95,167,255,0.45)" },
      { from: "ingest", to: "agent", color: "rgba(88,223,165,0.42)" },
      { from: "ingest", to: "org", color: "rgba(181,142,255,0.4)" },
      { from: "queue", to: "align", color: "rgba(126,156,255,0.42)" },
      { from: "queue", to: "denoise", color: "rgba(255,176,119,0.42)" },
      { from: "user", to: "align", color: "rgba(118,216,255,0.38)" },
      { from: "agent", to: "community", color: "rgba(87,222,160,0.38)" },
      { from: "org", to: "community", color: "rgba(193,147,255,0.38)" },
      { from: "org", to: "forget", color: "rgba(255,126,159,0.24)" },
      { from: "align", to: "local", color: "rgba(105,156,255,0.45)" },
      { from: "align", to: "shared", color: "rgba(170,132,255,0.34)" },
      { from: "community", to: "shared", color: "rgba(78,227,208,0.42)" },
      { from: "community", to: "insight", color: "rgba(255,216,101,0.36)" },
      { from: "denoise", to: "insight", color: "rgba(255,176,119,0.4)" },
      { from: "forget", to: "insight", color: "rgba(255,126,159,0.2)" },
    ],
    [],
  );

  const peak = Math.max(
    stats.total_events,
    stats.pending_events,
    stats.total_memory_units,
    stats.total_edges,
    stats.memory_by_scope.local,
    stats.memory_by_scope.shared,
    stats.memory_by_domain.agent,
    stats.memory_by_domain.user,
    stats.memory_by_domain.organization,
    stats.memory_by_level.l2,
    alignCount,
    communityCount,
    denoiseCount,
    forgetCount,
    1,
  );

  const nodeMap = new Map(nodes.map((node) => [node.key, node]));
  const layerBadges = [
    { x: 52, label: "L0 摄取层" },
    { x: 318, label: "L1 域记忆层" },
    { x: 584, label: "L2 演化层" },
    { x: 850, label: "L3 洞察层" },
  ];

  return (
    <div className="flex h-full w-full items-center justify-center">
      <svg
        viewBox={`0 0 ${VIEWBOX_WIDTH} ${VIEWBOX_HEIGHT}`}
        className="h-full w-full"
        preserveAspectRatio="xMidYMid meet"
      >
        <defs>
          <pattern id={`${id}-grid`} width="28" height="28" patternUnits="userSpaceOnUse">
            <path d="M 28 0 L 0 0 0 28" fill="none" stroke="rgba(255,255,255,0.04)" strokeWidth="1" />
          </pattern>
          <filter id={`${id}-glow`} x="-60%" y="-60%" width="220%" height="220%">
            <feGaussianBlur stdDeviation="5" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <linearGradient id={`${id}-bg`} x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor="rgba(255,255,255,0.05)" />
            <stop offset="100%" stopColor="rgba(255,255,255,0.015)" />
          </linearGradient>
          {nodes.map((node) => (
            <linearGradient key={node.key} id={`${id}-${node.key}-fill`} x1="0" y1="0" x2="1" y2="1">
              <stop offset="0%" stopColor={node.fill} />
              <stop offset="100%" stopColor="rgba(255,255,255,0.02)" />
            </linearGradient>
          ))}
        </defs>

        <rect x="0" y="0" width={VIEWBOX_WIDTH} height={VIEWBOX_HEIGHT} rx="28" fill={`url(#${id}-bg)`} />
        <rect x="0" y="0" width={VIEWBOX_WIDTH} height={VIEWBOX_HEIGHT} rx="28" fill={`url(#${id}-grid)`} opacity="0.65" />

        {layerBadges.map((badge) => (
          <g key={badge.label} transform={`translate(${badge.x}, 8)`}>
            <rect width="112" height="18" rx="9" fill="rgba(255,255,255,0.05)" stroke="rgba(255,255,255,0.08)" />
            <text x="56" y="12.5" textAnchor="middle" fill="rgba(255,255,255,0.68)" style={{ fontSize: "10px", fontWeight: 700, letterSpacing: "0.12em" }}>
              {badge.label}
            </text>
          </g>
        ))}

        {links.map((link, index) => {
          const source = nodeMap.get(link.from);
          const target = nodeMap.get(link.to);
          if (!source || !target) return null;

          const path = pathBetween(source, target);
          return (
            <g key={`${link.from}-${link.to}`}>
              <path d={path} fill="none" stroke={link.color} strokeOpacity={link.opacity ?? 1} strokeWidth="2.4" />
              <path d={path} fill="none" stroke="rgba(255,255,255,0.08)" strokeWidth="0.8" strokeDasharray="5 8" />
              {!reduceMotion ? (
                <g filter={`url(#${id}-glow)`}>
                  <circle r="2.4" fill={link.color.replace(/0\.\d+\)/, "0.95)")}>
                    <animateMotion
                      dur={`${4.2 + (index % 5) * 0.55}s`}
                      repeatCount="indefinite"
                      rotate="auto"
                      path={path}
                    />
                  </circle>
                </g>
              ) : null}
            </g>
          );
        })}

        {nodes.map((node, index) => {
          const ratio = valueRatio(node.value, peak);
          return (
            <g key={node.key} transform={`translate(${node.x},${node.y})`}>
              <rect
                x="-3"
                y="-3"
                width={node.width + 6}
                height={node.height + 6}
                rx="18"
                fill={node.stroke}
                opacity={reduceMotion ? 0.08 : 0.12}
                filter={`url(#${id}-glow)`}
              />
              <rect
                width={node.width}
                height={node.height}
                rx="16"
                fill={`url(#${id}-${node.key}-fill)`}
                stroke={node.stroke}
                strokeOpacity="0.6"
              />
              <circle cx="14" cy="14" r="4" fill={node.stroke} />
              {!reduceMotion ? (
                <circle cx="14" cy="14" r="7" fill={node.stroke} opacity="0.24">
                  <animate attributeName="r" values="6;10;6" dur={`${3.2 + (index % 4) * 0.35}s`} repeatCount="indefinite" />
                  <animate attributeName="opacity" values="0.12;0.3;0.12" dur={`${3.2 + (index % 4) * 0.35}s`} repeatCount="indefinite" />
                </circle>
              ) : null}
              <text x="26" y="16.5" fill="rgba(255,255,255,0.88)" style={{ fontSize: "11px", fontWeight: 700 }}>
                {node.label}
              </text>
              <text x={node.width - 12} y="17" textAnchor="end" fill="rgba(255,255,255,0.96)" style={{ fontSize: "12px", fontWeight: 800 }}>
                {formatNumber(node.value)}
              </text>
              <rect x="14" y={node.height - 9} width={node.width - 28} height="3.5" rx="1.75" fill="rgba(255,255,255,0.08)" />
              <rect x="14" y={node.height - 9} width={(node.width - 28) * ratio} height="3.5" rx="1.75" fill={node.stroke} />
            </g>
          );
        })}
      </svg>
    </div>
  );
}
