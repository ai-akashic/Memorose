import { cn } from "@/lib/utils";

export function HakuLogo({ size = 24, className }: { size?: number; className?: string }) {
  return <MemoroseLogo size={size} className={className} />;
}

// Color definitions for each petal (3-stop gradients: base → ridge → tip)
const outerColors = [
  ["#180d30", "#5a35a0", "#3a2070"], // Amethyst
  ["#0d1835", "#2850a0", "#1a3570"], // Sapphire
  ["#0d2820", "#20856a", "#155a48"], // Emerald
  ["#2a2510", "#8a7528", "#5a5018"], // Topaz
  ["#300d18", "#a03555", "#702038"], // Ruby
];
const midColors = [
  ["#4040a0", "#8888dd", "#6868c0"], // Iris
  ["#3070a8", "#68b0e8", "#4890cc"], // Azure
  ["#30a880", "#68e0bb", "#48c0a0"], // Jade
  ["#a89030", "#e8d068", "#ccb048"], // Gold
  ["#a84068", "#e878a8", "#cc5888"], // Rose
];
const innerColors = [
  ["#a088dd", "#d8c8ff", "#c0a8f0"], // Lavender
  ["#78b8f0", "#c0e0ff", "#98d0f8"], // Sky
  ["#68e0c0", "#b0f8e8", "#88ecd8"], // Mint
  ["#f0d068", "#fff0b0", "#f8e088"], // Champagne
  ["#f088b0", "#ffd0e0", "#f8a8c8"], // Blush
];

const outerAngles = [0, 72, 144, 216, 288];
const midAngles = [36, 108, 180, 252, 324];
const innerAngles = [18, 90, 162, 234, 306];

const outerPetal = "M256 256 Q200 185 256 80 Q312 185 256 256Z";
const midPetal = "M256 256 Q210 200 256 125 Q302 200 256 256Z";
const innerPetal = "M256 256 Q224 218 256 170 Q288 218 256 256Z";

export function MemoroseLogo({ size = 24, className }: { size?: number; className?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 512 512"
      fill="none"
      width={size}
      height={size}
      className={cn("shrink-0", className)}
    >
      <defs>
        {/* Outer petal gradients — deep jewel tones */}
        {outerColors.map(([base, ridge, tip], i) => (
          <linearGradient key={`o${i}`} id={`mr-o${i}`} x1="0.5" y1="1" x2="0.5" y2="0">
            <stop offset="0%" stopColor={base} />
            <stop offset="55%" stopColor={ridge} />
            <stop offset="100%" stopColor={tip} />
          </linearGradient>
        ))}

        {/* Middle petal gradients — rich saturated */}
        {midColors.map(([base, ridge, tip], i) => (
          <linearGradient key={`m${i}`} id={`mr-m${i}`} x1="0.5" y1="1" x2="0.5" y2="0">
            <stop offset="0%" stopColor={base} />
            <stop offset="55%" stopColor={ridge} />
            <stop offset="100%" stopColor={tip} />
          </linearGradient>
        ))}

        {/* Inner petal gradients — luminous jewel pastels */}
        {innerColors.map(([base, ridge, tip], i) => (
          <linearGradient key={`i${i}`} id={`mr-i${i}`} x1="0.5" y1="1" x2="0.5" y2="0">
            <stop offset="0%" stopColor={base} />
            <stop offset="55%" stopColor={ridge} />
            <stop offset="100%" stopColor={tip} />
          </linearGradient>
        ))}

        {/* Specular highlight overlay */}
        <linearGradient id="mr-hl" x1="0.5" y1="1" x2="0.5" y2="0">
          <stop offset="0%" stopColor="white" stopOpacity="0" />
          <stop offset="50%" stopColor="white" stopOpacity="0.18" />
          <stop offset="80%" stopColor="white" stopOpacity="0.05" />
          <stop offset="100%" stopColor="white" stopOpacity="0" />
        </linearGradient>

        {/* Ambient occlusion at center overlap */}
        <radialGradient id="mr-ao" cx="0.5" cy="0.5" r="0.22">
          <stop offset="0%" stopColor="#000" stopOpacity="0.28" />
          <stop offset="80%" stopColor="#000" stopOpacity="0.06" />
          <stop offset="100%" stopColor="#000" stopOpacity="0" />
        </radialGradient>

        {/* Core radial glow */}
        <radialGradient id="mr-core" cx="0.5" cy="0.5" r="0.22">
          <stop offset="0%" stopColor="#ffffff" stopOpacity="0.98" />
          <stop offset="35%" stopColor="#f0eaff" stopOpacity="0.6" />
          <stop offset="65%" stopColor="#d0c8e8" stopOpacity="0.15" />
          <stop offset="100%" stopColor="#d0c8e8" stopOpacity="0" />
        </radialGradient>

        {/* Ambient halo behind the bloom */}
        <radialGradient id="mr-halo" cx="0.5" cy="0.5" r="0.5">
          <stop offset="0%" stopColor="#7060a0" stopOpacity="0.06" />
          <stop offset="100%" stopColor="#18181b" stopOpacity="0" />
        </radialGradient>

        {/* Layered shadow filters */}
        <filter id="mr-sh-o" x="-25%" y="-25%" width="150%" height="150%">
          <feDropShadow dx="0" dy="4" stdDeviation="16" floodColor="#08050f" floodOpacity="0.6" />
        </filter>
        <filter id="mr-sh-m" x="-20%" y="-20%" width="140%" height="140%">
          <feDropShadow dx="0" dy="2" stdDeviation="10" floodColor="#10082a" floodOpacity="0.45" />
        </filter>
        <filter id="mr-glow" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="6" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
        <filter id="mr-cglow" x="-60%" y="-60%" width="220%" height="220%">
          <feGaussianBlur stdDeviation="8" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      {/* Soft halo */}
      <circle cx="256" cy="256" r="240" fill="url(#mr-halo)" />

      {/* Outer petals (L0 — raw events) + highlight overlay */}
      <g filter="url(#mr-sh-o)">
        <g opacity="0.75">
          {outerAngles.map((a, i) => (
            <path key={`o${i}`} d={outerPetal} fill={`url(#mr-o${i})`} transform={`rotate(${a} 256 256)`} />
          ))}
        </g>
        <g opacity="0.30">
          {outerAngles.map((a, i) => (
            <path key={`oh${i}`} d={outerPetal} fill="url(#mr-hl)" transform={`rotate(${a} 256 256)`} />
          ))}
        </g>
      </g>

      {/* Middle petals (L1 — consolidated) + highlight overlay */}
      <g filter="url(#mr-sh-m)">
        <g opacity="0.85">
          {midAngles.map((a, i) => (
            <path key={`m${i}`} d={midPetal} fill={`url(#mr-m${i})`} transform={`rotate(${a} 256 256)`} />
          ))}
        </g>
        <g opacity="0.22">
          {midAngles.map((a, i) => (
            <path key={`mh${i}`} d={midPetal} fill="url(#mr-hl)" transform={`rotate(${a} 256 256)`} />
          ))}
        </g>
      </g>

      {/* Ambient occlusion at center */}
      <circle cx="256" cy="256" r="110" fill="url(#mr-ao)" />

      {/* Inner petals (L2 — insights) */}
      <g filter="url(#mr-glow)">
        <g opacity="0.93">
          {innerAngles.map((a, i) => (
            <path key={`i${i}`} d={innerPetal} fill={`url(#mr-i${i})`} transform={`rotate(${a} 256 256)`} />
          ))}
        </g>
        <g opacity="0.15">
          {innerAngles.map((a, i) => (
            <path key={`ih${i}`} d={innerPetal} fill="url(#mr-hl)" transform={`rotate(${a} 256 256)`} />
          ))}
        </g>
      </g>

      {/* Knowledge-graph veins */}
      <g opacity="0.06" stroke="#d4d4d8" strokeWidth="1.2" fill="none">
        <polygon points="333,149 382,295 256,387 130,295 179,149" />
      </g>

      {/* Core glow */}
      <circle cx="256" cy="256" r="55" fill="url(#mr-core)" filter="url(#mr-cglow)" />

      {/* Center point — the seed of memory */}
      <circle cx="256" cy="256" r="5" fill="#ffffff" opacity="0.95" filter="url(#mr-cglow)" />
    </svg>
  );
}
