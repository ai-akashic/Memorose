import { cn } from "@/lib/utils";

export function HakuLogo({ size = 24, className }: { size?: number; className?: string }) {
  return <MemoroseLogo size={size} className={className} />;
}

// Color definitions for each petal (3-stop gradients: base → ridge → tip)
// Adjusted for a more low-profile, desaturated "hardcore" look
const outerColors = [
  ["#11091d", "#362060", "#231343"], // Muted Amethyst
  ["#080e20", "#183060", "#0f2043"], // Muted Sapphire
  ["#081813", "#135040", "#0c362b"], // Muted Emerald
  ["#19160a", "#534618", "#362d0e"], // Muted Topaz
  ["#1d080e", "#602033", "#431322"], // Muted Ruby
];
const midColors = [
  ["#262660", "#525285", "#3e3e73"], // Muted Iris
  ["#1d4365", "#3e6a8b", "#2b567a"], // Muted Azure
  ["#1d654d", "#3e8670", "#2b7360"], // Muted Jade
  ["#65561d", "#8b7d3e", "#7a6a2b"], // Muted Gold
  ["#65263e", "#8b4865", "#7a3551"], // Muted Rose
];
const innerColors = [
  ["#605285", "#827899", "#736590"], // Pale Lavender
  ["#486e90", "#738699", "#5b7c95"], // Pale Sky
  ["#3e8673", "#6a958b", "#528173"], // Pale Mint
  ["#907d3e", "#998f6a", "#958652"], // Pale Champagne
  ["#90526a", "#997d86", "#95657a"], // Pale Blush
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
          <stop offset="50%" stopColor="white" stopOpacity="0.08" />
          <stop offset="80%" stopColor="white" stopOpacity="0.02" />
          <stop offset="100%" stopColor="white" stopOpacity="0" />
        </linearGradient>

        {/* Ambient occlusion at center overlap */}
        <radialGradient id="mr-ao" cx="0.5" cy="0.5" r="0.22">
          <stop offset="0%" stopColor="#000" stopOpacity="0.35" />
          <stop offset="80%" stopColor="#000" stopOpacity="0.10" />
          <stop offset="100%" stopColor="#000" stopOpacity="0" />
        </radialGradient>

        {/* Core radial glow */}
        <radialGradient id="mr-core" cx="0.5" cy="0.5" r="0.22">
          <stop offset="0%" stopColor="#ffffff" stopOpacity="0.80" />
          <stop offset="35%" stopColor="#d4d4d8" stopOpacity="0.4" />
          <stop offset="65%" stopColor="#a1a1aa" stopOpacity="0.08" />
          <stop offset="100%" stopColor="#a1a1aa" stopOpacity="0" />
        </radialGradient>

        {/* Ambient halo behind the bloom */}
        <radialGradient id="mr-halo" cx="0.5" cy="0.5" r="0.5">
          <stop offset="0%" stopColor="#52525b" stopOpacity="0.03" />
          <stop offset="100%" stopColor="#18181b" stopOpacity="0" />
        </radialGradient>

        {/* Layered shadow filters */}
        <filter id="mr-sh-o" x="-25%" y="-25%" width="150%" height="150%">
          <feDropShadow dx="0" dy="4" stdDeviation="12" floodColor="#000000" floodOpacity="0.7" />
        </filter>
        <filter id="mr-sh-m" x="-20%" y="-20%" width="140%" height="140%">
          <feDropShadow dx="0" dy="2" stdDeviation="8" floodColor="#000000" floodOpacity="0.5" />
        </filter>
        <filter id="mr-glow" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="4" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
        <filter id="mr-cglow" x="-60%" y="-60%" width="220%" height="220%">
          <feGaussianBlur stdDeviation="6" result="blur" />
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
