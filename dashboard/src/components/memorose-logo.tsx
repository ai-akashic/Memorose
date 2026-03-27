import { cn } from "@/lib/utils";

const outerAngles = [0, 72, 144, 216, 288];
const innerAngles = [36, 108, 180, 252, 324];

const outerPetal =
  "M256 256C224 218 202 180 194 142C189 117 198 94 220 82C236 73 250 75 256 92C262 75 276 73 292 82C314 94 323 117 318 142C310 180 288 218 256 256Z";
const innerPetal =
  "M256 256C236 232 224 209 221 187C219 171 225 157 239 149C247 144 253 146 256 154C259 146 265 144 273 149C287 157 293 171 291 187C288 209 276 232 256 256Z";

export function MemoroseLogo({ size = 24, className }: { size?: number; className?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 512 512"
      width={size}
      height={size}
      className={cn("shrink-0", className)}
      role="img"
      aria-labelledby="memoroseLogoTitle"
    >
      <title id="memoroseLogoTitle">Memorose logo</title>
      <defs>
        <linearGradient id="outerFill" x1="256" y1="86" x2="256" y2="256" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#E6B8AA" />
          <stop offset="100%" stopColor="#C97865" />
        </linearGradient>
        <linearGradient id="innerFill" x1="256" y1="146" x2="256" y2="256" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#F4DDD2" />
          <stop offset="100%" stopColor="#D9A08D" />
        </linearGradient>
      </defs>

      <g opacity="0.98">
        <g fill="url(#outerFill)">
          {outerAngles.map((angle) => (
            <path key={`o-${angle}`} d={outerPetal} transform={`rotate(${angle} 256 256)`} />
          ))}
        </g>
        <g fill="url(#innerFill)">
          {innerAngles.map((angle) => (
            <path key={`i-${angle}`} d={innerPetal} transform={`rotate(${angle} 256 256)`} />
          ))}
        </g>
        <circle cx="256" cy="256" r="28" fill="#4E5B54" />
      </g>
    </svg>
  );
}
