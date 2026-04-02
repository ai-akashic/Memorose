"use client";

import type { MemoryAsset } from "@/lib/types";
import { truncate } from "@/lib/utils";

function assetKindLabel(assetType: string) {
  const normalized = assetType.toLowerCase();
  if (normalized.startsWith("image")) return "image";
  if (normalized.startsWith("audio")) return "audio";
  if (normalized.startsWith("video")) return "video";
  return "asset";
}

function isLinkableSource(storageKey: string) {
  return storageKey.startsWith("http://") || storageKey.startsWith("https://");
}

function assetSourceLabel(asset: MemoryAsset) {
  const key = asset.storage_key?.trim() || "";
  if (
    key.startsWith("http://") ||
    key.startsWith("https://") ||
    key.startsWith("s3://") ||
    key.startsWith("local://")
  ) {
    return truncate(key, 72);
  }
  return asset.original_name || "inline";
}

export function MemoryAssets({
  assets,
  compact = false,
}: {
  assets: MemoryAsset[];
  compact?: boolean;
}) {
  if (!assets.length) {
    return null;
  }

  return (
    <div className="space-y-2">
      {assets.map((asset, index) => (
        <div
          key={`${asset.storage_key}-${index}`}
          className="rounded-xl border border-border/60 bg-card/60 p-3"
        >
          <div className="mb-1.5 flex items-center gap-2 text-[10px] font-medium uppercase tracking-widest text-muted-foreground">
            <span className="rounded-full border border-border px-2 py-0.5">
              {assetKindLabel(asset.asset_type)}
            </span>
          </div>
          {asset.description ? (
            <p
              className={
                compact
                  ? "line-clamp-2 text-xs leading-relaxed text-foreground/85"
                  : "text-xs leading-relaxed text-foreground/90 whitespace-pre-wrap"
              }
            >
              {asset.description}
            </p>
          ) : null}
          <div className="mt-2 text-[11px] font-mono text-muted-foreground break-all">
            {isLinkableSource(asset.storage_key) ? (
              <a
                href={asset.storage_key}
                target="_blank"
                rel="noreferrer"
                className="transition-colors hover:text-primary"
              >
                {assetSourceLabel(asset)}
              </a>
            ) : (
              <span>{assetSourceLabel(asset)}</span>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}
