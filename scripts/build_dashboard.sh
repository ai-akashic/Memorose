#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "==> Building Memorose Dashboard..."

# 1. Build Next.js static export
cd "$ROOT_DIR/dashboard"
if ! command -v pnpm &> /dev/null; then
    echo "Error: pnpm is required. Install with: npm install -g pnpm"
    exit 1
fi

echo "    Installing dependencies..."
pnpm install --frozen-lockfile 2>/dev/null || pnpm install

echo "    Building static export..."
pnpm build

# 2. Copy to server static directory
STATIC_DIR="$ROOT_DIR/crates/memorose-server/static/dashboard"
echo "    Copying to $STATIC_DIR..."
rm -rf "$STATIC_DIR"
mkdir -p "$STATIC_DIR"
cp -r out/* "$STATIC_DIR/"

# 3. Ensure root index.html exists (redirects to login)
if [ ! -f "$STATIC_DIR/index.html" ]; then
    echo '<!DOCTYPE html><html><head><meta http-equiv="refresh" content="0;url=/dashboard/login/"></head></html>' > "$STATIC_DIR/index.html"
    echo "    Created root redirect index.html"
fi

echo "==> Dashboard build complete!"
echo "    Static files: $STATIC_DIR"
echo "    Start the server and visit: http://localhost:3000/dashboard"
