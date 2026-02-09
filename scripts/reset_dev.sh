#!/bin/bash

# Reset development environment data
# WARNING: This will delete all local databases and logs

set -e

# Change to project root
cd "$(dirname "$0")/.."

echo "🧹 Cleaning up development data..."

# Stop servers if running
./scripts/start_cluster.sh stop || true

# Remove data directories
if [ -d "data" ]; then
    echo "🗑️  Removing data/ directory..."
    rm -rf data/node-*
    rm -rf data/lancedb
    rm -rf data/rocksdb
    rm -rf data/tantivy
fi

# Remove logs
if [ -d "logs" ]; then
    echo "🗑️  Removing logs/ directory..."
    rm -rf logs/*
fi

echo "✨ Environment reset complete."
