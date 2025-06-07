#!/usr/bin/env bash
set -euo pipefail

# ─── CONFIG ──────────────────────────────────────────────────────────────────
# Ensure you have:
#   export GH_TOKEN=your_personal_access_token_with_write:packages
: "${GH_TOKEN:?Environment variable GH_TOKEN must be set}"

# The URL of your GitHub Packages NuGet feed
FEED_URL="https://nuget.pkg.github.com/hoangsonww/index.json"

# Path to the .NET project
PROJECT_DIR="src/DataPipelineApi"

# ─── 1) Pack inside Docker ───────────────────────────────────────────────────
echo "📦 Packing the .NET project..."
docker run --rm \
  -v "$PWD/$PROJECT_DIR":/app \
  -w /app \
  mcr.microsoft.com/dotnet/sdk:6.0 \
  dotnet pack -c Release --no-build

# ─── 2) Push the nupkg to GitHub Packages by URL ─────────────────────────────
echo "🚀 Pushing package to GitHub Packages..."
docker run --rm \
  -e GH_TOKEN="$GH_TOKEN" \
  -v "$PWD/$PROJECT_DIR":/app \
  -w /app \
  mcr.microsoft.com/dotnet/sdk:6.0 \
  bash -lc 'dotnet nuget push "bin/Release/*.nupkg" \
    --source "'"$FEED_URL"'" \
    --api-key $GH_TOKEN \
    --skip-duplicate'

echo "✅ Done! Your package should now be visible at https://github.com/hoangsonww/End-to-End-Data-Pipeline/packages"
