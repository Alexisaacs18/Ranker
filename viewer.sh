#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-9000}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Serving viewer on http://localhost:${PORT}"
echo "Press Ctrl+C to stop."
cd "$SCRIPT_DIR"

python -m http.server "$PORT"
