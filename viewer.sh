#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-9000}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Serving viewer on http://localhost:${PORT}"
echo "Press Ctrl+C to stop."
cd "$SCRIPT_DIR"
export PORT

# Run Flask server (serves static + /api/run/<script> for Python scripts).
# Uses .venv or venv if present (so scripts get requests, beautifulsoup4, etc.).
# On first run: python3 -m venv venv && venv/bin/pip install -r requirements.txt
if [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
  exec "$SCRIPT_DIR/.venv/bin/python" server.py
elif [ -x "$SCRIPT_DIR/venv/bin/python" ]; then
  exec "$SCRIPT_DIR/venv/bin/python" server.py
else
  exec python3 server.py
fi
