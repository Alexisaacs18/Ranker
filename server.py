#!/usr/bin/env python3
"""
Flask server for the Medical Provider Risk Database viewer.
Serves static files and provides /api/run/<script> endpoints to run Python scripts.
"""

import os
import subprocess
import sys
from pathlib import Path

from flask import Flask, abort, jsonify, request, send_from_directory

SCRIPT_DIR = Path(__file__).resolve().parent

# Prefer the project's venv when running scripts (so they get requests, beautifulsoup4, etc.)
# Order matches viewer.sh: .venv then venv
def _python_for_scripts():
    for name in (".venv", "venv"):
        p = SCRIPT_DIR / name / "bin" / "python"
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)
    return sys.executable

PYTHON_FOR_SCRIPTS = _python_for_scripts()

# Script name -> (python_file, args_list, timeout_seconds)
RUNNERS = {
    "test_connection": ("test_connection.py", [], 60),
    "scraper": ("unfiled_qui_tam_scraper.py", [], 600),
    "converter": ("converter.py", [], 120),
    "gpt_ranker": ("gpt_ranker.py", ["--chunk-size", "0", "--max-rows", "10"], 600),
}

app = Flask(__name__)


@app.route("/")
def index():
    return send_from_directory(SCRIPT_DIR, "index.html")


@app.route("/api/run/<script>", methods=["POST"])
def run_script(script):
    if script not in RUNNERS:
        return jsonify({"ok": False, "error": f"Unknown script: {script}"}), 400

    py_file, args, timeout = RUNNERS[script]
    py_path = SCRIPT_DIR / py_file
    if not py_path.is_file():
        return jsonify({"ok": False, "error": f"Script not found: {py_file}"}), 500

    cmd = [PYTHON_FOR_SCRIPTS, str(py_path)] + args
    try:
        r = subprocess.run(
            cmd,
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return jsonify({
            "ok": True,
            "stdout": r.stdout or "",
            "stderr": r.stderr or "",
            "returncode": r.returncode,
        })
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "error": f"Script timed out after {timeout}s"}), 504
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/<path:path>")
def static_files(path):
    if path.startswith("api/"):
        abort(404)
    if path.endswith(".py") or "/.git" in path or path.startswith(".git"):
        abort(404)
    full = SCRIPT_DIR / path
    if full.is_file():
        return send_from_directory(SCRIPT_DIR, path)
    abort(404)


def main():
    port = int(os.environ.get("PORT", 9000))
    print(f"Serving viewer on http://localhost:{port}")
    print(f"Scripts run with: {PYTHON_FOR_SCRIPTS}")
    print("Press Ctrl+C to stop.")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
