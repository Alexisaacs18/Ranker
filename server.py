#!/usr/bin/env python3
"""
Flask server for the Medical Provider Risk Database viewer.
Serves static files and provides /api/run/<script> endpoints to run Python scripts.
"""

import os
import shutil
import signal
import subprocess
import sys
import threading
from pathlib import Path

from flask import Flask, abort, jsonify, request, send_from_directory

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"

# Process tracking for /api/stop
_current_process = None
_current_script = None
_run_lock = threading.Lock()
_stop_requested = False  # set by /api/stop when we kill; run_script adds "stopped": True and clears

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
    "combine_website_scrapes": ("combine_website_scrapes.py", [], 60),
    "pubmed_trending": ("pubmed_trending_scraper.py", [], 600),
    "converter": ("converter.py", [], 120),
    "gpt_ranker": ("gpt_ranker.py", ["--chunk-size", "0", "--max-rows", "100"], 600),
    "website_scraper": ("website_scraper.py", [], 300),
}

app = Flask(__name__)


@app.route("/")
def index():
    return send_from_directory(SCRIPT_DIR, "index.html")


@app.route("/api/run/<script>", methods=["POST"])
def run_script(script):
    global _current_process, _current_script, _stop_requested
    try:
        if script not in RUNNERS:
            return jsonify({"ok": False, "error": f"Unknown script: {script}"}), 400

        py_file, args, timeout = RUNNERS[script]
        py_path = SCRIPT_DIR / py_file
        if not py_path.is_file():
            return jsonify({"ok": False, "error": f"Script not found: {py_file}"}), 500

        with _run_lock:
            if _current_process is not None and _current_process.poll() is None:
                return jsonify({"ok": False, "error": "A script is already running. Stop it first."}), 409

        # Handle URL parameter for website_scraper
        if script == "website_scraper":
            try:
                data = request.get_json() or {}
            except Exception as e:
                return jsonify({"ok": False, "error": f"Invalid JSON in request: {str(e)}"}), 400
            
            # Safely get URL
            url = data.get("url")
            if url is None:
                url = ""
            url = str(url).strip() if url else ""
            if not url:
                return jsonify({"ok": False, "error": "URL is required"}), 400
            
            # Safely get max_pages
            max_pages = data.get("max_pages", 1)
            try:
                max_pages = int(max_pages) if max_pages is not None else 1
            except (ValueError, TypeError):
                max_pages = 1
            
            # Safely get optional parameters
            link_selector = data.get("link_selector")
            if link_selector is not None:
                link_selector = str(link_selector).strip() or None
            else:
                link_selector = None
            
            url_pattern = data.get("url_pattern")
            if url_pattern is not None:
                url_pattern = str(url_pattern).strip() or None
            else:
                url_pattern = None
            
            args = [url, str(max_pages)]
            if link_selector:
                args.append(link_selector)
            if url_pattern:
                args.append(url_pattern)

        # Build command - args is already set from RUNNERS or modified for website_scraper
        cmd = [PYTHON_FOR_SCRIPTS, str(py_path)] + args
        
        try:
            # Create new process group on Unix to allow killing child processes
            kwargs = {
                "cwd": str(SCRIPT_DIR),
                "stdout": subprocess.PIPE,
                "stderr": subprocess.PIPE,
                "text": True,
            }
            # Use setsid on Unix systems (Linux, macOS) to create new process group
            if hasattr(os, 'setsid') and sys.platform != 'win32':
                try:
                    kwargs["preexec_fn"] = os.setsid
                except (AttributeError, OSError):
                    # If setsid fails, just continue without it
                    pass
            proc = subprocess.Popen(cmd, **kwargs)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Failed to start process: {str(e)}"}), 500

        with _run_lock:
            _current_process = proc
            _current_script = script

        try:
            stdout, stderr = proc.communicate(timeout=timeout)
            stopped = _stop_requested
            if _stop_requested:
                _stop_requested = False
            return jsonify({
                "ok": True,
                "stdout": stdout or "",
                "stderr": stderr or "",
                "returncode": proc.returncode,
                "stopped": stopped,
            })
        except KeyboardInterrupt:
            # Handle interruption
            proc.kill()
            proc.communicate()
            _stop_requested = False
            return jsonify({
                "ok": True,
                "stdout": "",
                "stderr": "",
                "returncode": proc.returncode,
                "stopped": True,
            })
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            return jsonify({"ok": False, "error": f"Script timed out after {timeout}s"}), 504
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"Error in run_script: {error_details}", file=sys.stderr)
            if proc.poll() is None:
                try:
                    proc.kill()
                    proc.wait(timeout=2)
                except:
                    pass
            return jsonify({"ok": False, "error": f"{str(e)}"}), 500
        finally:
            with _run_lock:
                _current_process = None
                _current_script = None
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Unhandled error in run_script: {error_details}", file=sys.stderr)
        return jsonify({"ok": False, "error": f"Unexpected error: {str(e)}"}), 500


@app.route("/api/stop", methods=["POST"])
def stop_script():
    global _stop_requested
    with _run_lock:
        proc = _current_process
    if proc is not None:
        try:
            if proc.poll() is None:  # Process is still running
                _stop_requested = True
                try:
                    # Kill the entire process group to handle child processes
                    if hasattr(os, 'setsid') and sys.platform != 'win32':
                        try:
                            pgid = os.getpgid(proc.pid)
                            os.killpg(pgid, signal.SIGTERM)
                            try:
                                proc.wait(timeout=2)
                            except subprocess.TimeoutExpired:
                                try:
                                    os.killpg(pgid, signal.SIGKILL)
                                except (ProcessLookupError, OSError):
                                    pass
                                try:
                                    proc.wait(timeout=1)
                                except subprocess.TimeoutExpired:
                                    pass
                        except (ProcessLookupError, OSError):
                            # Process group doesn't exist, fall back to direct kill
                            proc.terminate()
                            try:
                                proc.wait(timeout=2)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait(timeout=1)
                    else:
                        # Windows or systems without setsid
                        proc.terminate()
                        try:
                            proc.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            try:
                                proc.wait(timeout=1)
                            except subprocess.TimeoutExpired:
                                pass
                except (ProcessLookupError, OSError) as e:
                    # Process already terminated or doesn't exist
                    pass
                return jsonify({"ok": True, "message": "Script stopped"})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True, "message": "No script running"})


@app.route("/api/delete-data", methods=["POST"])
def delete_data():
    """Clear all files and subdirectories in the data folder."""
    if not DATA_DIR.exists():
        return jsonify({"ok": True, "message": "Data folder already empty"})

    try:
        # Ensure we only operate on a path under SCRIPT_DIR
        data_resolved = DATA_DIR.resolve()
        script_resolved = SCRIPT_DIR.resolve()
        if not str(data_resolved).startswith(str(script_resolved)):
            return jsonify({"ok": False, "error": "Invalid data path"}), 500

        shutil.rmtree(DATA_DIR)
        DATA_DIR.mkdir(parents=True)
        return jsonify({"ok": True, "message": "Data folder cleared"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({"ok": False, "error": "Not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    import traceback
    error_msg = str(error) if error else "Unknown error"
    traceback.print_exc()
    return jsonify({"ok": False, "error": f"Internal server error: {error_msg}"}), 500

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
