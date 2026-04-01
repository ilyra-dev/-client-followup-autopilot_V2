"""
Health check for the Client Follow-Up Autopilot daemon.
Reads the heartbeat file to determine if the daemon is running and healthy.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import HEARTBEAT_PATH, DAEMON_LOG_PATH


def check_health(max_age_seconds=120):
    """
    Check if the daemon is running and healthy.

    Args:
        max_age_seconds: Max age of heartbeat before considered unhealthy (default 2 min)

    Returns:
        Dict with 'status' ('healthy', 'unhealthy', 'not_running'),
        'last_heartbeat', 'age_seconds', 'details'
    """
    if not HEARTBEAT_PATH.exists():
        return {
            "status": "not_running",
            "last_heartbeat": None,
            "age_seconds": None,
            "details": "No heartbeat file found. Daemon has never started or heartbeat was deleted.",
        }

    try:
        heartbeat_str = HEARTBEAT_PATH.read_text().strip()
        last_heartbeat = datetime.fromisoformat(heartbeat_str)

        now = datetime.now(timezone.utc)
        if last_heartbeat.tzinfo is None:
            last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)

        age = (now - last_heartbeat).total_seconds()

        if age <= max_age_seconds:
            return {
                "status": "healthy",
                "last_heartbeat": heartbeat_str,
                "age_seconds": round(age),
                "details": f"Daemon is running. Last heartbeat {round(age)}s ago.",
            }
        else:
            return {
                "status": "unhealthy",
                "last_heartbeat": heartbeat_str,
                "age_seconds": round(age),
                "details": f"Heartbeat is {round(age)}s old (threshold: {max_age_seconds}s). Daemon may be stuck or crashed.",
            }

    except (ValueError, OSError) as e:
        return {
            "status": "unhealthy",
            "last_heartbeat": None,
            "age_seconds": None,
            "details": f"Could not read heartbeat: {e}",
        }


def get_recent_logs(lines=20):
    """Get the last N lines from the daemon log."""
    if not DAEMON_LOG_PATH.exists():
        return "No daemon log found."

    try:
        with open(DAEMON_LOG_PATH, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        return "".join(all_lines[-lines:])
    except OSError as e:
        return f"Could not read log: {e}"


if __name__ == "__main__":
    result = check_health()

    # Color output
    status_colors = {"healthy": "\033[92m", "unhealthy": "\033[93m", "not_running": "\033[91m"}
    reset = "\033[0m"
    color = status_colors.get(result["status"], "")

    print(f"\n{color}Status: {result['status'].upper()}{reset}")
    print(f"Details: {result['details']}")
    if result["last_heartbeat"]:
        print(f"Last heartbeat: {result['last_heartbeat']}")
    if result["age_seconds"] is not None:
        print(f"Age: {result['age_seconds']}s")

    if "--logs" in sys.argv:
        print(f"\n--- Recent Logs ---")
        print(get_recent_logs(30))

    if "--json" in sys.argv:
        print(json.dumps(result, indent=2))

    # Exit code for monitoring scripts
    sys.exit(0 if result["status"] == "healthy" else 1)
