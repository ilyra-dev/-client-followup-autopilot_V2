"""
Tests for health_check.py — daemon health monitoring.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")

from health_check import check_health


class TestHealthCheck:
    """Tests for health check logic."""

    def test_no_heartbeat_file(self, tmp_path):
        """No heartbeat = not running."""
        hb_path = tmp_path / "heartbeat"
        with patch("health_check.HEARTBEAT_PATH", hb_path):
            result = check_health()
            assert result["status"] == "not_running"

    def test_fresh_heartbeat_is_healthy(self, tmp_path):
        """Recent heartbeat = healthy."""
        hb_path = tmp_path / "heartbeat"
        hb_path.write_text(datetime.now(timezone.utc).isoformat())

        with patch("health_check.HEARTBEAT_PATH", hb_path):
            result = check_health(max_age_seconds=120)
            assert result["status"] == "healthy"
            assert result["age_seconds"] is not None
            assert result["age_seconds"] < 120

    def test_stale_heartbeat_is_unhealthy(self, tmp_path):
        """Heartbeat older than threshold = unhealthy."""
        hb_path = tmp_path / "heartbeat"
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        hb_path.write_text(old_time.isoformat())

        with patch("health_check.HEARTBEAT_PATH", hb_path):
            result = check_health(max_age_seconds=120)
            assert result["status"] == "unhealthy"
            assert result["age_seconds"] > 120

    def test_corrupt_heartbeat_is_unhealthy(self, tmp_path):
        """Corrupt heartbeat file = unhealthy."""
        hb_path = tmp_path / "heartbeat"
        hb_path.write_text("not a valid timestamp")

        with patch("health_check.HEARTBEAT_PATH", hb_path):
            result = check_health()
            assert result["status"] == "unhealthy"

    def test_custom_max_age(self, tmp_path):
        """Custom max_age_seconds should be respected."""
        hb_path = tmp_path / "heartbeat"
        slightly_old = datetime.now(timezone.utc) - timedelta(seconds=30)
        hb_path.write_text(slightly_old.isoformat())

        with patch("health_check.HEARTBEAT_PATH", hb_path):
            # 60s threshold — 30s old should be healthy
            result = check_health(max_age_seconds=60)
            assert result["status"] == "healthy"

            # 10s threshold — 30s old should be unhealthy
            result = check_health(max_age_seconds=10)
            assert result["status"] == "unhealthy"
