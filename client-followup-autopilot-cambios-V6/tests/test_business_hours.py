"""
Tests for is_within_business_hours in compute_next_followup.py.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")

from compute_next_followup import is_within_business_hours


class TestBusinessHours:
    """Tests for business hours enforcement."""

    @patch("compute_next_followup.datetime")
    def test_within_business_hours(self, mock_dt):
        """10 AM on a weekday should be within business hours."""
        # Mock Wednesday 10:00 UTC
        mock_now = datetime(2026, 2, 18, 10, 0, 0, tzinfo=timezone.utc)  # Wednesday
        mock_dt.now.return_value = mock_now
        mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)

        result = is_within_business_hours(start_hour=8, end_hour=18)
        assert result is True

    @patch("compute_next_followup.datetime")
    def test_outside_business_hours_early(self, mock_dt):
        """5 AM on a weekday should be outside business hours."""
        mock_now = datetime(2026, 2, 18, 5, 0, 0, tzinfo=timezone.utc)  # Wednesday 5AM
        mock_dt.now.return_value = mock_now
        mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)

        result = is_within_business_hours(start_hour=8, end_hour=18)
        assert result is False

    @patch("compute_next_followup.datetime")
    def test_outside_business_hours_late(self, mock_dt):
        """10 PM on a weekday should be outside business hours."""
        mock_now = datetime(2026, 2, 18, 22, 0, 0, tzinfo=timezone.utc)  # Wednesday 10PM
        mock_dt.now.return_value = mock_now
        mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)

        result = is_within_business_hours(start_hour=8, end_hour=18)
        assert result is False

    @patch("compute_next_followup.datetime")
    def test_weekend_not_business_hours(self, mock_dt):
        """Saturday at 10 AM should NOT be within business hours."""
        mock_now = datetime(2026, 2, 21, 10, 0, 0, tzinfo=timezone.utc)  # Saturday
        mock_dt.now.return_value = mock_now
        mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)

        result = is_within_business_hours(start_hour=8, end_hour=18)
        assert result is False

    def test_unknown_country_defaults_to_utc(self):
        """Unknown country should not crash, defaults to UTC."""
        result = is_within_business_hours(country="Atlantis")
        assert isinstance(result, bool)

    def test_none_country_defaults_to_utc(self):
        """None country should work fine."""
        result = is_within_business_hours(country=None)
        assert isinstance(result, bool)

    def test_known_country_accepted(self):
        """Known countries should not crash."""
        for country in ["México", "Colombia", "Chile", "Argentina", "España"]:
            result = is_within_business_hours(country=country)
            assert isinstance(result, bool)
