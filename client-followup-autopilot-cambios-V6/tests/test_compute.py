"""
Tests for compute_next_followup.py — date/schedule logic.
No external API calls needed; pure logic tests.
"""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")

from compute_next_followup import (
    compute_next_followup_date,
    is_followup_due,
    days_overdue,
)


class TestComputeNextFollowupDate:
    """Tests for compute_next_followup_date()."""

    def _today(self):
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    def test_stage_0_overdue_returns_today(self):
        """Stage 0 with overdue item should return today."""
        yesterday = (self._today() - timedelta(days=1)).strftime("%Y-%m-%d")
        result = compute_next_followup_date(0, due_date=yesterday)
        assert result == self._today()

    def test_stage_0_future_due_date_returns_due_date(self):
        """Stage 0 with future due date should return the due date."""
        tomorrow = self._today() + timedelta(days=1)
        result = compute_next_followup_date(0, due_date=tomorrow.strftime("%Y-%m-%d"))
        assert result == tomorrow

    def test_stage_0_no_due_date_returns_today(self):
        """Stage 0 with no due date should return today."""
        result = compute_next_followup_date(0, due_date=None)
        assert result == self._today()

    def test_stage_1_to_2_after_3_days(self):
        """Stage 1→2 should be 3 days after last follow-up."""
        three_days_ago = (self._today() - timedelta(days=3)).strftime("%Y-%m-%d")
        result = compute_next_followup_date(1, last_followup_date=three_days_ago)
        assert result == self._today()

    def test_stage_2_to_3_after_4_days(self):
        """Stage 2→3 should be 4 days after last follow-up."""
        four_days_ago = (self._today() - timedelta(days=4)).strftime("%Y-%m-%d")
        result = compute_next_followup_date(2, last_followup_date=four_days_ago)
        assert result == self._today()

    def test_stage_3_to_4_after_7_days(self):
        """Stage 3→4 should be 7 days after last follow-up."""
        seven_days_ago = (self._today() - timedelta(days=7)).strftime("%Y-%m-%d")
        result = compute_next_followup_date(3, last_followup_date=seven_days_ago)
        assert result == self._today()

    def test_stage_4_returns_none(self):
        """Stage 4 (completed) should return None."""
        result = compute_next_followup_date(4)
        assert result is None

    def test_no_last_followup_returns_today(self):
        """Stage 1+ with no last follow-up date should fallback to today."""
        result = compute_next_followup_date(1, last_followup_date=None)
        assert result == self._today()

    def test_string_dates_parsed_correctly(self):
        """Should handle string date inputs."""
        date_str = (self._today() - timedelta(days=1)).strftime("%Y-%m-%d")
        result = compute_next_followup_date(0, due_date=date_str)
        assert isinstance(result, datetime)

    def test_datetime_dates_accepted(self):
        """Should handle datetime inputs directly."""
        yesterday = self._today() - timedelta(days=1)
        result = compute_next_followup_date(0, due_date=yesterday)
        assert result == self._today()


class TestIsFollowupDue:
    """Tests for is_followup_due()."""

    def _today(self):
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    def test_stage_0_overdue_is_due(self):
        """Overdue item at stage 0 should be due."""
        yesterday = (self._today() - timedelta(days=1)).strftime("%Y-%m-%d")
        is_due, next_stage, next_date = is_followup_due(0, due_date=yesterday)
        assert is_due is True
        assert next_stage == 1

    def test_stage_0_future_not_due(self):
        """Item with future due date at stage 0 should NOT be due."""
        tomorrow = (self._today() + timedelta(days=1)).strftime("%Y-%m-%d")
        is_due, next_stage, next_date = is_followup_due(0, due_date=tomorrow)
        assert is_due is False
        assert next_stage == 1

    def test_stage_1_not_yet_due(self):
        """Stage 1 with recent follow-up (1 day ago) should NOT be due yet."""
        yesterday = (self._today() - timedelta(days=1)).strftime("%Y-%m-%d")
        is_due, next_stage, _ = is_followup_due(1, last_followup_date=yesterday)
        assert is_due is False
        assert next_stage == 2

    def test_stage_1_is_due_after_3_days(self):
        """Stage 1 with follow-up 3+ days ago should be due."""
        three_days_ago = (self._today() - timedelta(days=3)).strftime("%Y-%m-%d")
        is_due, next_stage, _ = is_followup_due(1, last_followup_date=three_days_ago)
        assert is_due is True
        assert next_stage == 2

    def test_stage_4_never_due(self):
        """Stage 4 should never be due (all stages completed)."""
        is_due, next_stage, next_date = is_followup_due(4)
        assert is_due is False
        assert next_stage is None
        assert next_date is None


class TestDaysOverdue:
    """Tests for days_overdue()."""

    def _today(self):
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    def test_overdue_3_days(self):
        three_days_ago = (self._today() - timedelta(days=3)).strftime("%Y-%m-%d")
        assert days_overdue(three_days_ago) == 3

    def test_due_today_is_zero(self):
        today_str = self._today().strftime("%Y-%m-%d")
        assert days_overdue(today_str) == 0

    def test_future_date_is_zero(self):
        tomorrow = (self._today() + timedelta(days=1)).strftime("%Y-%m-%d")
        assert days_overdue(tomorrow) == 0

    def test_none_date_is_zero(self):
        assert days_overdue(None) == 0

    def test_empty_string_is_zero(self):
        assert days_overdue("") == 0

    def test_datetime_input(self):
        three_days_ago = self._today() - timedelta(days=3)
        assert days_overdue(three_days_ago) == 3
