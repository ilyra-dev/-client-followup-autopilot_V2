"""
Tests for config.py — environment loading and constants.
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")

import config


class TestConfig:
    """Tests for configuration loading."""

    def test_system_mode_valid(self):
        assert config.SYSTEM_MODE in ("DRAFT", "SEMI_AUTO", "AUTO")

    def test_followup_schedule_has_4_stages(self):
        assert len(config.FOLLOWUP_SCHEDULE) == 4
        assert 1 in config.FOLLOWUP_SCHEDULE
        assert 4 in config.FOLLOWUP_SCHEDULE

    def test_followup_schedule_values_ascending(self):
        values = [config.FOLLOWUP_SCHEDULE[k] for k in sorted(config.FOLLOWUP_SCHEDULE.keys())]
        for i in range(1, len(values)):
            assert values[i] > values[i - 1], f"Schedule not ascending: {values}"

    def test_supported_languages(self):
        assert config.SUPPORTED_LANGUAGES == ("ES", "EN", "PT")

    def test_language_date_formats_complete(self):
        for lang in config.SUPPORTED_LANGUAGES:
            assert lang in config.LANGUAGE_DATE_FORMATS

    def test_country_timezones_has_12_countries(self):
        assert len(config.COUNTRY_TIMEZONES) == 12

    def test_business_hours_valid(self):
        assert 0 <= config.BUSINESS_HOURS_START < 24
        assert 0 < config.BUSINESS_HOURS_END <= 24
        assert config.BUSINESS_HOURS_START < config.BUSINESS_HOURS_END

    def test_rate_limits_positive(self):
        assert config.NOTION_RATE_LIMIT_RPS > 0
        assert config.GMAIL_SEND_DELAY > 0
        assert config.SLACK_SEND_DELAY > 0

    def test_tmp_dir_path(self):
        assert isinstance(config.TMP_DIR, Path)

    def test_polling_intervals_positive(self):
        assert config.POLL_INTERVAL_OUTBOUND > 0
        assert config.POLL_INTERVAL_TEAM_INBOUND > 0
        assert config.POLL_INTERVAL_CLIENT_INBOUND > 0
