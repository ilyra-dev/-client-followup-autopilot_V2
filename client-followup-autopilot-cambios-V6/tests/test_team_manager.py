"""
Tests for team_manager.py — team member resolution and routing.
Notion API is mocked.
"""

import sys
import os
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")
os.environ.setdefault("NOTION_TEAM_DATABASE_ID", "test-team-db")
os.environ.setdefault("CS_TEAM_EMAIL", "cs-team@example.com")

import team_manager


MOCK_MEMBERS = [
    {"name": "Alice Admin", "email": "alice@example.com", "role": "admin", "languages": ["ES", "EN", "PT"]},
    {"name": "Bob CS", "email": "bob@example.com", "role": "cs", "languages": ["ES", "EN"]},
    {"name": "Carlos CS", "email": "carlos@example.com", "role": "cs", "languages": ["ES", "PT"]},
    {"name": "Diana Analyst", "email": "diana@example.com", "role": "member", "languages": ["EN"]},
]


class TestTeamManager:
    """Tests for team member functions."""

    def setup_method(self):
        """Reset cache before each test."""
        team_manager._cache["members"] = []
        team_manager._cache["last_refresh"] = 0.0

    @patch.object(team_manager, "get_team_members", return_value=MOCK_MEMBERS)
    def test_resolve_email_exact_match(self, mock_get):
        result = team_manager.resolve_email("Bob CS")
        assert result == "bob@example.com"

    @patch.object(team_manager, "get_team_members", return_value=MOCK_MEMBERS)
    def test_resolve_email_case_insensitive(self, mock_get):
        result = team_manager.resolve_email("bob cs")
        assert result == "bob@example.com"

    @patch.object(team_manager, "get_team_members", return_value=MOCK_MEMBERS)
    def test_resolve_email_not_found(self, mock_get):
        result = team_manager.resolve_email("Nonexistent Person")
        assert result is None

    @patch.object(team_manager, "get_team_members", return_value=MOCK_MEMBERS)
    def test_resolve_email_empty_name(self, mock_get):
        result = team_manager.resolve_email("")
        assert result is None

    @patch.object(team_manager, "get_team_members", return_value=MOCK_MEMBERS)
    def test_get_cc_recipients_es(self, mock_get):
        """ES client: should include admin + ES-speaking members."""
        cc = team_manager.get_cc_recipients("ES")
        assert "alice@example.com" in cc  # admin (always)
        assert "bob@example.com" in cc  # speaks ES
        assert "carlos@example.com" in cc  # speaks ES

    @patch.object(team_manager, "get_team_members", return_value=MOCK_MEMBERS)
    def test_get_cc_recipients_en(self, mock_get):
        """EN client: should include admin + EN-speaking members."""
        cc = team_manager.get_cc_recipients("EN")
        assert "alice@example.com" in cc  # admin
        assert "diana@example.com" in cc  # speaks EN

    @patch.object(team_manager, "get_team_members", return_value=[])
    def test_get_cc_fallback_to_cs_team_email(self, mock_get):
        """No team members should fallback to CS_TEAM_EMAIL."""
        cc = team_manager.get_cc_recipients("ES")
        assert cc == "cs-team@example.com"

    @patch.object(team_manager, "get_team_members", return_value=MOCK_MEMBERS)
    def test_get_admins_cc(self, mock_get):
        result = team_manager.get_admins_cc()
        assert "alice@example.com" in result
        assert "bob@example.com" not in result

    @patch.object(team_manager, "get_team_members", return_value=MOCK_MEMBERS)
    def test_get_daily_summary_recipients(self, mock_get):
        """Daily summary goes to everyone."""
        recipients = team_manager.get_daily_summary_recipients()
        assert len(recipients) == 4

    @patch.object(team_manager, "get_team_members", return_value=MOCK_MEMBERS)
    def test_get_cs_members(self, mock_get):
        """Should return only cs/member roles."""
        cs = team_manager.get_cs_members()
        names = [m["name"] for m in cs]
        assert "Bob CS" in names
        assert "Carlos CS" in names
        assert "Diana Analyst" in names
        assert "Alice Admin" not in names
