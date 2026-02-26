"""
Tests for send_followup.py — follow-up execution logic.
All external APIs are mocked.
"""

import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test-key")
os.environ.setdefault("COMPANY_NAME", "TestCo")

from send_followup import send_followup_for_item, _is_valid_email, _check_already_sent


class TestEmailValidation:
    """Tests for email format validation."""

    def test_valid_email(self):
        assert _is_valid_email("test@example.com") is True

    def test_valid_email_with_dots(self):
        assert _is_valid_email("first.last@company.co.uk") is True

    def test_invalid_email_no_at(self):
        assert _is_valid_email("notanemail") is False

    def test_invalid_email_no_domain(self):
        assert _is_valid_email("test@") is False

    def test_empty_email(self):
        assert _is_valid_email("") is False

    def test_none_email(self):
        assert _is_valid_email(None) is False


class TestSendFollowupDraftMode:
    """Tests for send_followup_for_item in DRAFT mode."""

    def _make_item(self):
        return {
            "page_id": "page-001",
            "project_name": "Proyecto Test",
            "client_name": "Cliente Test",
            "client_email": "client@example.com",
            "senior_contact_email": "senior@example.com",
            "client_country": "México",
            "client_language": "ES",
            "pending_item": "Planos actualizados",
            "due_date": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
            "days_overdue": 3,
            "impact_description": "Falta info para auditoría",
            "follow_up_stage": 0,
            "next_stage": 1,
            "last_followup_date": "",
            "manual_override": False,
            "gmail_thread_id": "",
            "status": "Sin empezar",
            "client_success": "CS Member",
            "analista": "Analyst",
        }

    @patch("send_followup.notion_client")
    @patch("send_followup.draft_manager")
    @patch("send_followup.team_manager")
    @patch("send_followup.claude_client")
    def test_draft_mode_success(self, mock_claude, mock_team, mock_draft, mock_notion):
        """DRAFT mode should create a draft and return success."""
        mock_team.resolve_email.return_value = "cs@example.com"
        mock_team.get_admins_cc.return_value = "admin@example.com"
        mock_claude.generate_followup_email.return_value = {
            "subject": "Recordatorio", "body_html": "<p>Hola</p>"
        }
        mock_draft.create_draft_and_notify.return_value = {"draft_id": "draft-123", "slack_ts": "ts-1"}
        mock_notion.get_page.return_value = {"properties": {"Follow-Up Stage": {"type": "number", "number": 0}}}
        mock_notion.get_number_property.return_value = 0

        with patch("send_followup.SYSTEM_MODE", "DRAFT"):
            result = send_followup_for_item(self._make_item())
            assert result["success"] is True
            assert result["draft_id"] == "draft-123"

    @patch("send_followup.notion_client")
    @patch("send_followup.team_manager")
    @patch("send_followup.claude_client")
    def test_no_recipient_email_fails(self, mock_claude, mock_team, mock_notion):
        """Should fail gracefully if no recipient email."""
        mock_team.resolve_email.return_value = None
        mock_team.get_admins_cc.return_value = ""
        mock_notion.get_page.return_value = {"properties": {"Follow-Up Stage": {"type": "number", "number": 0}}}
        mock_notion.get_number_property.return_value = 0

        item = self._make_item()
        item["client_email"] = ""

        result = send_followup_for_item(item)
        assert result["success"] is False
        assert "No recipient" in result.get("error", "")

    @patch("send_followup.notion_client")
    @patch("send_followup.team_manager")
    @patch("send_followup.claude_client")
    def test_invalid_email_format_fails(self, mock_claude, mock_team, mock_notion):
        """Should fail if email format is invalid."""
        mock_team.resolve_email.return_value = None
        mock_team.get_admins_cc.return_value = ""
        mock_notion.get_page.return_value = {"properties": {"Follow-Up Stage": {"type": "number", "number": 0}}}
        mock_notion.get_number_property.return_value = 0

        item = self._make_item()
        item["client_email"] = "not-an-email"

        result = send_followup_for_item(item)
        assert result["success"] is False
        assert "Invalid email" in result.get("error", "")

    @patch("send_followup.notion_client")
    @patch("send_followup.team_manager")
    @patch("send_followup.claude_client")
    def test_stage_4_uses_senior_contact(self, mock_claude, mock_team, mock_notion):
        """Stage 4 should send to senior contact email."""
        mock_team.resolve_email.return_value = "cs@example.com"
        mock_team.get_admins_cc.return_value = ""
        mock_claude.generate_followup_email.return_value = {
            "subject": "Escalation", "body_html": "<p>Dear Director</p>"
        }
        mock_notion.get_page.return_value = {"properties": {"Follow-Up Stage": {"type": "number", "number": 3}}}
        mock_notion.get_number_property.return_value = 3

        item = self._make_item()
        item["follow_up_stage"] = 3
        item["next_stage"] = 4

        with patch("send_followup.SYSTEM_MODE", "DRAFT"), \
             patch("send_followup.draft_manager") as mock_draft:
            mock_draft.create_draft_and_notify.return_value = {"draft_id": "d-4", "slack_ts": "ts"}
            result = send_followup_for_item(item)

            # Verify it was called with senior contact
            call_args = mock_draft.create_draft_and_notify.call_args
            assert call_args.kwargs.get("to") == "senior@example.com" or \
                   (call_args.args and call_args.args[0] == "senior@example.com") or \
                   "senior@example.com" in str(call_args)

    @patch("send_followup.notion_client")
    @patch("send_followup.team_manager")
    @patch("send_followup.claude_client")
    def test_deduplication_prevents_resend(self, mock_claude, mock_team, mock_notion):
        """Should skip if stage already sent (dedup check)."""
        mock_team.resolve_email.return_value = None
        mock_team.get_admins_cc.return_value = ""
        # Simulate that Notion already shows stage 1 completed
        mock_notion.get_page.return_value = {"properties": {"Follow-Up Stage": {"type": "number", "number": 1}}}
        mock_notion.get_number_property.return_value = 1

        item = self._make_item()
        item["next_stage"] = 1  # Trying to send stage 1 again

        result = send_followup_for_item(item)
        assert result["success"] is False
        assert "Already sent" in result.get("error", "")


class TestFallbackTemplate:
    """Tests for fallback template loading."""

    def test_fallback_template_exists(self):
        """All expected fallback templates should exist."""
        templates_dir = Path(__file__).parent.parent / "tools" / "templates"
        expected = [
            "reminder_es.html", "reminder_en.html", "reminder_pt.html",
            "second_notice_es.html", "second_notice_en.html", "second_notice_pt.html",
            "urgent_es.html", "urgent_en.html", "urgent_pt.html",
            "escalation_es.html", "escalation_en.html", "escalation_pt.html",
        ]
        for template in expected:
            assert (templates_dir / template).exists(), f"Missing template: {template}"
