"""
Tests for learning_engine.py — draft comparison and mode recommendations.
"""

import sys
import os
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test-key")

from learning_engine import (
    _similarity,
    _strip_html,
    _find_matching_sent,
    get_mode_recommendation,
)


class TestSimilarity:
    """Tests for text similarity calculation."""

    def test_identical_texts(self):
        assert _similarity("hello world", "hello world") == 1.0

    def test_completely_different(self):
        sim = _similarity("abc", "xyz")
        assert sim < 0.5

    def test_similar_texts(self):
        sim = _similarity(
            "Dear client, please send the updated plans",
            "Dear client, please send the updated building plans",
        )
        assert sim > 0.7

    def test_empty_text(self):
        assert _similarity("", "hello") == 0.0
        assert _similarity("hello", "") == 0.0
        assert _similarity("", "") == 0.0

    def test_none_input(self):
        assert _similarity(None, "hello") == 0.0
        assert _similarity("hello", None) == 0.0


class TestStripHtml:
    """Tests for HTML stripping."""

    def test_strip_tags(self):
        assert _strip_html("<p>Hello <strong>World</strong></p>") == "Hello World"

    def test_strip_multiple_spaces(self):
        result = _strip_html("<p>Hello</p>  <p>World</p>")
        assert "  " not in result

    def test_empty_html(self):
        assert _strip_html("") == ""


class TestFindMatchingSent:
    """Tests for draft-to-sent email matching."""

    def test_match_by_recipient_and_subject(self):
        draft = {"to": "client@test.com", "subject": "Follow-up: Project Alpha"}
        sent_emails = [
            {"to": "other@test.com", "subject": "Unrelated"},
            {"to": "client@test.com", "subject": "Follow-up: Project Alpha"},
        ]
        match = _find_matching_sent(draft, sent_emails)
        assert match is not None
        assert match["to"] == "client@test.com"

    def test_no_match_wrong_recipient(self):
        draft = {"to": "client@test.com", "subject": "Follow-up"}
        sent_emails = [
            {"to": "other@test.com", "subject": "Follow-up"},
        ]
        match = _find_matching_sent(draft, sent_emails)
        assert match is None

    def test_no_match_different_subject(self):
        draft = {"to": "client@test.com", "subject": "Follow-up: Project Alpha"}
        sent_emails = [
            {"to": "client@test.com", "subject": "Completely different topic"},
        ]
        match = _find_matching_sent(draft, sent_emails)
        assert match is None

    def test_best_match_selected(self):
        draft = {"to": "client@test.com", "subject": "Follow-up: Project Alpha update"}
        sent_emails = [
            {"to": "client@test.com", "subject": "Follow-up: Project Alpha update needed"},
            {"to": "client@test.com", "subject": "Follow-up: Something else"},
        ]
        match = _find_matching_sent(draft, sent_emails)
        assert "Alpha" in match["subject"]


class TestModeRecommendation:
    """Tests for mode transition recommendations."""

    @patch("learning_engine.load_metrics")
    def test_insufficient_data_stays_draft(self, mock_metrics):
        mock_metrics.return_value = {"total_drafts": 5, "approval_rate": 0.9}
        rec = get_mode_recommendation()
        assert rec["recommendation"] == "DRAFT"
        assert "Not enough data" in rec["reason"]

    @patch("learning_engine.load_metrics")
    def test_high_approval_recommends_auto(self, mock_metrics):
        mock_metrics.return_value = {"total_drafts": 30, "approval_rate": 0.96}
        rec = get_mode_recommendation()
        assert rec["recommendation"] == "AUTO"

    @patch("learning_engine.load_metrics")
    def test_medium_approval_recommends_semi_auto(self, mock_metrics):
        mock_metrics.return_value = {"total_drafts": 25, "approval_rate": 0.85}
        rec = get_mode_recommendation()
        assert rec["recommendation"] == "SEMI_AUTO"

    @patch("learning_engine.load_metrics")
    def test_low_approval_stays_draft(self, mock_metrics):
        mock_metrics.return_value = {"total_drafts": 25, "approval_rate": 0.5}
        rec = get_mode_recommendation()
        assert rec["recommendation"] == "DRAFT"
