#!/usr/bin/env python3
"""
Test runner for Client Follow-Up Autopilot.
Uses unittest (stdlib) — no pytest required.
Loads stubs for missing third-party modules.
"""

import sys
import os
import unittest
import tempfile
import json
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

# ─── Setup ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
TOOLS_DIR = ROOT / "tools"

# Set env vars BEFORE any imports
os.environ["SYSTEM_MODE"] = "DRAFT"
os.environ["ANTHROPIC_API_KEY"] = "sk-ant-test-key"
os.environ["NOTION_API_KEY"] = "secret_test"
os.environ["NOTION_DATABASE_ID"] = "test-db-id"
os.environ["NOTION_TEAM_DATABASE_ID"] = "test-team-db"
os.environ["SLACK_BOT_TOKEN"] = "xoxb-test"
os.environ["SLACK_REVIEW_CHANNEL"] = "C0TEST"
os.environ["SLACK_DEFAULT_CHANNEL"] = "C0TEST2"
os.environ["GMAIL_SENDER_EMAIL"] = "test@example.com"
os.environ["GMAIL_DEFAULT_SENDER_EMAIL"] = "test@example.com"
os.environ["COMPANY_NAME"] = "TestCo"
os.environ["CS_TEAM_EMAIL"] = "cs@example.com"
os.environ["GMAIL_AUTH_MODE"] = "oauth2"

# Load stubs for missing packages
sys.path.insert(0, str(Path(__file__).parent))
import stubs  # noqa: F401

# Now safe to add tools to path
sys.path.insert(0, str(TOOLS_DIR))


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 1: Config
# ═══════════════════════════════════════════════════════════════════════════════
import config

class TestConfig(unittest.TestCase):
    def test_system_mode_valid(self):
        self.assertIn(config.SYSTEM_MODE, ("DRAFT", "SEMI_AUTO", "AUTO"))

    def test_followup_schedule_4_stages(self):
        self.assertEqual(len(config.FOLLOWUP_SCHEDULE), 4)
        for stage in [1, 2, 3, 4]:
            self.assertIn(stage, config.FOLLOWUP_SCHEDULE)

    def test_schedule_values_ascending(self):
        vals = [config.FOLLOWUP_SCHEDULE[k] for k in sorted(config.FOLLOWUP_SCHEDULE)]
        for i in range(1, len(vals)):
            self.assertGreater(vals[i], vals[i-1])

    def test_supported_languages(self):
        self.assertEqual(config.SUPPORTED_LANGUAGES, ("ES", "EN", "PT"))

    def test_language_date_formats(self):
        for lang in config.SUPPORTED_LANGUAGES:
            self.assertIn(lang, config.LANGUAGE_DATE_FORMATS)

    def test_country_timezones_count(self):
        self.assertEqual(len(config.COUNTRY_TIMEZONES), 12)

    def test_business_hours_valid(self):
        self.assertGreaterEqual(config.BUSINESS_HOURS_START, 0)
        self.assertLess(config.BUSINESS_HOURS_START, config.BUSINESS_HOURS_END)
        self.assertLessEqual(config.BUSINESS_HOURS_END, 24)

    def test_rate_limits_positive(self):
        self.assertGreater(config.NOTION_RATE_LIMIT_RPS, 0)

    def test_polling_intervals_positive(self):
        self.assertGreater(config.POLL_INTERVAL_OUTBOUND, 0)
        self.assertGreater(config.POLL_INTERVAL_TEAM_INBOUND, 0)
        self.assertGreater(config.POLL_INTERVAL_CLIENT_INBOUND, 0)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 2: Notion Property Helpers
# ═══════════════════════════════════════════════════════════════════════════════
from notion_client import (
    get_text_property, get_select_property, get_date_property,
    get_number_property, get_checkbox_property, get_email_property,
    get_multi_select_property, get_status_property, get_people_property,
    get_people_first, get_rollup_text,
    build_select, build_number, build_date, build_checkbox, build_rich_text,
    build_email, build_status,
)

class TestNotionPropertyExtractors(unittest.TestCase):
    def test_text_from_title(self):
        page = {"properties": {"N": {"type": "title", "title": [{"plain_text": "Hello"}]}}}
        self.assertEqual(get_text_property(page, "N"), "Hello")

    def test_text_from_rich_text(self):
        page = {"properties": {"N": {"type": "rich_text", "rich_text": [{"plain_text": "Hi"}]}}}
        self.assertEqual(get_text_property(page, "N"), "Hi")

    def test_text_empty_title(self):
        page = {"properties": {"N": {"type": "title", "title": []}}}
        self.assertEqual(get_text_property(page, "N"), "")

    def test_text_missing_property(self):
        self.assertEqual(get_text_property({"properties": {}}, "X"), "")

    def test_select(self):
        page = {"properties": {"L": {"type": "select", "select": {"name": "ES"}}}}
        self.assertEqual(get_select_property(page, "L"), "ES")

    def test_select_none(self):
        page = {"properties": {"L": {"type": "select", "select": None}}}
        self.assertEqual(get_select_property(page, "L"), "")

    def test_date(self):
        page = {"properties": {"D": {"type": "date", "date": {"start": "2026-01-15"}}}}
        self.assertEqual(get_date_property(page, "D"), "2026-01-15")

    def test_date_none(self):
        page = {"properties": {"D": {"type": "date", "date": None}}}
        self.assertEqual(get_date_property(page, "D"), "")

    def test_number(self):
        page = {"properties": {"S": {"type": "number", "number": 3}}}
        self.assertEqual(get_number_property(page, "S"), 3)

    def test_number_none(self):
        page = {"properties": {"S": {"type": "number", "number": None}}}
        self.assertEqual(get_number_property(page, "S"), 0)

    def test_checkbox_true(self):
        page = {"properties": {"O": {"type": "checkbox", "checkbox": True}}}
        self.assertTrue(get_checkbox_property(page, "O"))

    def test_checkbox_false(self):
        page = {"properties": {"O": {"type": "checkbox", "checkbox": False}}}
        self.assertFalse(get_checkbox_property(page, "O"))

    def test_email(self):
        page = {"properties": {"E": {"type": "email", "email": "a@b.com"}}}
        self.assertEqual(get_email_property(page, "E"), "a@b.com")

    def test_email_none(self):
        page = {"properties": {"E": {"type": "email", "email": None}}}
        self.assertEqual(get_email_property(page, "E"), "")

    def test_multi_select(self):
        page = {"properties": {"T": {"type": "multi_select", "multi_select": [{"name": "A"}, {"name": "B"}]}}}
        self.assertEqual(get_multi_select_property(page, "T"), ["A", "B"])

    def test_multi_select_empty(self):
        page = {"properties": {"T": {"type": "multi_select", "multi_select": []}}}
        self.assertEqual(get_multi_select_property(page, "T"), [])

    def test_status(self):
        page = {"properties": {"S": {"type": "status", "status": {"name": "En curso"}}}}
        self.assertEqual(get_status_property(page, "S"), "En curso")

    def test_status_none(self):
        page = {"properties": {"S": {"type": "status", "status": None}}}
        self.assertEqual(get_status_property(page, "S"), "")

    def test_people(self):
        page = {"properties": {"T": {"type": "people", "people": [{"name": "A"}, {"name": "B"}]}}}
        self.assertEqual(get_people_property(page, "T"), ["A", "B"])

    def test_people_first(self):
        page = {"properties": {"O": {"type": "people", "people": [{"name": "A"}, {"name": "B"}]}}}
        self.assertEqual(get_people_first(page, "O"), "A")

    def test_people_first_empty(self):
        page = {"properties": {"O": {"type": "people", "people": []}}}
        self.assertEqual(get_people_first(page, "O"), "")

    def test_rollup_text_title(self):
        page = {"properties": {"R": {"type": "rollup", "rollup": {
            "type": "array", "array": [{"type": "title", "title": [{"plain_text": "Proj"}]}]
        }}}}
        self.assertEqual(get_rollup_text(page, "R"), "Proj")

    def test_rollup_text_email(self):
        page = {"properties": {"R": {"type": "rollup", "rollup": {
            "type": "array", "array": [{"type": "email", "email": "x@y.com"}]
        }}}}
        self.assertEqual(get_rollup_text(page, "R"), "x@y.com")

    def test_rollup_empty(self):
        page = {"properties": {"R": {"type": "rollup", "rollup": {"type": "array", "array": []}}}}
        self.assertEqual(get_rollup_text(page, "R"), "")


class TestNotionPropertyBuilders(unittest.TestCase):
    def test_build_select(self):
        self.assertEqual(build_select("ES"), {"select": {"name": "ES"}})

    def test_build_number(self):
        self.assertEqual(build_number(3), {"number": 3})

    def test_build_date(self):
        self.assertEqual(build_date("2026-01-15"), {"date": {"start": "2026-01-15"}})

    def test_build_checkbox(self):
        self.assertEqual(build_checkbox(True), {"checkbox": True})

    def test_build_rich_text(self):
        self.assertEqual(build_rich_text("Hi"), {"rich_text": [{"text": {"content": "Hi"}}]})

    def test_build_email(self):
        self.assertEqual(build_email("a@b.com"), {"email": "a@b.com"})

    def test_build_status(self):
        self.assertEqual(build_status("X"), {"status": {"name": "X"}})


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 3: Compute Next Follow-Up
# ═══════════════════════════════════════════════════════════════════════════════
from compute_next_followup import (
    compute_next_followup_date, is_followup_due, days_overdue,
    is_within_business_hours,
)

class TestComputeNextFollowup(unittest.TestCase):
    def _today(self):
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    def test_stage0_overdue_returns_today(self):
        yesterday = (self._today() - timedelta(days=1)).strftime("%Y-%m-%d")
        result = compute_next_followup_date(0, due_date=yesterday)
        self.assertEqual(result, self._today())

    def test_stage0_future_returns_due_date(self):
        tomorrow = self._today() + timedelta(days=1)
        result = compute_next_followup_date(0, due_date=tomorrow.strftime("%Y-%m-%d"))
        self.assertEqual(result, tomorrow)

    def test_stage0_no_due_date_returns_today(self):
        result = compute_next_followup_date(0, due_date=None)
        self.assertEqual(result, self._today())

    def test_stage1_due_after_3_days(self):
        three_days_ago = (self._today() - timedelta(days=3)).strftime("%Y-%m-%d")
        result = compute_next_followup_date(1, last_followup_date=three_days_ago)
        self.assertEqual(result, self._today())

    def test_stage4_returns_none(self):
        result = compute_next_followup_date(4)
        self.assertIsNone(result)


class TestIsFollowupDue(unittest.TestCase):
    def _today(self):
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    def test_stage0_overdue_is_due(self):
        yesterday = (self._today() - timedelta(days=1)).strftime("%Y-%m-%d")
        is_due, next_stage, _ = is_followup_due(0, due_date=yesterday)
        self.assertTrue(is_due)
        self.assertEqual(next_stage, 1)

    def test_stage0_future_not_due(self):
        tomorrow = (self._today() + timedelta(days=1)).strftime("%Y-%m-%d")
        is_due, _, _ = is_followup_due(0, due_date=tomorrow)
        self.assertFalse(is_due)

    def test_stage1_not_yet_due(self):
        yesterday = (self._today() - timedelta(days=1)).strftime("%Y-%m-%d")
        is_due, _, _ = is_followup_due(1, last_followup_date=yesterday)
        self.assertFalse(is_due)

    def test_stage4_never_due(self):
        is_due, next_stage, next_date = is_followup_due(4)
        self.assertFalse(is_due)
        self.assertIsNone(next_stage)
        self.assertIsNone(next_date)


class TestDaysOverdue(unittest.TestCase):
    def _today(self):
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    def test_overdue_3_days(self):
        d = (self._today() - timedelta(days=3)).strftime("%Y-%m-%d")
        self.assertEqual(days_overdue(d), 3)

    def test_due_today(self):
        self.assertEqual(days_overdue(self._today().strftime("%Y-%m-%d")), 0)

    def test_future_is_zero(self):
        d = (self._today() + timedelta(days=1)).strftime("%Y-%m-%d")
        self.assertEqual(days_overdue(d), 0)

    def test_none_is_zero(self):
        self.assertEqual(days_overdue(None), 0)

    def test_empty_is_zero(self):
        self.assertEqual(days_overdue(""), 0)

    def test_datetime_input(self):
        d = self._today() - timedelta(days=5)
        self.assertEqual(days_overdue(d), 5)


class TestBusinessHours(unittest.TestCase):
    def test_unknown_country_no_crash(self):
        result = is_within_business_hours(country="Atlantis")
        self.assertIsInstance(result, bool)

    def test_none_country(self):
        result = is_within_business_hours(country=None)
        self.assertIsInstance(result, bool)

    def test_known_countries(self):
        for c in ["México", "Colombia", "Chile", "Argentina", "España"]:
            r = is_within_business_hours(country=c)
            self.assertIsInstance(r, bool)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 4: Claude Client (Mocked API)
# ═══════════════════════════════════════════════════════════════════════════════
from claude_client import (
    _build_system_prompt, _parse_json_response,
    generate_followup_email, classify_response, extract_context,
    TONE_MAP, LANGUAGE_INSTRUCTIONS,
)

class TestBuildSystemPrompt(unittest.TestCase):
    def test_contains_company(self):
        self.assertIn("LeafLATAM", _build_system_prompt("LeafLATAM", "ES", 1))

    def test_stage1_friendly(self):
        p = _build_system_prompt("X", "ES", 1)
        self.assertTrue("amable" in p or "gentil" in p or "friendly" in p.lower())

    def test_stage4_escalation(self):
        p = _build_system_prompt("X", "ES", 4)
        self.assertTrue("escal" in p.lower() or "formal" in p.lower())

    def test_lang_es(self):
        self.assertIn("español", _build_system_prompt("X", "ES", 1))

    def test_lang_en(self):
        self.assertIn("English", _build_system_prompt("X", "EN", 1))

    def test_lang_pt(self):
        self.assertIn("português", _build_system_prompt("X", "PT", 1))

    def test_style_examples_included(self):
        examples = ["Example 1", "Example 2"]
        p = _build_system_prompt("X", "ES", 1, style_examples=examples)
        self.assertIn("Example 1", p)
        self.assertIn("Example 2", p)

    def test_all_stages_tones(self):
        for stage in [1, 2, 3, 4]:
            for lang in ["ES", "EN", "PT"]:
                self.assertIn(lang, TONE_MAP[stage])


class TestParseJsonResponse(unittest.TestCase):
    def test_plain_json(self):
        r = _parse_json_response('{"subject": "T", "body_html": "<p>H</p>"}')
        self.assertEqual(r["subject"], "T")

    def test_markdown_wrapped(self):
        r = _parse_json_response('```json\n{"subject": "T", "body_html": "<p>H</p>"}\n```')
        self.assertEqual(r["subject"], "T")

    def test_invalid_json(self):
        self.assertIsNone(_parse_json_response("not json"))

    def test_empty(self):
        self.assertIsNone(_parse_json_response(""))

    def test_none(self):
        self.assertIsNone(_parse_json_response(None))


class TestGenerateFollowupEmail(unittest.TestCase):
    @patch("claude_client._call_claude_with_retry")
    def test_success(self, mock_call):
        mock_call.return_value = '{"subject": "Recordatorio", "body_html": "<p>Hola</p>"}'
        result = generate_followup_email({"project_name": "X"}, "ES", 1, "Co")
        self.assertIsNotNone(result)
        self.assertIn("subject", result)
        self.assertIn("body_html", result)

    @patch("claude_client._call_claude_with_retry", return_value=None)
    def test_api_failure(self, mock_call):
        result = generate_followup_email({"project_name": "X"}, "ES", 1)
        self.assertIsNone(result)

    @patch("claude_client._call_claude_with_retry")
    def test_missing_keys(self, mock_call):
        mock_call.return_value = '{"subject": "Only subject"}'
        result = generate_followup_email({"project_name": "X"}, "ES", 1)
        self.assertIsNone(result)


class TestClassifyResponse(unittest.TestCase):
    @patch("claude_client._call_claude_with_retry")
    def test_classify_received(self, mock_call):
        mock_call.return_value = '{"classification": "received", "confidence": 0.95, "summary": "Got files"}'
        r = classify_response("Here are the files", "Updated plans")
        self.assertEqual(r["classification"], "received")

    @patch("claude_client._call_claude_with_retry", return_value=None)
    def test_classify_fail(self, mock_call):
        self.assertIsNone(classify_response("text", "item"))


class TestExtractContext(unittest.TestCase):
    @patch("claude_client._call_claude_with_retry")
    def test_extract_success(self, mock_call):
        mock_call.return_value = '{"project_name": "Proj", "confidence": 0.9}'
        r = extract_context("Please send to client: the updated plans for Proj")
        self.assertEqual(r["project_name"], "Proj")

    @patch("claude_client._call_claude_with_retry", return_value=None)
    def test_extract_fail(self, mock_call):
        self.assertIsNone(extract_context("some text"))


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 5: Send Follow-Up
# ═══════════════════════════════════════════════════════════════════════════════
from send_followup import _is_valid_email

class TestEmailValidation(unittest.TestCase):
    def test_valid(self):
        self.assertTrue(_is_valid_email("test@example.com"))

    def test_valid_subdomain(self):
        self.assertTrue(_is_valid_email("a@b.co.uk"))

    def test_invalid_no_at(self):
        self.assertFalse(_is_valid_email("noemail"))

    def test_invalid_no_domain(self):
        self.assertFalse(_is_valid_email("test@"))

    def test_empty(self):
        self.assertFalse(_is_valid_email(""))

    def test_none(self):
        self.assertFalse(_is_valid_email(None))

    def test_spaces(self):
        self.assertFalse(_is_valid_email("  "))


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 6: Style Store
# ═══════════════════════════════════════════════════════════════════════════════
from style_store import load_style_examples, save_style_example, load_metrics, save_metrics, init_style_data

class TestStyleStore(unittest.TestCase):
    def test_load_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("style_store.STYLE_DATA_DIR", Path(tmp)):
                result = load_style_examples()
                self.assertEqual(result, [])

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("style_store.STYLE_DATA_DIR", Path(tmp)):
                save_style_example("Email body", "ES", project_name="P1", stage=1)
                result = load_style_examples(language="ES")
                self.assertEqual(len(result), 1)
                self.assertIn("Email body", result[0])

    def test_max_examples(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("style_store.STYLE_DATA_DIR", Path(tmp)):
                for i in range(10):
                    save_style_example(f"Ex {i}", "ES", stage=1)
                result = load_style_examples(language="ES", max_examples=3)
                self.assertEqual(len(result), 3)


class TestMetrics(unittest.TestCase):
    def test_load_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("style_store.STYLE_DATA_DIR", Path(tmp)):
                m = load_metrics()
                self.assertEqual(m["total_drafts"], 0)
                self.assertEqual(m["approval_rate"], 0.0)

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("style_store.STYLE_DATA_DIR", Path(tmp)):
                save_metrics({"total_drafts": 10, "approval_rate": 0.8})
                m = load_metrics()
                self.assertEqual(m["total_drafts"], 10)

    def test_init_creates_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("style_store.STYLE_DATA_DIR", Path(tmp)):
                init_style_data()
                self.assertTrue((Path(tmp) / "drafts_log.jsonl").exists())
                self.assertTrue((Path(tmp) / "sent_log.jsonl").exists())
                self.assertTrue((Path(tmp) / "style_examples.json").exists())
                self.assertTrue((Path(tmp) / "learning_metrics.json").exists())

    def test_init_no_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            mp = Path(tmp) / "learning_metrics.json"
            mp.write_text('{"total_drafts": 42}')
            with patch("style_store.STYLE_DATA_DIR", Path(tmp)):
                init_style_data()
                data = json.loads(mp.read_text())
                self.assertEqual(data["total_drafts"], 42)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 7: Learning Engine
# ═══════════════════════════════════════════════════════════════════════════════
from learning_engine import _similarity, _strip_html, get_mode_recommendation

class TestSimilarity(unittest.TestCase):
    def test_identical(self):
        self.assertEqual(_similarity("hello", "hello"), 1.0)

    def test_different(self):
        self.assertLess(_similarity("abc", "xyz"), 0.5)

    def test_similar(self):
        s = _similarity("please send the plans", "please send the updated plans")
        self.assertGreater(s, 0.7)

    def test_empty(self):
        self.assertEqual(_similarity("", "hello"), 0.0)
        self.assertEqual(_similarity("", ""), 0.0)

    def test_none(self):
        self.assertEqual(_similarity(None, "hello"), 0.0)


class TestStripHtml(unittest.TestCase):
    def test_strip_tags(self):
        self.assertEqual(_strip_html("<p>Hello <b>W</b></p>"), "Hello W")

    def test_empty(self):
        self.assertEqual(_strip_html(""), "")


class TestModeRecommendation(unittest.TestCase):
    @patch("learning_engine.load_metrics")
    def test_insufficient_data(self, mock_m):
        mock_m.return_value = {"total_drafts": 5, "approval_rate": 0.9}
        r = get_mode_recommendation()
        self.assertEqual(r["recommendation"], "DRAFT")

    @patch("learning_engine.load_metrics")
    def test_high_approval_auto(self, mock_m):
        mock_m.return_value = {"total_drafts": 30, "approval_rate": 0.96}
        r = get_mode_recommendation()
        self.assertEqual(r["recommendation"], "AUTO")

    @patch("learning_engine.load_metrics")
    def test_medium_approval_semi(self, mock_m):
        mock_m.return_value = {"total_drafts": 25, "approval_rate": 0.85}
        r = get_mode_recommendation()
        self.assertEqual(r["recommendation"], "SEMI_AUTO")

    @patch("learning_engine.load_metrics")
    def test_low_approval_draft(self, mock_m):
        mock_m.return_value = {"total_drafts": 25, "approval_rate": 0.5}
        r = get_mode_recommendation()
        self.assertEqual(r["recommendation"], "DRAFT")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 8: Fallback Templates
# ═══════════════════════════════════════════════════════════════════════════════

class TestFallbackTemplates(unittest.TestCase):
    """Verify all required HTML templates exist."""
    TEMPLATES_DIR = TOOLS_DIR / "templates"

    def test_all_templates_exist(self):
        expected = [
            "reminder_es.html", "reminder_en.html", "reminder_pt.html",
            "second_notice_es.html", "second_notice_en.html", "second_notice_pt.html",
            "urgent_es.html", "urgent_en.html", "urgent_pt.html",
            "escalation_es.html", "escalation_en.html", "escalation_pt.html",
        ]
        for t in expected:
            self.assertTrue(
                (self.TEMPLATES_DIR / t).exists(),
                f"Missing template: {t}"
            )

    def test_templates_have_placeholders(self):
        """Templates should contain placeholder variables."""
        for tmpl in self.TEMPLATES_DIR.glob("*.html"):
            content = tmpl.read_text(encoding="utf-8")
            self.assertGreater(len(content), 50, f"Template too small: {tmpl.name}")
            # Should have at least one placeholder like {{variable}}
            self.assertTrue(
                "{{" in content or "{%" in content or "{" in content,
                f"No placeholders in {tmpl.name}"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 9: Validate Schema Module
# ═══════════════════════════════════════════════════════════════════════════════
from validate_schema import REQUIRED_PROPERTIES_MAIN, REQUIRED_PROPERTIES_TEAM

class TestSchemaDefinitions(unittest.TestCase):
    def test_main_db_has_required_fields(self):
        required = ["Nombre", "Status", "Manual Override", "Follow-Up Stage",
                     "Fecha límite de Client Success", "Client Language"]
        for f in required:
            self.assertIn(f, REQUIRED_PROPERTIES_MAIN, f"Missing required field: {f}")

    def test_team_db_has_required_fields(self):
        for f in ["Name", "Email", "Role", "Languages", "Active"]:
            self.assertIn(f, REQUIRED_PROPERTIES_TEAM, f"Missing required field: {f}")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 10: Project Structure & Files
# ═══════════════════════════════════════════════════════════════════════════════

class TestProjectStructure(unittest.TestCase):
    """Verify all required files and directories exist."""

    def test_env_example_exists(self):
        self.assertTrue((ROOT / ".env.example").exists())

    def test_readme_exists(self):
        self.assertTrue((ROOT / "README.md").exists())

    def test_dockerfile_exists(self):
        self.assertTrue((ROOT / "Dockerfile").exists())

    def test_docker_compose_exists(self):
        self.assertTrue((ROOT / "docker-compose.yml").exists())

    def test_requirements_txt_exists(self):
        self.assertTrue((ROOT / "requirements.txt").exists())

    def test_tools_init_exists(self):
        self.assertTrue((TOOLS_DIR / "__init__.py").exists())

    def test_validate_schema_exists(self):
        self.assertTrue((TOOLS_DIR / "validate_schema.py").exists())

    def test_all_python_files_parse(self):
        """Every .py file should be valid Python (no syntax errors)."""
        import py_compile
        errors = []
        for py_file in TOOLS_DIR.glob("*.py"):
            try:
                py_compile.compile(str(py_file), doraise=True)
            except py_compile.PyCompileError as e:
                errors.append(f"{py_file.name}: {e}")
        self.assertEqual(errors, [], f"Syntax errors:\n" + "\n".join(errors))

    def test_workflows_dir_exists(self):
        self.assertTrue((ROOT / "workflows").is_dir())

    def test_workflow_files_exist(self):
        wf = ROOT / "workflows"
        expected = [
            "setup_and_configuration.md",
            "flow1_outbound_followup.md",
            "flow2_inbound_team.md",
            "flow3_inbound_client.md",
        ]
        for f in expected:
            self.assertTrue((wf / f).exists(), f"Missing workflow: {f}")


# ═══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Custom test runner with colored output and summary
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    test_classes = [
        TestConfig,
        TestNotionPropertyExtractors,
        TestNotionPropertyBuilders,
        TestComputeNextFollowup,
        TestIsFollowupDue,
        TestDaysOverdue,
        TestBusinessHours,
        TestBuildSystemPrompt,
        TestParseJsonResponse,
        TestGenerateFollowupEmail,
        TestClassifyResponse,
        TestExtractContext,
        TestEmailValidation,
        TestStyleStore,
        TestMetrics,
        TestSimilarity,
        TestStripHtml,
        TestModeRecommendation,
        TestFallbackTemplates,
        TestSchemaDefinitions,
        TestProjectStructure,
    ]

    for tc in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(tc))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Summary
    print("\n" + "=" * 70)
    total = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped)
    passed = total - failures - errors - skipped

    print(f"  TOTAL: {total}  |  PASSED: {passed}  |  FAILED: {failures}  |  ERRORS: {errors}  |  SKIPPED: {skipped}")
    if failures == 0 and errors == 0:
        print("  ✅ ALL TESTS PASSED")
    else:
        print("  ❌ SOME TESTS FAILED")
        if result.failures:
            print("\n  FAILURES:")
            for test, traceback in result.failures:
                print(f"    - {test}: {traceback.splitlines()[-1]}")
        if result.errors:
            print("\n  ERRORS:")
            for test, traceback in result.errors:
                tb_lines = traceback.strip().splitlines()
                print(f"    - {test}: {tb_lines[-1]}")
    print("=" * 70)

    sys.exit(0 if (failures == 0 and errors == 0) else 1)
