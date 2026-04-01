"""
Tests for claude_client.py — prompt building and response parsing.
Tests the internal logic without making actual API calls.
"""

import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test-key")

from claude_client import (
    _build_system_prompt,
    _parse_json_response,
    TONE_MAP,
    LANGUAGE_INSTRUCTIONS,
    generate_followup_email,
    classify_response,
    extract_context,
)


class TestBuildSystemPrompt:
    """Tests for system prompt construction."""

    def test_contains_company_name(self):
        prompt = _build_system_prompt("LeafLATAM", "ES", 1)
        assert "LeafLATAM" in prompt

    def test_stage_1_friendly_tone(self):
        prompt = _build_system_prompt("Co", "ES", 1)
        assert "amable" in prompt or "gentil" in prompt

    def test_stage_4_escalation_tone(self):
        prompt = _build_system_prompt("Co", "ES", 4)
        assert "escalamiento" in prompt or "formal" in prompt

    def test_language_instruction_es(self):
        prompt = _build_system_prompt("Co", "ES", 1)
        assert "español" in prompt

    def test_language_instruction_en(self):
        prompt = _build_system_prompt("Co", "EN", 1)
        assert "English" in prompt

    def test_language_instruction_pt(self):
        prompt = _build_system_prompt("Co", "PT", 1)
        assert "português" in prompt

    def test_style_examples_included(self):
        examples = ["Example email 1", "Example email 2"]
        prompt = _build_system_prompt("Co", "ES", 1, style_examples=examples)
        assert "Example email 1" in prompt
        assert "Example email 2" in prompt

    def test_all_stages_have_tones(self):
        for stage in [1, 2, 3, 4]:
            for lang in ["ES", "EN", "PT"]:
                assert lang in TONE_MAP[stage]


class TestParseJsonResponse:
    """Tests for JSON response parsing."""

    def test_plain_json(self):
        result = _parse_json_response('{"subject": "Test", "body_html": "<p>Hi</p>"}')
        assert result["subject"] == "Test"

    def test_markdown_wrapped_json(self):
        text = '```json\n{"subject": "Test", "body_html": "<p>Hi</p>"}\n```'
        result = _parse_json_response(text)
        assert result["subject"] == "Test"

    def test_invalid_json_returns_none(self):
        result = _parse_json_response("not json at all")
        assert result is None

    def test_empty_returns_none(self):
        result = _parse_json_response("")
        assert result is None

    def test_none_returns_none(self):
        result = _parse_json_response(None)
        assert result is None


class TestGenerateFollowupEmail:
    """Tests for email generation with mocked Claude API."""

    def test_successful_generation(self):
        mock_response = '{"subject": "Recordatorio: Proyecto X", "body_html": "<p>Estimado cliente</p>"}'
        with patch("claude_client._call_claude_with_retry", return_value=mock_response):
            result = generate_followup_email(
                context={"project_name": "Proyecto X", "client_name": "Juan"},
                language="ES",
                stage=1,
                company_name="TestCo",
            )
            assert result is not None
            assert "subject" in result
            assert "body_html" in result

    def test_unsupported_language_defaults_to_en(self):
        mock_response = '{"subject": "Test", "body_html": "<p>Hello</p>"}'
        with patch("claude_client._call_claude_with_retry", return_value=mock_response):
            result = generate_followup_email(
                context={"project_name": "X"},
                language="FR",  # unsupported
                stage=1,
                company_name="TestCo",
            )
            # Should not fail, just default to EN
            assert result is not None

    def test_api_failure_returns_none(self):
        with patch("claude_client._call_claude_with_retry", return_value=None):
            result = generate_followup_email(
                context={"project_name": "X"},
                language="ES",
                stage=1,
            )
            assert result is None

    def test_missing_keys_returns_none(self):
        mock_response = '{"subject": "Only subject, no body"}'
        with patch("claude_client._call_claude_with_retry", return_value=mock_response):
            result = generate_followup_email(
                context={"project_name": "X"},
                language="ES",
                stage=1,
            )
            assert result is None


class TestClassifyResponse:
    """Tests for response classification with mocked Claude API."""

    def test_classify_received(self):
        mock_response = '{"classification": "received", "confidence": 0.95, "summary": "Client sent files"}'
        with patch("claude_client._call_claude_with_retry", return_value=mock_response):
            result = classify_response("Here are the plans you requested", "Updated plans")
            assert result["classification"] == "received"
            assert result["confidence"] == 0.95

    def test_classify_question(self):
        mock_response = '{"classification": "question", "confidence": 0.8, "summary": "Client asking about deadline"}'
        with patch("claude_client._call_claude_with_retry", return_value=mock_response):
            result = classify_response("When is the deadline?", "Updated plans")
            assert result["classification"] == "question"

    def test_classify_api_failure(self):
        with patch("claude_client._call_claude_with_retry", return_value=None):
            result = classify_response("Some text", "Some item")
            assert result is None
