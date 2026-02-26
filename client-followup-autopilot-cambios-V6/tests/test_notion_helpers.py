"""
Tests for notion_client.py — property helpers and builders.
Tests the pure extraction logic without making actual API calls.
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")
os.environ.setdefault("NOTION_API_KEY", "secret_test")
os.environ.setdefault("NOTION_DATABASE_ID", "test-db-id")

from notion_client import (
    get_text_property,
    get_select_property,
    get_date_property,
    get_number_property,
    get_checkbox_property,
    get_email_property,
    get_multi_select_property,
    get_status_property,
    get_people_property,
    get_people_first,
    get_rollup_text,
    build_select,
    build_number,
    build_date,
    build_checkbox,
    build_rich_text,
    build_email,
    build_status,
)


class TestPropertyExtractors:
    """Tests for Notion property extraction helpers."""

    def test_get_text_from_title(self):
        page = {"properties": {"Name": {"type": "title", "title": [{"plain_text": "Test Project"}]}}}
        assert get_text_property(page, "Name") == "Test Project"

    def test_get_text_from_rich_text(self):
        page = {"properties": {"Note": {"type": "rich_text", "rich_text": [{"plain_text": "Hello"}]}}}
        assert get_text_property(page, "Note") == "Hello"

    def test_get_text_empty_title(self):
        page = {"properties": {"Name": {"type": "title", "title": []}}}
        assert get_text_property(page, "Name") == ""

    def test_get_text_missing_property(self):
        page = {"properties": {}}
        assert get_text_property(page, "Nonexistent") == ""

    def test_get_select(self):
        page = {"properties": {"Lang": {"type": "select", "select": {"name": "ES"}}}}
        assert get_select_property(page, "Lang") == "ES"

    def test_get_select_none(self):
        page = {"properties": {"Lang": {"type": "select", "select": None}}}
        assert get_select_property(page, "Lang") == ""

    def test_get_date(self):
        page = {"properties": {"Due": {"type": "date", "date": {"start": "2026-01-15"}}}}
        assert get_date_property(page, "Due") == "2026-01-15"

    def test_get_date_none(self):
        page = {"properties": {"Due": {"type": "date", "date": None}}}
        assert get_date_property(page, "Due") == ""

    def test_get_number(self):
        page = {"properties": {"Stage": {"type": "number", "number": 3}}}
        assert get_number_property(page, "Stage") == 3

    def test_get_number_none_returns_zero(self):
        page = {"properties": {"Stage": {"type": "number", "number": None}}}
        assert get_number_property(page, "Stage") == 0

    def test_get_checkbox_true(self):
        page = {"properties": {"Override": {"type": "checkbox", "checkbox": True}}}
        assert get_checkbox_property(page, "Override") is True

    def test_get_checkbox_false(self):
        page = {"properties": {"Override": {"type": "checkbox", "checkbox": False}}}
        assert get_checkbox_property(page, "Override") is False

    def test_get_email(self):
        page = {"properties": {"Email": {"type": "email", "email": "test@example.com"}}}
        assert get_email_property(page, "Email") == "test@example.com"

    def test_get_email_none(self):
        page = {"properties": {"Email": {"type": "email", "email": None}}}
        assert get_email_property(page, "Email") == ""

    def test_get_multi_select(self):
        page = {"properties": {"Tags": {"type": "multi_select", "multi_select": [{"name": "ES"}, {"name": "EN"}]}}}
        assert get_multi_select_property(page, "Tags") == ["ES", "EN"]

    def test_get_multi_select_empty(self):
        page = {"properties": {"Tags": {"type": "multi_select", "multi_select": []}}}
        assert get_multi_select_property(page, "Tags") == []

    def test_get_status(self):
        page = {"properties": {"Status": {"type": "status", "status": {"name": "En curso"}}}}
        assert get_status_property(page, "Status") == "En curso"

    def test_get_status_none(self):
        page = {"properties": {"Status": {"type": "status", "status": None}}}
        assert get_status_property(page, "Status") == ""

    def test_get_people(self):
        page = {"properties": {"Team": {"type": "people", "people": [{"name": "Alice"}, {"name": "Bob"}]}}}
        assert get_people_property(page, "Team") == ["Alice", "Bob"]

    def test_get_people_first(self):
        page = {"properties": {"Owner": {"type": "people", "people": [{"name": "Alice"}, {"name": "Bob"}]}}}
        assert get_people_first(page, "Owner") == "Alice"

    def test_get_people_first_empty(self):
        page = {"properties": {"Owner": {"type": "people", "people": []}}}
        assert get_people_first(page, "Owner") == ""

    def test_get_rollup_text_title(self):
        page = {"properties": {"Proj": {"type": "rollup", "rollup": {
            "type": "array", "array": [{"type": "title", "title": [{"plain_text": "My Project"}]}]
        }}}}
        assert get_rollup_text(page, "Proj") == "My Project"

    def test_get_rollup_text_email(self):
        page = {"properties": {"ClientEmail": {"type": "rollup", "rollup": {
            "type": "array", "array": [{"type": "email", "email": "client@test.com"}]
        }}}}
        assert get_rollup_text(page, "ClientEmail") == "client@test.com"

    def test_get_rollup_empty_array(self):
        page = {"properties": {"X": {"type": "rollup", "rollup": {"type": "array", "array": []}}}}
        assert get_rollup_text(page, "X") == ""


class TestPropertyBuilders:
    """Tests for Notion property value builders."""

    def test_build_select(self):
        assert build_select("ES") == {"select": {"name": "ES"}}

    def test_build_number(self):
        assert build_number(3) == {"number": 3}

    def test_build_date(self):
        assert build_date("2026-01-15") == {"date": {"start": "2026-01-15"}}

    def test_build_checkbox(self):
        assert build_checkbox(True) == {"checkbox": True}
        assert build_checkbox(False) == {"checkbox": False}

    def test_build_rich_text(self):
        result = build_rich_text("Hello")
        assert result == {"rich_text": [{"text": {"content": "Hello"}}]}

    def test_build_email(self):
        assert build_email("a@b.com") == {"email": "a@b.com"}

    def test_build_status(self):
        assert build_status("En curso") == {"status": {"name": "En curso"}}
