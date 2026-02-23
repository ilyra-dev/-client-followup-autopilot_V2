"""
Tests for check_pending_items.py — Notion query and item filtering.
All Notion API calls are mocked.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")
os.environ.setdefault("NOTION_API_KEY", "secret_test")
os.environ.setdefault("NOTION_DATABASE_ID", "test-db-id")

from check_pending_items import get_actionable_items, ACTIVE_STATUSES


def _make_page(
    page_id="page-001",
    nombre="Test Item",
    status="Sin empezar",
    manual_override=False,
    stage=0,
    due_date=None,
    client_email="client@test.com",
    language="ES",
):
    if due_date is None:
        due_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

    return {
        "id": page_id,
        "properties": {
            "Nombre": {"type": "title", "title": [{"plain_text": nombre}]},
            "Status": {"type": "status", "status": {"name": status}},
            "Manual Override": {"type": "checkbox", "checkbox": manual_override},
            "Follow-Up Stage": {"type": "number", "number": stage},
            "Fecha límite de Client Success": {"type": "date", "date": {"start": due_date} if due_date else None},
            "Last Follow-Up Date": {"type": "date", "date": None},
            "Client Language": {"type": "select", "select": {"name": language}},
            "Gmail Thread ID": {"type": "rich_text", "rich_text": []},
            "Follow-Up Log": {"type": "rich_text", "rich_text": []},
            "Owner - Client Success": {"type": "people", "people": [{"name": "CS Member"}]},
            "Entregable Proyecto": {"type": "relation", "relation": [{"id": "task-001"}]},
            "Comentarios Client Success": {"type": "rich_text", "rich_text": []},
            "Detalle Falta info / Pausado [Proyectos]": {"type": "rollup", "rollup": {"type": "array", "array": []}},
            "Fecha Objetivo [Proyectos]": {"type": "rollup", "rollup": {"type": "array", "array": []}},
            "Responsable [Proyectos]": {"type": "rollup", "rollup": {"type": "array", "array": []}},
            "Status [Proyectos]": {"type": "rollup", "rollup": {"type": "array", "array": []}},
        },
    }


class TestGetActionableItems:
    """Tests for get_actionable_items filtering logic."""

    @patch("check_pending_items.notion_client")
    def test_returns_overdue_items(self, mock_nc):
        """Overdue items with active status should be returned."""
        mock_nc.query_database.return_value = [_make_page()]
        mock_nc.get_status_property.return_value = "Sin empezar"
        mock_nc.get_checkbox_property.return_value = False
        mock_nc.get_number_property.return_value = 0
        mock_nc.get_date_property.return_value = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        mock_nc.get_rollup_date.return_value = ""
        mock_nc.resolve_client_email.return_value = "client@test.com"
        mock_nc.resolve_project_name.return_value = "Test Project"
        mock_nc.resolve_client_name.return_value = "Test Client"
        mock_nc.resolve_senior_contact_email.return_value = ""
        mock_nc.resolve_client_country.return_value = "México"
        mock_nc.get_select_property.return_value = "ES"
        mock_nc.get_text_property.return_value = "Test Item"
        mock_nc.get_rollup_text.return_value = ""
        mock_nc.get_people_first.return_value = "CS Member"
        mock_nc.get_rollup_people_first.return_value = ""
        mock_nc.get_rollup_status.return_value = ""

        items = get_actionable_items()
        assert len(items) == 1
        assert items[0]["project_name"] == "Test Project"

    @patch("check_pending_items.notion_client")
    def test_skips_manual_override(self, mock_nc):
        """Items with Manual Override should be skipped."""
        mock_nc.query_database.return_value = [_make_page(manual_override=True)]
        mock_nc.get_status_property.return_value = "Sin empezar"
        mock_nc.get_checkbox_property.return_value = True  # Override active

        items = get_actionable_items()
        assert len(items) == 0

    @patch("check_pending_items.notion_client")
    def test_skips_completed_status(self, mock_nc):
        """Items with non-active status (e.g., Listo) should be skipped."""
        mock_nc.query_database.return_value = [_make_page(status="Listo")]
        mock_nc.get_status_property.return_value = "Listo"
        mock_nc.get_checkbox_property.return_value = False

        items = get_actionable_items()
        assert len(items) == 0

    @patch("check_pending_items.notion_client")
    def test_skips_stage_4(self, mock_nc):
        """Items already at stage 4 should be skipped."""
        mock_nc.query_database.return_value = [_make_page(stage=4)]
        mock_nc.get_status_property.return_value = "En curso"
        mock_nc.get_checkbox_property.return_value = False
        mock_nc.get_number_property.return_value = 4

        items = get_actionable_items()
        assert len(items) == 0

    @patch("check_pending_items.notion_client")
    def test_skips_no_client_email(self, mock_nc):
        """Items without client email should be skipped (with warning)."""
        mock_nc.query_database.return_value = [_make_page()]
        mock_nc.get_status_property.return_value = "Sin empezar"
        mock_nc.get_checkbox_property.return_value = False
        mock_nc.get_number_property.return_value = 0
        mock_nc.get_date_property.return_value = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        mock_nc.get_rollup_date.return_value = ""
        mock_nc.resolve_client_email.return_value = ""  # No email!
        mock_nc.get_text_property.return_value = "Test Item"

        items = get_actionable_items()
        assert len(items) == 0

    def test_active_statuses_defined(self):
        """ACTIVE_STATUSES should contain the expected values."""
        assert "Sin empezar" in ACTIVE_STATUSES
        assert "En curso" in ACTIVE_STATUSES
