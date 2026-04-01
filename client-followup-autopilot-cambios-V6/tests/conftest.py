"""
Shared test fixtures and configuration for Client Follow-Up Autopilot tests.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add tools directory to path so we can import modules
TOOLS_DIR = Path(__file__).parent.parent / "tools"
sys.path.insert(0, str(TOOLS_DIR))

# Set required env vars BEFORE importing any tool modules
os.environ.setdefault("SYSTEM_MODE", "DRAFT")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test-fake-key")
os.environ.setdefault("NOTION_API_KEY", "secret_test_fake_key")
os.environ.setdefault("NOTION_DATABASE_ID", "test-db-id-000000000000")
os.environ.setdefault("NOTION_TEAM_DATABASE_ID", "test-team-db-id-000000")
os.environ.setdefault("SLACK_BOT_TOKEN", "xoxb-test-fake-token")
os.environ.setdefault("SLACK_REVIEW_CHANNEL", "C0TEST0001")
os.environ.setdefault("SLACK_DEFAULT_CHANNEL", "C0TEST0002")
os.environ.setdefault("GMAIL_SENDER_EMAIL", "test@example.com")
os.environ.setdefault("GMAIL_DEFAULT_SENDER_EMAIL", "test@example.com")
os.environ.setdefault("COMPANY_NAME", "TestCompany")
os.environ.setdefault("CS_TEAM_EMAIL", "cs@example.com")
os.environ.setdefault("GMAIL_AUTH_MODE", "oauth2")


# ─── Notion Page Fixtures ─────────────────────────────────────────────────────

def _make_notion_page(
    page_id="page-001",
    nombre="Test Deliverable",
    status="Sin empezar",
    manual_override=False,
    follow_up_stage=0,
    due_date=None,
    last_followup=None,
    client_language="ES",
    gmail_thread_id="",
    entregable_rel_id=None,
):
    """Build a mock Notion page object with realistic structure."""
    if due_date is None:
        due_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

    page = {
        "id": page_id,
        "properties": {
            "Nombre": {
                "type": "title",
                "title": [{"plain_text": nombre}] if nombre else [],
            },
            "Status": {
                "type": "status",
                "status": {"name": status} if status else None,
            },
            "Manual Override": {
                "type": "checkbox",
                "checkbox": manual_override,
            },
            "Follow-Up Stage": {
                "type": "number",
                "number": follow_up_stage,
            },
            "Fecha límite de Client Success": {
                "type": "date",
                "date": {"start": due_date} if due_date else None,
            },
            "Last Follow-Up Date": {
                "type": "date",
                "date": {"start": last_followup} if last_followup else None,
            },
            "Client Language": {
                "type": "select",
                "select": {"name": client_language} if client_language else None,
            },
            "Gmail Thread ID": {
                "type": "rich_text",
                "rich_text": [{"plain_text": gmail_thread_id}] if gmail_thread_id else [],
            },
            "Follow-Up Log": {
                "type": "rich_text",
                "rich_text": [],
            },
            "Owner - Client Success": {
                "type": "people",
                "people": [{"name": "Test CS Member"}],
            },
            "Entregable Proyecto": {
                "type": "relation",
                "relation": [{"id": entregable_rel_id}] if entregable_rel_id else [],
            },
            "Comentarios Client Success": {
                "type": "rich_text",
                "rich_text": [],
            },
            "Detalle Falta info / Pausado [Proyectos]": {
                "type": "rollup",
                "rollup": {"type": "array", "array": []},
            },
            "Fecha Objetivo [Proyectos]": {
                "type": "rollup",
                "rollup": {"type": "array", "array": []},
            },
            "Responsable [Proyectos]": {
                "type": "rollup",
                "rollup": {"type": "array", "array": []},
            },
            "Status [Proyectos]": {
                "type": "rollup",
                "rollup": {"type": "array", "array": []},
            },
        },
    }
    return page


@pytest.fixture
def sample_notion_page():
    """A default actionable Notion page (overdue, stage 0)."""
    return _make_notion_page()


@pytest.fixture
def sample_notion_page_with_override():
    """A Notion page with Manual Override checked."""
    return _make_notion_page(manual_override=True)


@pytest.fixture
def sample_notion_page_completed():
    """A Notion page already at stage 4."""
    return _make_notion_page(follow_up_stage=4, status="Listo")


@pytest.fixture
def sample_actionable_item():
    """A fully resolved actionable item dict (output of check_pending_items)."""
    return {
        "page_id": "page-001",
        "project_name": "Proyecto Solar LATAM",
        "client_name": "Juan Pérez",
        "client_email": "juan@clientcorp.com",
        "senior_contact_email": "director@clientcorp.com",
        "client_country": "México",
        "client_language": "ES",
        "pending_item": "Planos actualizados de instalación",
        "due_date": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
        "days_overdue": 3,
        "impact_description": "Falta enviar planos actualizados para completar la auditoría",
        "follow_up_stage": 0,
        "next_stage": 1,
        "last_followup_date": "",
        "manual_override": False,
        "delivery_team_email": "",
        "delivery_team_slack_channel": "",
        "gmail_thread_id": "",
        "status": "Sin empezar",
        "client_success": "Test CS Member",
        "analista": "Test Analyst",
        "cs_comments": "",
        "project_status": "",
    }


@pytest.fixture
def sample_gmail_message():
    """A mock Gmail message from a client."""
    return {
        "id": "msg-001",
        "threadId": "thread-001",
        "snippet": "Adjunto los planos actualizados que solicitaron.",
        "from": "juan@clientcorp.com",
        "to": "test@example.com",
        "subject": "Re: Planos actualizados — Proyecto Solar LATAM",
        "date": "Thu, 19 Feb 2026 10:00:00 -0600",
        "body": "Buenos días, adjunto los planos actualizados que solicitaron para el proyecto.",
        "labelIds": ["INBOX", "UNREAD"],
    }


@pytest.fixture
def sample_team_members():
    """Mock team members list."""
    return [
        {"name": "Test CS Member", "email": "cs1@example.com", "role": "cs", "languages": ["ES", "EN"]},
        {"name": "Test Analyst", "email": "analyst@example.com", "role": "member", "languages": ["ES"]},
        {"name": "Admin User", "email": "admin@example.com", "role": "admin", "languages": ["ES", "EN", "PT"]},
    ]
