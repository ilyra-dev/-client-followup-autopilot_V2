"""
Query Notion for pending items that need follow-up action.

Reads from "Pendientes Client Success" DB and resolves client data
via relation chain to Pendientes Proyectos → Proyectos.

Filters for items where:
  - Status is "Sin empezar" or "En curso"
  - Manual Override is NOT checked
  - Follow-Up Stage < 4 (not all stages completed)
  - Follow-up is due based on schedule

Returns structured data ready for send_followup.py
"""

import json
import logging
import sys
from datetime import datetime, timezone

import notion_client
from compute_next_followup import is_followup_due, days_overdue

logger = logging.getLogger(__name__)


# Notion status values in Pendientes CS that mean "needs follow-up"
ACTIVE_STATUSES = ["Sin empezar", "En curso", "En proceso", "Falta info"]

# Project statuses (from Status [Proyectos] rollup) that trigger follow-ups
ACTIVE_PROJECT_STATUSES = ["En proceso", "Falta info", "Falta Info"]


def get_actionable_items():
    """
    Query Notion for items that need follow-up now.

    Returns:
        List of dicts with all data needed for follow-up email generation.
    """
    # Build Notion filter — status type uses "status" not "select"
    filter_params = {
        "and": [
            {
                "property": "Status",
                "status": {"is_not_empty": True}
            },
            {
                "property": "Manual Override",
                "checkbox": {"equals": False}
            }
        ]
    }

    try:
        pages = notion_client.query_database(filter_params)
    except Exception as e:
        logger.error(f"Failed to query Notion: {e}")
        return []

    actionable = []
    for page in pages:
        page_id = page["id"]

        # Extract properties using real Notion property names
        status = notion_client.get_status_property(page, "Status")

        # Check project status from rollup (Status [Proyectos])
        project_status = notion_client.get_rollup_status(page, "Status [Proyectos]")

        # Item is active if EITHER pendientes status OR project status matches
        status_match = status in ACTIVE_STATUSES
        project_status_match = project_status in ACTIVE_PROJECT_STATUSES
        if not (status_match or project_status_match):
            continue

        manual_override = notion_client.get_checkbox_property(page, "Manual Override")
        if manual_override:
            continue

        current_stage = notion_client.get_number_property(page, "Follow-Up Stage")
        if current_stage >= 4:
            continue

        # Due date from CS deadline
        due_date = notion_client.get_date_property(page, "Fecha límite de Client Success")
        # Fallback to project target date rollup
        if not due_date:
            due_date = notion_client.get_rollup_date(page, "Fecha Objetivo [Proyectos]")

        last_followup = notion_client.get_date_property(page, "Last Follow-Up Date")

        # Resolve client email via relation chain
        client_email = notion_client.resolve_client_email(page)
        if not client_email:
            pending_name = notion_client.get_text_property(page, "Nombre")
            logger.warning(f"Skipping page {page_id} ({pending_name}): no client email found")
            continue

        # Check if follow-up is due
        is_due, next_stage, _ = is_followup_due(current_stage, last_followup, due_date)
        if not is_due:
            continue

        # Resolve project name via relation chain
        project_name = notion_client.resolve_project_name(page)

        # Resolve client name via relation chain
        client_name = notion_client.resolve_client_name(page)

        # Resolve senior contact email for Stage 4 escalation
        senior_contact_email = notion_client.resolve_senior_contact_email(page)

        # Resolve client country for business hours check
        client_country = notion_client.resolve_client_country(page)

        # Build item data
        item = {
            "page_id": page_id,
            "project_name": project_name,
            "client_name": client_name,
            "client_email": client_email,
            "senior_contact_email": senior_contact_email,
            "client_country": client_country,
            "client_language": notion_client.get_select_property(page, "Client Language") or "ES",
            "pending_item": notion_client.get_text_property(page, "Nombre"),
            "due_date": due_date,
            "days_overdue": days_overdue(due_date),
            "impact_description": notion_client.get_rollup_text(page, "Detalle Falta info / Pausado [Proyectos]"),
            "follow_up_stage": current_stage,
            "next_stage": next_stage,
            "last_followup_date": last_followup,
            "manual_override": False,
            "delivery_team_email": "",  # not in current DB; can be added later
            "delivery_team_slack_channel": "",
            "gmail_thread_id": notion_client.get_text_property(page, "Gmail Thread ID"),
            "status": status,
            # CS member and analyst — people fields, resolved by name
            "client_success": notion_client.get_people_first(page, "Owner - Client Success"),
            "cs_email": notion_client.get_people_email(page, "Owner - Client Success"),
            "analista": notion_client.get_rollup_people_first(page, "Responsable [Proyectos]"),
            # Extra context for email generation
            "cs_comments": notion_client.get_text_property(page, "Comentarios Client Success"),
            "project_status": project_status,
            # Documentation URL from Proyectos table (Dropbox/Drive link)
            "documentation_url": notion_client.resolve_documentation_url(page),
        }

        actionable.append(item)
        logger.info(f"Actionable: {item['project_name']} — {item['pending_item']} — "
                     f"Stage {current_stage} → {next_stage}")

    logger.info(f"Found {len(actionable)} items needing follow-up")
    return actionable


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    items = get_actionable_items()
    if items:
        print(f"\nFound {len(items)} actionable items:\n")
        for item in items:
            print(f"  - {item['project_name']}: {item['pending_item']} "
                  f"(Stage {item['follow_up_stage']} → {item['next_stage']}, "
                  f"{item['days_overdue']} days overdue, {item['client_language']})")
    else:
        print("\nNo items need follow-up right now.")

    # Also output as JSON for piping
    if "--json" in sys.argv:
        print(json.dumps(items, indent=2, ensure_ascii=False))
