"""
Extract context from team messages and forward to clients.
Takes a raw team message (from email or Slack), uses Claude to extract context,
matches to a Notion record, and creates a draft/sends a follow-up to the client.
"""

import logging

from datetime import datetime, timezone

import claude_client
import gmail_client
import notion_client
import draft_manager
import team_manager
from config import SYSTEM_MODE, COMPANY_NAME, GMAIL_DEFAULT_SENDER_EMAIL
from style_store import load_style_examples

logger = logging.getLogger(__name__)


def process_team_message(message_data, source="email"):
    """
    Process a single team message and relay to the appropriate client.

    Args:
        message_data: Dict with message info.
            For email: Gmail message dict (from, subject, body, etc.)
            For Slack: Dict with 'text', 'user', 'ts', 'channel'
        source: 'email' or 'slack'

    Returns:
        Dict with 'success', 'action', 'project', 'details'
    """
    # Extract raw text from the message
    if source == "email":
        raw_text = f"Subject: {message_data.get('subject', '')}\n\n{message_data.get('body', message_data.get('snippet', ''))}"
        msg_id = message_data.get("id", "")
    elif source == "slack":
        raw_text = message_data.get("text", "")
        msg_id = message_data.get("ts", "")
    else:
        return {"success": False, "action": "error", "details": f"Unknown source: {source}"}

    if not raw_text or len(raw_text.strip()) < 10:
        return {"success": False, "action": "skipped", "details": "Message too short"}

    # 1. Extract context using Claude
    context = claude_client.extract_context(raw_text, company_name=COMPANY_NAME)
    if not context:
        logger.warning(f"Could not extract context from {source} message")
        return {"success": False, "action": "extraction_failed", "details": "Claude could not extract context"}

    confidence = context.get("confidence", 0.0)
    project_name = context.get("project_name", "Unknown")

    if confidence < 0.5:
        logger.warning(f"Low confidence extraction ({confidence}) for {source} message. Flagging for CS review.")
        # Log in Notion if we can identify the project
        return {
            "success": False,
            "action": "low_confidence",
            "project": project_name,
            "confidence": confidence,
            "details": "Extraction confidence too low. Flagged for CS review.",
        }

    # 2. Find matching Notion record
    notion_item = _find_notion_record(project_name, context.get("client_name"))
    if not notion_item:
        logger.warning(f"No Notion record found for project: {project_name}")
        return {
            "success": False,
            "action": "no_notion_match",
            "project": project_name,
            "details": f"Could not find Notion record for project '{project_name}'",
        }

    page_id = notion_item["page_id"]
    client_email = notion_item["client_email"]
    client_name = notion_item["client_name"]
    language = notion_item["client_language"]

    # Resolve CS member as sender for relay
    cs_name = notion_item.get("client_success", "")
    cs_email = team_manager.resolve_email(cs_name) if cs_name else None
    if not cs_email:
        cs_email = GMAIL_DEFAULT_SENDER_EMAIL

    # CC only admins — the CS member is the sender
    cc_recipients = team_manager.get_admins_cc() or team_manager.get_cc_recipients(language)

    if not client_email:
        logger.warning(f"No client email for project: {project_name}")
        return {"success": False, "action": "no_client_email", "project": project_name}

    # 3. Generate client-facing message
    info_summary = context.get("summary", "")
    action_needed = context.get("action_needed", "")

    style_examples = load_style_examples(language)

    # Generate a relay email using Claude
    relay_context = {
        "project_name": project_name,
        "client_name": client_name,
        "pending_item": f"Update: {context.get('information_type', 'information')}",
        "due_date": "",
        "days_overdue": 0,
        "impact_description": f"{info_summary}. {action_needed}",
        "follow_up_stage": 0,
    }

    email = claude_client.generate_followup_email(
        context=relay_context,
        language=language,
        stage=1,  # Use friendly tone for relay messages
        company_name=COMPANY_NAME,
        style_examples=style_examples,
    )

    if not email:
        return {"success": False, "action": "generation_failed", "project": project_name}

    # 4. Draft or send based on mode (FROM the CS member)
    if SYSTEM_MODE == "DRAFT":
        result = draft_manager.create_draft_and_notify(
            to=client_email,
            subject=email["subject"],
            body_html=email["body_html"],
            project_name=project_name,
            client_name=client_name,
            language=language,
            notion_page_id=page_id,
            stage=0,  # Relay, not part of escalation sequence
            context_data=relay_context,
            cc=cc_recipients,
            from_email=cs_email,
        )
        action = "draft_created"
    else:
        result = gmail_client.send_email(
            to=client_email,
            subject=email["subject"],
            body_html=email["body_html"],
            cc=cc_recipients,
            from_email=cs_email,
        )
        action = "email_sent"

    if not result:
        return {"success": False, "action": f"{action}_failed", "project": project_name}

    # 4b. Create tracking sub-page
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subpage_title = f"Relay — Team message forwarded — {now_str}"
    subpage_entries = [
        {"label": "Source", "value": source},
        {"label": "Info Type", "value": context.get("information_type", "N/A")},
        {"label": "Action", "value": action},
        {"label": "Sender", "value": cs_email},
        {"label": "Recipient", "value": client_email},
        {"label": "CC", "value": cc_recipients or "N/A"},
    ]
    try:
        content_blocks = notion_client.build_subpage_content(subpage_entries)
        notion_client.create_followup_subpage(page_id, subpage_title, content_blocks)
    except Exception as e:
        logger.warning(f"Could not create relay sub-page: {e}")

    # 5. Log in Notion
    try:
        log_msg = f"RELAY ({source}): Team message forwarded to client. Info type: {context.get('information_type', 'N/A')}. Action: {action}."
        notion_client.append_to_log(page_id, log_msg)
    except Exception as e:
        logger.error(f"Failed to log relay in Notion: {e}")

    # Mark source email as read
    if source == "email" and msg_id:
        gmail_client.mark_as_read(msg_id)

    return {
        "success": True,
        "action": action,
        "project": project_name,
        "client": client_name,
        "info_type": context.get("information_type"),
        "details": f"Relayed to {client_email}",
    }


def _find_notion_record(project_name, client_name=None):
    """
    Find a Notion page in Pendientes Client Success matching the project name.
    Searches by the title field "Nombre".

    Returns:
        Dict with page_id, client_email, client_name, client_language, or None
    """
    # Search by the title property "Nombre"
    try:
        filter_params = {
            "property": "Nombre",
            "title": {"contains": project_name}
        }
        pages = notion_client.query_database(filter_params)
        if pages:
            page = pages[0]
            return {
                "page_id": page["id"],
                "client_email": notion_client.resolve_client_email(page),
                "client_name": "",
                "client_language": notion_client.get_select_property(page, "Client Language") or "ES",
                "client_success": notion_client.get_people_first(page, "Owner - Client Success"),
                "analista": notion_client.get_rollup_people_first(page, "Responsable [Proyectos]"),
            }
    except Exception:
        pass

    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("extract_and_forward.py — Use via daemon or directly:")
    print("  from extract_and_forward import process_team_message")
    print("  result = process_team_message(message_data, source='email')")
