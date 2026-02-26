"""
Draft Manager for Client Follow-Up Autopilot.
Creates Gmail Drafts and posts them to Slack review channel.
Used in DRAFT mode — CS team reviews, edits, and sends manually.
Also logs draft data for the learning engine to compare against actual sent emails.
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import gmail_client
import slack_client
from config import STYLE_DATA_DIR

logger = logging.getLogger(__name__)


def _ensure_style_data_dir():
    """Ensure the style data directory exists."""
    STYLE_DATA_DIR.mkdir(parents=True, exist_ok=True)


def _strip_html(html):
    """Simple HTML tag removal for preview text."""
    clean = re.sub(r"<[^>]+>", "", html)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def create_draft_and_notify(
    to,
    subject,
    body_html,
    project_name,
    client_name,
    language,
    notion_page_id,
    stage,
    context_data,
    cc=None,
    thread_id=None,
    from_email=None,
    attachments=None,
):
    """
    Create a Gmail draft and post it to Slack review channel.
    Also logs the draft for the learning engine.

    Args:
        to: Recipient email
        subject: Email subject
        body_html: HTML email body
        project_name: Project name for Slack notification
        client_name: Client name for Slack notification
        language: ES/EN/PT
        notion_page_id: Notion page ID for reference
        stage: Follow-up stage (1-4)
        context_data: Dict of context used to generate the email
        cc: Optional CC address
        thread_id: Optional Gmail thread ID
        from_email: Sender email (draft appears in this user's Gmail in service_account mode)
        attachments: Optional list of dicts with 'filename', 'data', 'mime_type'

    Returns:
        Dict with 'draft_id', 'slack_ts', 'logged' keys, or None on failure
    """
    # 1. Create Gmail Draft (with attachments if any)
    draft_result = gmail_client.create_draft(
        to, subject, body_html, cc, thread_id,
        from_email=from_email, attachments=attachments,
    )
    if not draft_result:
        logger.error(f"Failed to create Gmail draft for {project_name}")
        return None

    draft_id = draft_result["id"]
    logger.info(f"Gmail draft created: {draft_id}")

    # 2. Post to Slack review channel (tarjeta interactiva con botones)
    body_preview = _strip_html(body_html)
    slack_result = slack_client.post_draft_for_review(
        project_name=project_name,
        client_name=client_name,
        subject=subject,
        body_preview=body_preview,
        draft_id=draft_id,
        language=language,
        recipient_email=to,
        sender_email=from_email or "",
        stage=stage,
        cc=cc or "",
    )

    slack_ts = slack_result.get("ts") if slack_result else None

    # 3. Log draft for learning engine
    logged = _log_draft(
        draft_id=draft_id,
        to=to,
        subject=subject,
        body_html=body_html,
        body_text=body_preview,
        project_name=project_name,
        client_name=client_name,
        language=language,
        stage=stage,
        notion_page_id=notion_page_id,
        context_data=context_data,
        from_email=from_email,
    )

    return {
        "draft_id": draft_id,
        "slack_ts": slack_ts,
        "logged": logged,
    }


def _log_draft(draft_id, to, subject, body_html, body_text, project_name,
               client_name, language, stage, notion_page_id, context_data, from_email=None):
    """
    Log a draft to the drafts_log.jsonl file for the learning engine.
    Each line is a JSON object.
    """
    _ensure_style_data_dir()
    log_path = STYLE_DATA_DIR / "drafts_log.jsonl"

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "draft_id": draft_id,
        "from_email": from_email or "",
        "to": to,
        "subject": subject,
        "body_html": body_html,
        "body_text": body_text,
        "project_name": project_name,
        "client_name": client_name,
        "language": language,
        "stage": stage,
        "notion_page_id": notion_page_id,
        "context": context_data,
        "status": "pending_review",  # pending_review → sent_as_is | sent_edited | discarded
    }

    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        logger.info(f"Draft logged: {draft_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to log draft: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("draft_manager.py — No standalone test. Use via send_followup.py")
