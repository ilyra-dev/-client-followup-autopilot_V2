"""
Scan client inbox for responses to follow-up emails.
Checks:
1. Replies in tracked Gmail threads (matching Gmail Thread IDs from Notion)
2. New emails from known client email addresses

Returns list of potential client responses for processing.
"""

import logging
from datetime import datetime, timedelta, timezone

import gmail_client
import notion_client
import team_manager
from config import GMAIL_AUTH_MODE

logger = logging.getLogger(__name__)


def get_tracked_threads():
    """
    Get all active Gmail Thread IDs from Notion (items with active follow-up sequences).

    Returns:
        Dict mapping thread_id → page data dict
    """
    filter_params = {
        "and": [
            {"property": "Status", "status": {"is_not_empty": True}},
            {"property": "Gmail Thread ID", "rich_text": {"is_not_empty": True}},
        ]
    }

    try:
        pages = notion_client.query_database(filter_params)
    except Exception as e:
        logger.error(f"Failed to query Notion for tracked threads: {e}")
        return {}

    threads = {}
    for page in pages:
        status = notion_client.get_status_property(page, "Status")
        if status == "Listo":
            continue  # Skip already completed items

        thread_id = notion_client.get_text_property(page, "Gmail Thread ID")
        if thread_id:
            # Resolve client email via relation chain
            client_email = notion_client.resolve_client_email(page)

            threads[thread_id] = {
                "page_id": page["id"],
                "project_name": notion_client.resolve_project_name(page),
                "client_name": "",
                "client_email": client_email,
                "pending_item": notion_client.get_text_property(page, "Nombre"),
                "client_language": notion_client.get_select_property(page, "Client Language") or "ES",
                "status": status,
                "delivery_team_email": "",
                "delivery_team_slack_channel": "",
                "follow_up_stage": notion_client.get_number_property(page, "Follow-Up Stage"),
                "client_success": notion_client.get_people_first(page, "Owner - Client Success"),
                "analista": notion_client.get_rollup_people_first(page, "Responsable [Proyectos]"),
            }

    logger.info(f"Found {len(threads)} tracked threads in Notion")
    return threads


def get_known_client_emails():
    """
    Get all unique client email addresses from Notion.

    Returns:
        Dict mapping email (lowercase) → list of page data dicts
    """
    # Get all active items (not completed)
    filter_params = {
        "property": "Status",
        "status": {"does_not_equal": "Listo"},
    }

    try:
        pages = notion_client.query_database(filter_params)
    except Exception as e:
        logger.error(f"Failed to query Notion for client emails: {e}")
        return {}

    email_map = {}
    for page in pages:
        # Resolve client email via relation chain
        email = notion_client.resolve_client_email(page)
        if email:
            email_lower = email.lower()
            if email_lower not in email_map:
                email_map[email_lower] = []
            email_map[email_lower].append({
                "page_id": page["id"],
                "project_name": notion_client.resolve_project_name(page),
                "client_name": "",
                "pending_item": notion_client.get_text_property(page, "Nombre"),
                "client_language": notion_client.get_select_property(page, "Client Language") or "ES",
                "status": notion_client.get_status_property(page, "Status"),
                "delivery_team_email": "",
                "delivery_team_slack_channel": "",
                "follow_up_stage": notion_client.get_number_property(page, "Follow-Up Stage"),
                "client_success": notion_client.get_people_first(page, "Owner - Client Success"),
                "analista": notion_client.get_rollup_people_first(page, "Responsable [Proyectos]"),
            })

    logger.info(f"Found {len(email_map)} unique client emails")
    return email_map


def _get_scan_emails():
    """
    Determine which email inboxes to scan.

    In service_account mode: scan each CS member's inbox.
    In oauth2 mode: scan the single authenticated inbox (None = default).

    Returns:
        List of email strings to scan, or [None] for single-inbox mode.
    """
    if GMAIL_AUTH_MODE == "service_account":
        cs_members = team_manager.get_cs_members()
        if cs_members:
            emails = [m["email"] for m in cs_members]
            logger.info(f"Multi-inbox mode: scanning {len(emails)} CS inboxes")
            return emails
        logger.warning("service_account mode but no CS members found; falling back to single inbox")
    return [None]


def scan_for_responses(hours_back=2):
    """
    Scan Gmail for client responses.

    Strategy:
    1. Check tracked threads for new replies
    2. Check inbox for emails from known client addresses

    In service_account mode, scans the inbox of each CS member.
    In oauth2 mode, scans the single authenticated inbox.

    Args:
        hours_back: How far back to look (default 2 hours)

    Returns:
        List of response dicts with:
            - message: Gmail message dict (id, threadId, from, subject, body, etc.)
            - notion_items: List of matching Notion page data
            - match_type: 'thread' or 'email'
    """
    responses = []
    seen_message_ids = set()
    scan_emails = _get_scan_emails()

    # 1. Check tracked threads
    tracked = get_tracked_threads()
    if tracked:
        for thread_id, page_data in tracked.items():
            # Determine which inbox to check for this thread
            cs_name = page_data.get("client_success", "")
            cs_email = team_manager.resolve_email(cs_name) if cs_name else None

            try:
                thread_messages = gmail_client.get_thread(thread_id, from_email=cs_email)
                for msg in thread_messages:
                    from_addr = msg.get("from", "").lower()
                    if page_data["client_email"] and page_data["client_email"].lower() in from_addr:
                        if msg["id"] not in seen_message_ids and "UNREAD" in msg.get("labelIds", []):
                            responses.append({
                                "message": msg,
                                "notion_items": [page_data],
                                "match_type": "thread",
                            })
                            seen_message_ids.add(msg["id"])
            except Exception as e:
                logger.error(f"Error checking thread {thread_id}: {e}")

    # 2. Check inbox for emails from known clients (scan each CS inbox)
    client_emails = get_known_client_emails()
    if client_emails:
        query = f"is:unread newer_than:{hours_back}h in:inbox"
        for inbox_email in scan_emails:
            try:
                inbox_messages = gmail_client.read_inbox(
                    query, max_results=100, from_email=inbox_email,
                )
                for msg in inbox_messages:
                    if msg["id"] in seen_message_ids:
                        continue

                    from_addr = msg.get("from", "").lower()
                    for client_email, items in client_emails.items():
                        if client_email in from_addr:
                            responses.append({
                                "message": msg,
                                "notion_items": items,
                                "match_type": "email",
                            })
                            seen_message_ids.add(msg["id"])
                            break
            except Exception as e:
                logger.error(f"Error scanning inbox {inbox_email or 'default'}: {e}")

    logger.info(f"Found {len(responses)} potential client responses (scanned {len(scan_emails)} inbox(es))")
    return responses


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=== Scanning for client responses ===\n")
    responses = scan_for_responses()

    if responses:
        for r in responses:
            msg = r["message"]
            items = r["notion_items"]
            print(f"  From: {msg.get('from', 'unknown')}")
            print(f"  Subject: {msg.get('subject', 'no subject')}")
            print(f"  Match type: {r['match_type']}")
            print(f"  Related projects: {[i['project_name'] for i in items]}")
            print()
    else:
        print("No client responses found.")
