"""
Send (or draft) a follow-up email for a single pending item.
Behavior depends on SYSTEM_MODE:
  - DRAFT: Creates Gmail draft + Slack notification for review
  - SEMI_AUTO: Sends email after configurable delay (with cancel option)
  - AUTO: Sends email immediately

Updates Notion with new stage, dates, and log entry.
"""

import logging
import re
import time
from datetime import datetime, timezone

import claude_client
import gmail_client
import notion_client
import draft_manager
import team_manager
from config import (
    SYSTEM_MODE,
    COMPANY_NAME,
    SEMI_AUTO_DELAY,
    GMAIL_SEND_DELAY,
    GMAIL_DEFAULT_SENDER_EMAIL,
    STYLE_DATA_DIR,
    CC_ALWAYS_EMAILS,
)
from compute_next_followup import compute_next_followup_date
from style_store import load_style_examples

logger = logging.getLogger(__name__)


def _load_fallback_template(stage, language):
    """Load a fallback HTML template when Claude API fails."""
    from pathlib import Path

    template_names = {1: "reminder", 2: "second_notice", 3: "urgent", 4: "escalation"}
    template_name = template_names.get(stage, "reminder")
    template_path = Path(__file__).parent / "templates" / f"{template_name}_{language.lower()}.html"

    if template_path.exists():
        return template_path.read_text(encoding="utf-8")
    return None


EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


def _is_valid_email(email):
    """Validate email format."""
    return bool(email and EMAIL_REGEX.match(email.strip()))


def _get_idempotency_key(page_id, stage, date_str):
    """Generate an idempotency key to prevent duplicate sends."""
    import hashlib
    raw = f"{page_id}:{stage}:{date_str}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _check_already_sent(page_id, stage):
    """
    Check if a follow-up was already sent for this page+stage today.
    Reads the Follow-Up Log from Notion to detect duplicates.
    """
    try:
        page = notion_client.get_page(page_id)
        if not page:
            return False
        current_stage = notion_client.get_number_property(page, "Follow-Up Stage")
        # If the current stage in Notion is already >= the stage we want to send,
        # it means it was already processed
        if current_stage >= stage:
            return True
    except Exception:
        pass
    return False


def _download_attachment(url, filename):
    """
    Download a file from a URL (Notion-hosted, Dropbox, or Drive).
    Returns dict with 'filename', 'data' (bytes), 'mime_type'.
    """
    import mimetypes
    import requests as req

    try:
        # Handle Dropbox URLs: convert to direct download
        if "dropbox.com" in url:
            url = url.replace("dl=0", "dl=1").replace("www.dropbox.com", "dl.dropboxusercontent.com")

        # Handle Google Drive URLs: convert to direct download
        if "drive.google.com" in url:
            # Extract file ID from various Drive URL formats
            import re
            match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
            if match:
                file_id = match.group(1)
                url = f"https://drive.google.com/uc?export=download&id={file_id}"

        resp = req.get(url, timeout=30, allow_redirects=True)
        resp.raise_for_status()

        # Determine MIME type
        content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()
        if not content_type or content_type == "application/octet-stream":
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        return {
            "filename": filename,
            "data": resp.content,
            "mime_type": content_type,
        }
    except Exception as e:
        logger.warning(f"Failed to download {url}: {e}")
        return None


def send_followup_for_item(item):
    """
    Process a single follow-up item.

    Args:
        item: Dict from check_pending_items.get_actionable_items() with keys:
            page_id, project_name, client_name, client_email, senior_contact_email,
            client_language, pending_item, due_date, days_overdue,
            impact_description, follow_up_stage, next_stage,
            gmail_thread_id, status

    Returns:
        Dict with 'success', 'mode', 'draft_id' or 'message_id', and 'error'
    """
    page_id = item["page_id"]
    next_stage = item["next_stage"]
    language = item["client_language"]

    # Deduplication check: prevent sending same stage twice
    if _check_already_sent(page_id, next_stage):
        logger.info(f"Skipping {item['project_name']} — Stage {next_stage} already sent (dedup)")
        return {"success": False, "error": "Already sent (deduplication)", "mode": SYSTEM_MODE}

    # Resolve sender: CS member assigned to this project (from Notion people field)
    sender_email = item.get("cs_email", "")
    if not sender_email:
        sender_email = GMAIL_DEFAULT_SENDER_EMAIL
        logger.warning(f"No CS member email for {item['project_name']}, using default sender")

    # CC: Only Diana, Piero, and César Montes (resolved from Proyectos Owner field)
    cc_set = set()

    # Resolve fixed CC recipients from Notion Proyectos table
    fixed_cc = notion_client.resolve_fixed_cc_emails()
    cc_set.update(fixed_cc)

    # Fallback: use CC_ALWAYS_EMAILS env var if Notion resolution found nothing
    if not cc_set and CC_ALWAYS_EMAILS:
        for email in CC_ALWAYS_EMAILS.split(","):
            email = email.strip()
            if email:
                cc_set.add(email)

    # Never CC the sender
    cc_set.discard(sender_email)
    cc_recipients = ", ".join(sorted(cc_set)) if cc_set else ""

    # Determine recipient: senior contact for Stage 4, primary otherwise
    if next_stage == 4 and item.get("senior_contact_email"):
        recipient = item["senior_contact_email"]
    else:
        recipient = item["client_email"]

    if not recipient:
        logger.error(f"No recipient email for {item['project_name']}")
        return {"success": False, "error": "No recipient email"}

    if not _is_valid_email(recipient):
        logger.error(f"Invalid recipient email format for {item['project_name']}: {recipient}")
        return {"success": False, "error": f"Invalid email format: {recipient}"}

    # Build context for Claude
    # "information_needed" = what the client must provide (from Detalle Falta info)
    # "pending_item" = internal deliverable name (context only, not shown to client)
    context = {
        "project_name": item["project_name"],
        "client_name": item["client_name"],
        "pending_item": item["pending_item"],
        "information_needed": item["impact_description"] or "",
        "due_date": item["due_date"],
        "days_overdue": item["days_overdue"],
        "impact_description": item["impact_description"] or "May cause delays in the project timeline",
        "follow_up_stage": item["follow_up_stage"],
    }

    # Load style examples from learning engine
    style_examples = load_style_examples(language)

    # Generate email with Claude API
    sender_name = item.get("client_success", "") or "Client Success"
    email = claude_client.generate_followup_email(
        context=context,
        language=language,
        stage=next_stage,
        company_name=COMPANY_NAME,
        style_examples=style_examples,
        sender_name=sender_name,
    )

    # Fallback to template if Claude fails
    if not email:
        logger.warning(f"Claude failed for {item['project_name']}. Using fallback template.")
        fallback_body = _load_fallback_template(next_stage, language)
        if fallback_body:
            # Simple variable substitution in template
            fallback_body = fallback_body.replace("{{project_name}}", item["project_name"])
            fallback_body = fallback_body.replace("{{client_name}}", item["client_name"])
            fallback_body = fallback_body.replace("{{pending_item}}", item["pending_item"])
            fallback_body = fallback_body.replace("{{due_date}}", item["due_date"] or "N/A")
            email = {
                "subject": f"Follow-up: {item['project_name']}",
                "body_html": fallback_body,
            }
        else:
            logger.error(f"No fallback template for stage {next_stage}/{language}")
            return {"success": False, "error": "Claude API failed and no fallback template"}

    subject = email["subject"]
    body_html = email["body_html"]

    # ─── Download documentation from Proyectos (Dropbox/Drive) ─────────────
    attachments = []
    doc_url = item.get("documentation_url", "")
    if doc_url:
        try:
            # Extract a reasonable filename from the URL
            from urllib.parse import urlparse, unquote
            parsed = urlparse(doc_url)
            filename = unquote(parsed.path.split("/")[-1]) or "documento"
            if not any(filename.endswith(ext) for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".png", ".jpg"]):
                filename = f"{item['project_name']} - Documentación"

            downloaded = _download_attachment(doc_url, filename)
            if downloaded:
                attachments.append(downloaded)
                logger.info(f"Downloaded documentation: {filename} from {doc_url[:60]}...")
        except Exception as e:
            logger.warning(f"Could not download documentation from {doc_url[:60]}: {e}")

    # ─── Execute based on SYSTEM_MODE ────────────────────────────────────────
    result = {"success": False, "mode": SYSTEM_MODE}

    if SYSTEM_MODE == "DRAFT":
        # Create Gmail draft + Slack notification
        draft_result = draft_manager.create_draft_and_notify(
            to=recipient,
            subject=subject,
            body_html=body_html,
            project_name=item["project_name"],
            client_name=item["client_name"],
            language=language,
            notion_page_id=page_id,
            stage=next_stage,
            context_data=context,
            cc=cc_recipients,
            thread_id=item.get("gmail_thread_id"),
            from_email=sender_email,
            attachments=attachments,
        )

        if draft_result:
            result["success"] = True
            result["draft_id"] = draft_result["draft_id"]
            log_msg = f"Stage {next_stage} draft created (Draft ID: {draft_result['draft_id']}). Mode: DRAFT — awaiting CS review."
        else:
            result["error"] = "Failed to create draft"
            log_msg = f"Stage {next_stage} draft creation FAILED."

    elif SYSTEM_MODE == "SEMI_AUTO":
        # Create draft first, then schedule send after delay
        # CS can delete the draft during the cancel window to prevent sending
        draft_result = gmail_client.create_draft(
            to=recipient,
            subject=subject,
            body_html=body_html,
            cc=cc_recipients,
            thread_id=item.get("gmail_thread_id"),
            from_email=sender_email,
        )

        if not draft_result:
            result["error"] = "Failed to create draft for SEMI_AUTO"
            log_msg = f"Stage {next_stage} SEMI_AUTO draft creation FAILED."
        else:
            draft_id = draft_result["id"]
            logger.info(f"SEMI_AUTO: Draft {draft_id} created. Waiting {SEMI_AUTO_DELAY}s cancel window...")

            # Notify via Slack that email is queued
            try:
                import slack_client
                from config import SLACK_REVIEW_CHANNEL
                if SLACK_REVIEW_CHANNEL:
                    cancel_minutes = SEMI_AUTO_DELAY // 60
                    slack_client.send_message(
                        channel_id=SLACK_REVIEW_CHANNEL,
                        text=f"⏳ Follow-up para {item['project_name']} (Stage {next_stage}) se enviará en {cancel_minutes} min. "
                             f"Para cancelar: elimina el borrador (Draft ID: {draft_id}) de Gmail.",
                    )
            except Exception as e:
                logger.warning(f"Could not send SEMI_AUTO Slack notification: {e}")

            # Wait for the cancel window
            import math
            check_interval = 30  # Check every 30 seconds if draft still exists
            checks = math.ceil(SEMI_AUTO_DELAY / check_interval)
            draft_cancelled = False

            for i in range(checks):
                time.sleep(check_interval)
                # Check if draft still exists (CS may have deleted it to cancel)
                try:
                    existing = gmail_client.get_draft(draft_id, from_email=sender_email)
                    if not existing:
                        draft_cancelled = True
                        break
                except Exception:
                    # If we can't check, assume it still exists
                    pass

            if draft_cancelled:
                result["success"] = False
                result["error"] = "Cancelled by CS during review window"
                log_msg = f"Stage {next_stage} SEMI_AUTO CANCELLED by CS (draft deleted during {SEMI_AUTO_DELAY}s window)."
                logger.info(f"SEMI_AUTO: Draft {draft_id} was cancelled by CS")
            else:
                # Draft still exists — send the email
                send_result = gmail_client.send_email(
                    to=recipient,
                    subject=subject,
                    body_html=body_html,
                    cc=cc_recipients,
                    thread_id=item.get("gmail_thread_id"),
                    from_email=sender_email,
                )

                if send_result:
                    result["success"] = True
                    result["message_id"] = send_result["id"]
                    result["thread_id"] = send_result["threadId"]
                    log_msg = f"Stage {next_stage} email sent (SEMI_AUTO, after {SEMI_AUTO_DELAY}s window). Message ID: {send_result['id']}"
                else:
                    result["error"] = "Failed to send email after delay"
                    log_msg = f"Stage {next_stage} email send FAILED (SEMI_AUTO, after delay)."

                # Clean up draft after successful send
                try:
                    gmail_client.get_draft(draft_id, from_email=sender_email)
                except Exception:
                    pass  # Draft may have been auto-deleted by Gmail after send

    elif SYSTEM_MODE == "AUTO":
        # Send immediately
        send_result = gmail_client.send_email(
            to=recipient,
            subject=subject,
            body_html=body_html,
            cc=cc_recipients,
            thread_id=item.get("gmail_thread_id"),
            from_email=sender_email,
        )

        if send_result:
            result["success"] = True
            result["message_id"] = send_result["id"]
            result["thread_id"] = send_result["threadId"]
            log_msg = f"Stage {next_stage} email sent (AUTO). Message ID: {send_result['id']}"
        else:
            result["error"] = "Failed to send email"
            log_msg = f"Stage {next_stage} email send FAILED (AUTO)."

    # ─── Update Notion ──────────────────────────────────────────────────────
    if result["success"]:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Compute next follow-up date for the new stage
        next_followup = compute_next_followup_date(next_stage, now_str)
        next_followup_str = next_followup.strftime("%Y-%m-%d") if next_followup else None

        # Keep status as "En curso" (active follow-up) — status type, not select
        properties = {
            "Follow-Up Stage": notion_client.build_number(next_stage),
            "Last Follow-Up Date": notion_client.build_date(now_str),
            "Status": notion_client.build_status("En curso"),
        }

        if next_followup_str:
            properties["Next Follow-Up Date"] = notion_client.build_date(next_followup_str)

        # Store Gmail thread ID if this is the first email
        if result.get("thread_id") and not item.get("gmail_thread_id"):
            properties["Gmail Thread ID"] = notion_client.build_rich_text(result["thread_id"])

        try:
            notion_client.update_page(page_id, properties)
            notion_client.append_to_log(page_id, log_msg)
            logger.info(f"Notion updated for {item['project_name']}: Stage {next_stage}")
        except Exception as e:
            logger.error(f"Failed to update Notion for {item['project_name']}: {e}")
            result["notion_error"] = str(e)

        # Create follow-up sub-page for tracking
        stage_names = {1: "Reminder", 2: "Second Notice", 3: "Urgent", 4: "Escalation"}
        subpage_title = f"Stage {next_stage} — {stage_names.get(next_stage, 'Follow-up')} — {now_str}"
        subpage_entries = [
            {"label": "Mode", "value": SYSTEM_MODE},
            {"label": "Sender", "value": sender_email},
            {"label": "Recipient", "value": recipient},
            {"label": "Subject", "value": subject},
            {"label": "CC", "value": cc_recipients or "N/A"},
        ]
        if result.get("draft_id"):
            subpage_entries.append({"label": "Draft ID", "value": result["draft_id"]})
        if result.get("message_id"):
            subpage_entries.append({"label": "Message ID", "value": result["message_id"]})
        try:
            content_blocks = notion_client.build_subpage_content(subpage_entries)
            notion_client.create_followup_subpage(page_id, subpage_title, content_blocks)
        except Exception as e:
            logger.warning(f"Could not create follow-up sub-page: {e}")
    else:
        # Log failure in Notion
        try:
            notion_client.append_to_log(page_id, log_msg)
        except Exception:
            pass

    # Rate limit between sends
    time.sleep(GMAIL_SEND_DELAY)

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(f"send_followup.py — Current mode: {SYSTEM_MODE}")
    print("Use check_pending_items.py to find items, then call send_followup_for_item(item)")
