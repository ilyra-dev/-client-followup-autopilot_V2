"""
Process a client response detected by scan_client_inbox.py.

For each response:
1. Classify the response using Claude (received, partial, question, unrelated)
2. If information received:
   - Update Notion status to "Received"
   - Notify CS member + analyst via email and Slack
   - Stop follow-up sequence
3. If partial/question/unrelated:
   - Flag for CS review (don't auto-respond)

Multi-sender: notifications route to the assigned CS member and analyst.
Email forwards are sent FROM the CS member (via service account delegation).
CC = admin (area leaders) only — other CS members are not CC'd.
"""

import logging
from datetime import datetime, timezone

import claude_client
import gmail_client
import notion_client
import slack_client
import team_manager
from config import GMAIL_DEFAULT_SENDER_EMAIL

logger = logging.getLogger(__name__)


def process_response(response_data):
    """
    Process a single client response.

    Args:
        response_data: Dict from scan_client_inbox.scan_for_responses():
            - message: Gmail message dict
            - notion_items: List of matching Notion page data
            - match_type: 'thread' or 'email'

    Returns:
        Dict with 'success', 'action', 'classification', 'details'
    """
    message = response_data["message"]
    notion_items = response_data["notion_items"]
    match_type = response_data["match_type"]

    msg_from = message.get("from", "unknown")
    msg_subject = message.get("subject", "no subject")
    msg_body = message.get("body", message.get("snippet", ""))
    msg_id = message.get("id", "")

    logger.info(f"Processing response from {msg_from}: {msg_subject}")

    results = []

    for item in notion_items:
        page_id = item["page_id"]
        project_name = item["project_name"]
        pending_item = item["pending_item"]
        client_name = item["client_name"]
        client_language = item.get("client_language", "ES")

        # Resolve CS member and analyst for this project
        cs_name = item.get("client_success", "")
        analyst_name = item.get("analista", "")
        cs_email = team_manager.resolve_email(cs_name) if cs_name else None
        analyst_email = team_manager.resolve_email(analyst_name) if analyst_name else None

        if cs_name and not cs_email:
            logger.warning(f"Could not resolve CS email for '{cs_name}' on {project_name}")
        if analyst_name and not analyst_email:
            logger.warning(f"Could not resolve analyst email for '{analyst_name}' on {project_name}")

        # 1. Classify the response
        classification = claude_client.classify_response(
            email_body=msg_body[:2000],  # Limit input size
            pending_item=pending_item,
        )

        if not classification:
            logger.warning(f"Could not classify response for {project_name}")
            classification = {"classification": "question", "confidence": 0.0, "summary": "Classification failed"}

        cls = classification["classification"]
        confidence = classification.get("confidence", 0.0)
        summary = classification.get("summary", "")

        logger.info(f"Classification for {project_name}: {cls} (confidence: {confidence})")

        # 2. Act based on classification
        if cls == "received" and confidence >= 0.7:
            result = _handle_received(
                page_id=page_id,
                project_name=project_name,
                client_name=client_name,
                pending_item=pending_item,
                item=item,
                message=message,
                summary=summary,
                cs_email=cs_email,
                analyst_email=analyst_email,
            )
        elif cls == "partial":
            result = _handle_partial(
                page_id=page_id,
                project_name=project_name,
                client_name=client_name,
                pending_item=pending_item,
                summary=summary,
                cs_email=cs_email,
            )
        elif cls == "question":
            result = _handle_question(
                page_id=page_id,
                project_name=project_name,
                client_name=client_name,
                summary=summary,
                item=item,
                message=message,
                cs_email=cs_email,
                analyst_name=analyst_name,
                analyst_email=analyst_email,
            )
        else:
            result = _handle_unrelated(
                page_id=page_id,
                project_name=project_name,
                summary=summary,
            )

        result["classification"] = cls
        result["confidence"] = confidence
        result["cs_email"] = cs_email
        result["analyst_email"] = analyst_email
        results.append(result)

        # Mark email as read (in the CS member's inbox if service_account mode)
        gmail_client.mark_as_read(msg_id, from_email=cs_email)

    return results


def _handle_received(page_id, project_name, client_name, pending_item, item, message, summary, cs_email=None, analyst_email=None):
    """Handle a confirmed received response — stop sequence, notify CS + analyst."""
    admins_cc = team_manager.get_admins_cc()
    sender_email = cs_email or GMAIL_DEFAULT_SENDER_EMAIL

    # Update Notion: mark as Received
    try:
        notion_client.update_page(page_id, {
            "Status": notion_client.build_status("Listo"),
        })
        notion_client.append_to_log(
            page_id,
            f"RECEIVED: Client responded with requested information. Summary: {summary}"
        )
        logger.info(f"Notion updated: {project_name} → Received")
    except Exception as e:
        logger.error(f"Failed to update Notion for {project_name}: {e}")

    # Notify via Slack (delivery channel or default)
    delivery_channel = item.get("delivery_team_slack_channel")
    slack_result = slack_client.notify_delivery_team(
        project_name=project_name,
        client_name=client_name,
        item_received=f"{pending_item} — {summary}",
        channel_id=delivery_channel,
    )

    # Notify CS member + analyst via email (FROM the CS member)
    notify_to = []
    if cs_email:
        notify_to.append(cs_email)
    if analyst_email:
        notify_to.append(analyst_email)
    # Fallback: use delivery_team_email if no specific recipients
    if not notify_to:
        delivery_email = item.get("delivery_team_email")
        if delivery_email:
            notify_to.append(delivery_email)

    email_result = None
    if notify_to:
        email_body = f"""
        <p>El cliente <strong>{client_name}</strong> ha enviado la información solicitada para el proyecto <strong>{project_name}</strong>.</p>
        <p><strong>Item:</strong> {pending_item}</p>
        <p><strong>Resumen:</strong> {summary}</p>
        <p><strong>Email original del cliente:</strong></p>
        <blockquote>{message.get('snippet', '')[:500]}</blockquote>
        <p>La secuencia de follow-up ha sido detenida automáticamente.</p>
        """
        email_result = gmail_client.send_email(
            to=", ".join(notify_to),
            subject=f"Información recibida: {pending_item} — {project_name}",
            body_html=email_body,
            cc=admins_cc,
            from_email=sender_email,
        )

    # Create tracking sub-page
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subpage_title = f"Client Response — Received — {date_str}"
    subpage_entries = [
        {"label": "Classification", "value": "Received"},
        {"label": "Summary", "value": summary},
        {"label": "CS Notified", "value": cs_email or "N/A"},
        {"label": "Analyst Notified", "value": analyst_email or "N/A"},
        {"label": "Slack Notified", "value": "Yes" if slack_result else "No"},
        {"label": "Sequence", "value": "Stopped"},
    ]
    try:
        content_blocks = notion_client.build_subpage_content(subpage_entries)
        notion_client.create_followup_subpage(page_id, subpage_title, content_blocks)
    except Exception as e:
        logger.warning(f"Could not create response sub-page: {e}")

    return {
        "success": True,
        "action": "received",
        "project": project_name,
        "summary": summary,
        "slack_notified": slack_result is not None,
        "email_notified": email_result is not None,
    }


def _handle_partial(page_id, project_name, client_name, pending_item, summary, cs_email=None):
    """Handle partial information — flag for CS review, keep sequence paused."""
    try:
        notion_client.update_page(page_id, {
            "Status": notion_client.build_status("En curso"),
            "Manual Override": notion_client.build_checkbox(True),
        })
        notion_client.append_to_log(
            page_id,
            f"PARTIAL: Client sent partial information. Flagged for CS review. Summary: {summary}"
        )
    except Exception as e:
        logger.error(f"Failed to update Notion for {project_name}: {e}")

    # Notify the assigned CS member that partial info was received
    email_result = None
    if cs_email:
        admins_cc = team_manager.get_admins_cc()
        email_body = f"""
        <p>El cliente <strong>{client_name}</strong> envió información parcial para el proyecto <strong>{project_name}</strong>.</p>
        <p><strong>Item pendiente:</strong> {pending_item}</p>
        <p><strong>Resumen:</strong> {summary}</p>
        <p><strong>Acción necesaria:</strong> Revisa la respuesta del cliente y decide si se necesita un follow-up adicional.</p>
        <p>La secuencia de follow-up ha sido pausada automáticamente.</p>
        """
        email_result = gmail_client.send_email(
            to=cs_email,
            subject=f"Info parcial recibida: {pending_item} — {project_name}",
            body_html=email_body,
            cc=admins_cc,
            from_email=GMAIL_DEFAULT_SENDER_EMAIL,
        )

    # Create tracking sub-page
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subpage_title = f"Client Response — Partial — {date_str}"
    subpage_entries = [
        {"label": "Classification", "value": "Partial"},
        {"label": "Summary", "value": summary},
        {"label": "CS Notified", "value": cs_email or "N/A"},
        {"label": "Action", "value": "Flagged for CS review, sequence paused"},
    ]
    try:
        content_blocks = notion_client.build_subpage_content(subpage_entries)
        notion_client.create_followup_subpage(page_id, subpage_title, content_blocks)
    except Exception as e:
        logger.warning(f"Could not create partial response sub-page: {e}")

    return {
        "success": True,
        "action": "flagged_partial",
        "project": project_name,
        "summary": summary,
        "cs_notified": email_result is not None,
    }


def _handle_question(page_id, project_name, client_name, summary, item=None, message=None, cs_email=None, analyst_name=None, analyst_email=None):
    """
    Handle a question from the client:
    1. Forward to analyst via email (FROM CS member) and Slack
    2. Pause follow-up sequence (status → Question)
    3. When analyst responds, team inbound flow relays their answer via CS to client
    """
    admins_cc = team_manager.get_admins_cc()
    sender_email = cs_email or GMAIL_DEFAULT_SENDER_EMAIL
    msg_snippet = ""
    if message:
        msg_snippet = message.get("snippet", message.get("body", ""))[:500]

    # Update Notion: pause sequence, mark as Question
    try:
        notion_client.update_page(page_id, {
            "Status": notion_client.build_status("En curso"),
            "Manual Override": notion_client.build_checkbox(True),
        })
        notion_client.append_to_log(
            page_id,
            f"QUESTION: Client asked a question. Forwarded to analyst ({analyst_name or 'N/A'}). Summary: {summary}"
        )
    except Exception as e:
        logger.error(f"Failed to update Notion for {project_name}: {e}")

    # Forward question to analyst via Slack (with analyst name mention)
    slack_result = None
    if item:
        delivery_channel = item.get("delivery_team_slack_channel")
        slack_result = slack_client.notify_client_question(
            project_name=project_name,
            client_name=client_name,
            question_summary=summary,
            original_snippet=msg_snippet,
            channel_id=delivery_channel,
            analyst_name=analyst_name,
        )

    # Forward question to analyst via email (FROM the CS member)
    email_result = None
    forward_to = analyst_email
    # Fallback: use delivery_team_email if no specific analyst
    if not forward_to and item:
        forward_to = item.get("delivery_team_email")

    if forward_to:
        # CC the CS member too so they're aware (if sending FROM them)
        cc_parts = []
        if admins_cc:
            cc_parts.append(admins_cc)
        if cs_email and cs_email != forward_to:
            cc_parts.append(cs_email)
        cc_str = ", ".join(cc_parts)

        email_body = f"""
        <p>El cliente <strong>{client_name}</strong> tiene una pregunta sobre el proyecto <strong>{project_name}</strong>.</p>
        <p><strong>Resumen de la pregunta:</strong> {summary}</p>
        <p><strong>Email original del cliente:</strong></p>
        <blockquote>{msg_snippet}</blockquote>
        <hr>
        <p><strong>Acción necesaria:</strong> Por favor responde a este email con la información para el cliente.
        El sistema reenviará tu respuesta al cliente automáticamente.</p>
        <p style="color:#888; font-size:12px;">Aplica la etiqueta "client-followup-needed" a tu respuesta,
        o responde en el canal de Slack del proyecto.</p>
        """
        email_result = gmail_client.send_email(
            to=forward_to,
            subject=f"Pregunta de cliente: {project_name} — {client_name}",
            body_html=email_body,
            cc=cc_str,
            from_email=sender_email,
        )

    # Create tracking sub-page
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subpage_title = f"Client Question — Forwarded to Analyst — {date_str}"
    subpage_entries = [
        {"label": "Classification", "value": "Question"},
        {"label": "Summary", "value": summary},
        {"label": "Analyst", "value": f"{analyst_name or 'N/A'} ({forward_to or 'N/A'})"},
        {"label": "Forwarded From", "value": sender_email},
        {"label": "Slack Notified", "value": "Yes" if slack_result else "No"},
        {"label": "Email Forwarded", "value": forward_to or "N/A"},
        {"label": "Status", "value": "Awaiting analyst response"},
    ]
    try:
        content_blocks = notion_client.build_subpage_content(subpage_entries)
        notion_client.create_followup_subpage(page_id, subpage_title, content_blocks)
    except Exception as e:
        logger.warning(f"Could not create question sub-page: {e}")

    return {
        "success": True,
        "action": "question_forwarded",
        "project": project_name,
        "summary": summary,
        "analyst": analyst_name,
        "analyst_email": forward_to,
        "slack_notified": slack_result is not None,
        "email_notified": email_result is not None,
    }


def _handle_unrelated(page_id, project_name, summary):
    """Handle unrelated response — log and ignore."""
    try:
        notion_client.append_to_log(
            page_id,
            f"UNRELATED: Client email not related to pending item. Summary: {summary}"
        )
    except Exception as e:
        logger.error(f"Failed to log for {project_name}: {e}")

    return {
        "success": True,
        "action": "ignored_unrelated",
        "project": project_name,
        "summary": summary,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("process_client_response.py — Use via daemon or scan_client_inbox.py")
    print("Example: scan for responses, then process each one:")
    print("  from scan_client_inbox import scan_for_responses")
    print("  from process_client_response import process_response")
    print("  for r in scan_for_responses():")
    print("      results = process_response(r)")
