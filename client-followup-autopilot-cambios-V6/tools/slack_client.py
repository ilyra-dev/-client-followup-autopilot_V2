"""
Slack Bot API wrapper for Client Follow-Up Autopilot.
Handles sending messages, reading channels, and posting review drafts.
"""

import json
import logging
import time

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from config import (
    SLACK_BOT_TOKEN,
    SLACK_DEFAULT_CHANNEL,
    SLACK_REVIEW_CHANNEL,
    SLACK_SEND_DELAY,
)

logger = logging.getLogger(__name__)

_last_send_time = 0.0


def _get_client():
    """Return a Slack WebClient instance."""
    if not SLACK_BOT_TOKEN:
        raise ValueError("SLACK_BOT_TOKEN not set in .env")
    return WebClient(token=SLACK_BOT_TOKEN)


def _rate_limit():
    """Enforce Slack rate limit (~1 msg/sec)."""
    global _last_send_time
    elapsed = time.time() - _last_send_time
    if elapsed < SLACK_SEND_DELAY:
        time.sleep(SLACK_SEND_DELAY - elapsed)
    _last_send_time = time.time()


# ─── Send Messages ──────────────────────────────────────────────────────────

def send_message(channel_id, text, blocks=None):
    """
    Send a message to a Slack channel.

    Args:
        channel_id: Slack channel ID (e.g., 'C0XXXXXXX')
        text: Fallback text (shown in notifications)
        blocks: Optional Block Kit blocks for rich formatting

    Returns:
        Dict with message 'ts' (timestamp) on success, None on failure
    """
    try:
        _rate_limit()
        client = _get_client()
        kwargs = {"channel": channel_id, "text": text}
        if blocks:
            kwargs["blocks"] = blocks
        result = client.chat_postMessage(**kwargs)
        logger.info(f"Slack message sent to {channel_id} — ts: {result['ts']}")
        return {"ts": result["ts"], "channel": channel_id}
    except SlackApiError as e:
        logger.error(f"Slack send error: {e.response['error']}")
        return None
    except Exception as e:
        logger.error(f"Slack send unexpected error: {e}")
        return None


def post_draft_for_review(project_name, client_name, subject, body_preview, draft_id, language,
                          recipient_email="", sender_email="", stage=None, cc=""):
    """
    Publica un borrador de email como tarjeta interactiva en el canal de revisión.
    Muestra el contenido completo del email y botones de acción.
    """
    if not SLACK_REVIEW_CHANNEL:
        logger.warning("SLACK_REVIEW_CHANNEL no configurado. No se puede publicar borrador.")
        return None

    lang_flag = {"ES": ":flag-es:", "EN": ":flag-us:", "PT": ":flag-br:"}.get(language, ":email:")
    stage_emoji = {1: "📩", 2: "📨", 3: "🚨", 4: "🔴"}.get(stage, "📧")
    stage_name = {
        1: "Recordatorio", 2: "Segundo aviso",
        3: "Urgente", 4: "Escalamiento"
    }.get(stage, f"Etapa {stage}")

    # URL para abrir el borrador en Gmail
    gmail_draft_url = f"https://mail.google.com/mail/u/0/#drafts?compose={draft_id}"

    # ─── Construir bloques ───────────────────────────────────────────────

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{stage_emoji} Follow-Up — {project_name}",
                "emoji": True
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Proyecto:*\n{project_name}"},
                {"type": "mrkdwn", "text": f"*Cliente:*\n{client_name or '—'}"},
                {"type": "mrkdwn", "text": f"*Etapa:*\n{stage_emoji} {stage_name}"},
                {"type": "mrkdwn", "text": f"*Idioma:*\n{lang_flag} {language}"},
            ]
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Para:*\n{recipient_email or '—'}"},
                {"type": "mrkdwn", "text": f"*De:*\n{sender_email or '—'}"},
            ]
        },
    ]

    if cc:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"📋 *CC:* {cc}"}]
        })

    blocks.append({"type": "divider"})

    # Asunto
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*📧 Asunto:*\n{subject}"}
    })

    # Cuerpo completo del email — dividir en bloques de 3000 chars (límite de Slack)
    full_body = body_preview.strip()
    if not full_body:
        full_body = "(Sin contenido)"

    # Slack tiene límite de 3000 chars por bloque de texto
    MAX_BLOCK_LEN = 2900
    body_chunks = []
    remaining = full_body
    while remaining:
        if len(remaining) <= MAX_BLOCK_LEN:
            body_chunks.append(remaining)
            break
        # Cortar en el último espacio antes del límite
        cut_pos = remaining[:MAX_BLOCK_LEN].rfind(" ")
        if cut_pos <= 0:
            cut_pos = MAX_BLOCK_LEN
        body_chunks.append(remaining[:cut_pos])
        remaining = remaining[cut_pos:].strip()

    # Primer bloque con encabezado
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*📝 Contenido del email:*\n\n{body_chunks[0]}"}
    })

    # Bloques adicionales si el email es largo
    for chunk in body_chunks[1:]:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": chunk}
        })

    blocks.append({"type": "divider"})

    # ─── Botones de acción ───────────────────────────────────────────────

    action_value = json.dumps({
        "draft_id": draft_id,
        "sender_email": sender_email,
        "project_name": project_name,
        "client_name": client_name,
        "stage": stage,
    })

    blocks.append({
        "type": "actions",
        "block_id": f"draft_actions_{draft_id}",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "✅ Enviar ahora", "emoji": True},
                "style": "primary",
                "action_id": "send_draft",
                "value": action_value,
                "confirm": {
                    "title": {"type": "plain_text", "text": "Confirmar envío"},
                    "text": {"type": "mrkdwn", "text": f"¿Enviar el follow-up a *{client_name}* para el proyecto *{project_name}*?"},
                    "confirm": {"type": "plain_text", "text": "Sí, enviar"},
                    "deny": {"type": "plain_text", "text": "Cancelar"},
                }
            },
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "✏️ Editar en Gmail", "emoji": True},
                "action_id": "edit_draft_gmail",
                "url": gmail_draft_url,
            },
        ]
    })

    # Nota informativa
    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn",
             "text": "💡 *Enviar* = envía tal cual  |  *Editar en Gmail* = abre el borrador para modificar antes de enviar"}
        ]
    })

    text = f"Borrador follow-up: {project_name} — {client_name} (Etapa {stage})"
    return send_message(SLACK_REVIEW_CHANNEL, text, blocks)

    text = f"Borrador de follow-up para {client_name} — Proyecto: {project_name} (Etapa {stage})"
    return send_message(SLACK_REVIEW_CHANNEL, text, blocks)


def notify_delivery_team(project_name, client_name, item_received, channel_id=None):
    """
    Notify the delivery team that a client has responded with information.

    Args:
        project_name: Name of the project
        client_name: Name of the client
        item_received: Description of what was received
        channel_id: Override default delivery channel
    """
    target_channel = channel_id or SLACK_DEFAULT_CHANNEL
    if not target_channel:
        logger.warning("No delivery channel configured. Cannot notify team.")
        return None

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "✅ Información recibida de cliente", "emoji": True}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Proyecto:*\n{project_name}"},
                {"type": "mrkdwn", "text": f"*Cliente:*\n{client_name}"},
            ]
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Recibido:*\n{item_received}"}
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": "Secuencia de follow-up detenida automáticamente."}
            ]
        }
    ]

    text = f"Cliente {client_name} envió {item_received} — Proyecto: {project_name}"
    return send_message(target_channel, text, blocks)


def notify_client_question(project_name, client_name, question_summary, original_snippet="", channel_id=None, analyst_name=None):
    """
    Notify the analyst that a client has a question that needs answering.

    If analyst_name is provided, they are mentioned directly in the message.
    Uses the project's delivery Slack channel if available, otherwise SLACK_DEFAULT_CHANNEL.

    Args:
        project_name: Name of the project
        client_name: Name of the client
        question_summary: Summary of the client's question
        original_snippet: Excerpt from the client's original email
        channel_id: Override default delivery channel
        analyst_name: Name of the assigned analyst (mentioned in the notification)
    """
    target_channel = channel_id or SLACK_DEFAULT_CHANNEL
    if not target_channel:
        logger.warning("No delivery channel configured. Cannot notify team.")
        return None

    # Build header with analyst mention if available
    assigned_to = f" — Asignado a: {analyst_name}" if analyst_name else ""
    header_text = f"Pregunta de cliente — Respuesta necesaria{assigned_to}"
    # Slack header text max 150 chars
    if len(header_text) > 150:
        header_text = header_text[:147] + "..."

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text, "emoji": True}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Proyecto:*\n{project_name}"},
                {"type": "mrkdwn", "text": f"*Cliente:*\n{client_name}"},
            ]
        },
    ]

    # Add analyst assignment section if specified
    if analyst_name:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Analista asignado:* {analyst_name} — por favor revisa y responde."}
        })

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*Pregunta:*\n{question_summary}"}
    })

    if original_snippet:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Email original:*\n```{original_snippet[:300]}```"}
        })

    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": "Responde en este canal o por email con etiqueta `client-followup-needed`. El sistema reenviará tu respuesta al cliente."}
        ]
    })

    analyst_tag = f" ({analyst_name})" if analyst_name else ""
    text = f"Pregunta de {client_name} — Proyecto: {project_name}. Respuesta necesaria{analyst_tag}."
    return send_message(target_channel, text, blocks)


# ─── Read Messages ──────────────────────────────────────────────────────────

def read_messages(channel_id, since_timestamp=None, limit=100):
    """
    Read messages from a Slack channel.

    Args:
        channel_id: Channel to read from
        since_timestamp: Only get messages after this Slack timestamp
        limit: Max messages to return

    Returns:
        List of message dicts with 'text', 'user', 'ts', 'thread_ts'
    """
    try:
        client = _get_client()
        kwargs = {"channel": channel_id, "limit": limit}
        if since_timestamp:
            kwargs["oldest"] = since_timestamp

        result = client.conversations_history(**kwargs)
        messages = result.get("messages", [])
        logger.info(f"Read {len(messages)} messages from {channel_id}")
        return messages
    except SlackApiError as e:
        logger.error(f"Slack read error: {e.response['error']}")
        return []
    except Exception as e:
        logger.error(f"Slack read unexpected error: {e}")
        return []


def get_channel_id(channel_name):
    """
    Get a channel ID from its name.

    Args:
        channel_name: Channel name without # (e.g., 'general')

    Returns:
        Channel ID string or None
    """
    try:
        client = _get_client()
        result = client.conversations_list(types="public_channel,private_channel", limit=200)
        for channel in result.get("channels", []):
            if channel["name"] == channel_name:
                return channel["id"]
        logger.warning(f"Channel '{channel_name}' not found")
        return None
    except SlackApiError as e:
        logger.error(f"Slack channel lookup error: {e.response['error']}")
        return None


if __name__ == "__main__":
    # Quick connectivity test
    logging.basicConfig(level=logging.INFO)
    if not SLACK_BOT_TOKEN:
        print("ERROR: SLACK_BOT_TOKEN not set in .env")
    else:
        try:
            client = _get_client()
            result = client.auth_test()
            print(f"SUCCESS: Connected to Slack as {result['user']} in workspace {result['team']}")
        except Exception as e:
            print(f"ERROR: Could not connect to Slack: {e}")
