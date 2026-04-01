"""
Slack Interactivity Webhook Handler para Client Follow-Up Autopilot.

Recibe y procesa las interacciones de los botones en Slack:
  - "Enviar ahora": Envía el draft de Gmail al cliente y actualiza la tarjeta
  - "Editar en Gmail": Se maneja del lado del cliente (URL directa)

Se ejecuta como thread dentro del daemon (daemon_main.py lo inicia automáticamente).
También se puede ejecutar standalone: python slack_webhook.py
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs
from datetime import datetime, timezone

import gmail_client
import slack_client
from config import (
    SLACK_SIGNING_SECRET,
    SLACK_REVIEW_CHANNEL,
    GMAIL_DEFAULT_SENDER_EMAIL,
)

logger = logging.getLogger(__name__)

WEBHOOK_PORT = int(os.getenv("SLACK_WEBHOOK_PORT", "3000"))


# ─── Verificación de firma de Slack ──────────────────────────────────────────

def _verify_slack_signature(body, timestamp, signature):
    """Verifica que la solicitud realmente proviene de Slack."""
    if not SLACK_SIGNING_SECRET:
        logger.warning("SLACK_SIGNING_SECRET no configurado — omitiendo verificación")
        return True

    try:
        if abs(time.time() - int(timestamp)) > 300:
            return False
    except (ValueError, TypeError):
        return False

    sig_basestring = f"v0:{timestamp}:{body}".encode("utf-8")
    my_signature = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode("utf-8"),
        sig_basestring,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(my_signature, signature)


# ─── Procesadores de acciones ────────────────────────────────────────────────

def _handle_send_draft(payload, action):
    """
    Procesa el botón "Enviar ahora".
    1. Envía el draft de Gmail
    2. Actualiza la tarjeta en Slack con confirmación
    3. Actualiza Notion con el stage y log
    """
    try:
        value = json.loads(action["value"])
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error parseando valor del botón: {e}")
        _respond_ephemeral(payload, "❌ Error interno al procesar la acción.")
        return

    draft_id = value.get("draft_id")
    sender_email = value.get("sender_email", GMAIL_DEFAULT_SENDER_EMAIL)
    project_name = value.get("project_name", "—")
    client_name = value.get("client_name", "—")
    stage = value.get("stage", "?")

    user_info = payload.get("user", {})
    user_name = user_info.get("real_name", user_info.get("name", "Alguien"))

    logger.info(f"Envío solicitado por {user_name}: Draft {draft_id} para {project_name}")

    # 1. Enviar el draft
    try:
        send_result = gmail_client.send_draft(draft_id, from_email=sender_email)

        if send_result:
            message_id = send_result.get("id", "")
            thread_id = send_result.get("threadId", "")
            logger.info(f"Draft {draft_id} enviado exitosamente. Message ID: {message_id}")

            # 2. Actualizar tarjeta en Slack
            _update_message_sent(
                payload=payload,
                project_name=project_name,
                client_name=client_name,
                stage=stage,
                user_name=user_name,
                message_id=message_id,
            )

            # 3. Actualizar Notion
            _update_notion_after_send(
                draft_id=draft_id,
                stage=stage,
                message_id=message_id,
                thread_id=thread_id,
                user_name=user_name,
                project_name=project_name,
            )

        else:
            logger.error(f"Error al enviar draft {draft_id}")
            _respond_ephemeral(
                payload,
                f"❌ No se pudo enviar el draft para *{project_name}*.\n"
                f"Puede que el borrador ya fue enviado o eliminado de Gmail.",
            )
    except Exception as e:
        logger.error(f"Excepción al enviar draft {draft_id}: {e}")
        _respond_ephemeral(payload, f"❌ Error inesperado al enviar: {e}")


def _update_notion_after_send(draft_id, stage, message_id, thread_id, user_name, project_name):
    """Busca el draft en el log y actualiza Notion con la info del envío."""
    try:
        import notion_client
        from config import STYLE_DATA_DIR

        # Buscar el page_id en drafts_log.jsonl
        drafts_log = STYLE_DATA_DIR / "drafts_log.jsonl"
        page_id = None

        if drafts_log.exists():
            with open(drafts_log, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        entry = json.loads(line.strip())
                        if entry.get("draft_id") == draft_id:
                            page_id = entry.get("notion_page_id")
                            break
                    except json.JSONDecodeError:
                        continue

        if not page_id:
            logger.warning(f"No se encontró page_id para draft {draft_id} — Notion no actualizado")
            return

        # Actualizar propiedades en Notion
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        properties = {
            "Follow-Up Stage": notion_client.build_number(stage),
            "Last Follow-Up Date": notion_client.build_date(now_str),
            "Status": notion_client.build_status("En curso"),
        }

        if thread_id:
            properties["Gmail Thread ID"] = notion_client.build_rich_text(thread_id)

        notion_client.update_page(page_id, properties)

        log_msg = (
            f"Stage {stage} email enviado desde Slack por {user_name}. "
            f"Message ID: {message_id}"
        )
        notion_client.append_to_log(page_id, log_msg)

        logger.info(f"Notion actualizado: {project_name} → Stage {stage} (por {user_name})")

    except Exception as e:
        logger.error(f"Error actualizando Notion después de envío: {e}")


def _update_message_sent(payload, project_name, client_name, stage, user_name, message_id):
    """Reemplaza la tarjeta en Slack con confirmación de envío (sin botones)."""
    channel = payload.get("channel", {}).get("id")
    message_ts = payload.get("message", {}).get("ts")

    if not channel or not message_ts:
        return

    now_str = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    stage_emoji = {1: "📩", 2: "📨", 3: "🚨", 4: "🔴"}.get(stage, "📧")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "✅ Follow-Up Enviado Exitosamente", "emoji": True}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Proyecto:*\n{project_name}"},
                {"type": "mrkdwn", "text": f"*Cliente:*\n{client_name}"},
                {"type": "mrkdwn", "text": f"*Etapa:*\n{stage_emoji} {stage}"},
                {"type": "mrkdwn", "text": f"*Enviado por:*\n{user_name}"},
            ]
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn",
                 "text": f"📨 Enviado el {now_str} | Message ID: `{str(message_id)[:20]}...`"}
            ]
        },
    ]

    try:
        client = slack_client._get_client()
        client.chat_update(
            channel=channel,
            ts=message_ts,
            text=f"✅ Follow-up enviado para {client_name} — {project_name}",
            blocks=blocks,
        )
    except Exception as e:
        logger.error(f"Error actualizando mensaje de Slack: {e}")


def _respond_ephemeral(payload, text):
    """Responde con un mensaje efímero (solo visible para quien hizo click)."""
    response_url = payload.get("response_url")
    if not response_url:
        return

    import requests
    try:
        requests.post(response_url, json={
            "response_type": "ephemeral",
            "replace_original": False,
            "text": text,
        }, timeout=5)
    except Exception as e:
        logger.error(f"Error enviando respuesta efímera: {e}")


# ─── HTTP Handler ────────────────────────────────────────────────────────────

class SlackWebhookHandler(BaseHTTPRequestHandler):
    """Maneja solicitudes POST de Slack y GET para health check."""

    def do_GET(self):
        """Health check endpoint."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok", "service": "slack-webhook"}).encode())

    def do_POST(self):
        """Procesa interacciones de Slack."""
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")

        timestamp = self.headers.get("X-Slack-Request-Timestamp", "0")
        signature = self.headers.get("X-Slack-Signature", "")

        if not _verify_slack_signature(body, timestamp, signature):
            logger.warning("Solicitud con firma inválida rechazada")
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Invalid signature")
            return

        try:
            parsed = parse_qs(body)
            payload_str = parsed.get("payload", [""])[0]
            payload = json.loads(payload_str)
        except (json.JSONDecodeError, IndexError, KeyError) as e:
            logger.error(f"Error parseando payload: {e}")
            self.send_response(400)
            self.end_headers()
            return

        # Responder 200 inmediatamente (Slack timeout = 3 seg)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

        # Procesar en thread separado
        t = threading.Thread(target=_process_interaction, args=(payload,), daemon=True)
        t.start()

    def log_message(self, format, *args):
        logger.debug(f"HTTP: {format % args}")


def _process_interaction(payload):
    """Procesa una interacción de Slack (en thread separado)."""
    interaction_type = payload.get("type")
    user = payload.get("user", {}).get("real_name", "Alguien")

    if interaction_type == "block_actions":
        for action in payload.get("actions", []):
            action_id = action.get("action_id", "")

            if action_id == "send_draft":
                logger.info(f"Acción: {user} presionó 'Enviar ahora'")
                _handle_send_draft(payload, action)
            elif action_id == "edit_draft_gmail":
                logger.info(f"Acción: {user} abrió borrador en Gmail para editar")
            else:
                logger.info(f"Acción desconocida: {action_id} de {user}")
    else:
        logger.info(f"Tipo de interacción no manejado: {interaction_type}")


# ─── Servidor ────────────────────────────────────────────────────────────────

def start_webhook_server():
    """Inicia el servidor de webhooks para interacciones de Slack."""
    server = HTTPServer(("0.0.0.0", WEBHOOK_PORT), SlackWebhookHandler)
    logger.info(f"Slack webhook server escuchando en puerto {WEBHOOK_PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Webhook server detenido")
        server.server_close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    start_webhook_server()
