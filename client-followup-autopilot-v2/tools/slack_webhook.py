"""
Slack Interactivity Webhook Handler para Client Follow-Up Autopilot.

Recibe y procesa las interacciones de los botones en Slack:
  - "Enviar ahora": Envía el draft de Gmail al cliente
  - "Editar en Gmail": Se maneja del lado del cliente (URL directa, no pasa por acá)

Ejecución:
  python slack_webhook.py
  → Levanta un servidor HTTP en el puerto configurado (default 3000)
  → Configurar en Slack: Request URL = https://tu-dominio.com/slack/interactivity
"""

import hashlib
import hmac
import json
import logging
import os
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
    """
    Verifica que la solicitud realmente proviene de Slack.

    Args:
        body: Cuerpo raw de la solicitud (bytes)
        timestamp: Header X-Slack-Request-Timestamp
        signature: Header X-Slack-Signature

    Returns:
        bool
    """
    if not SLACK_SIGNING_SECRET:
        logger.warning("SLACK_SIGNING_SECRET no configurado — omitiendo verificación")
        return True

    # Protección contra replay attacks (5 min)
    if abs(time.time() - int(timestamp)) > 300:
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
    Toma el draft de Gmail y lo envía.
    Actualiza el mensaje de Slack para confirmar.
    """
    try:
        value = json.loads(action["value"])
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error parseando valor del botón: {e}")
        return

    draft_id = value.get("draft_id")
    sender_email = value.get("sender_email", GMAIL_DEFAULT_SENDER_EMAIL)
    project_name = value.get("project_name", "—")
    client_name = value.get("client_name", "—")
    stage = value.get("stage", "?")

    user_name = payload.get("user", {}).get("real_name", payload.get("user", {}).get("name", "Alguien"))

    logger.info(f"Envío solicitado por {user_name}: Draft {draft_id} para {project_name}")

    # Enviar el draft
    try:
        send_result = gmail_client.send_draft(draft_id, from_email=sender_email)

        if send_result:
            message_id = send_result.get("id", "")
            logger.info(f"Draft {draft_id} enviado exitosamente. Message ID: {message_id}")

            # Actualizar el mensaje en Slack para mostrar confirmación
            _update_message_sent(
                payload=payload,
                project_name=project_name,
                client_name=client_name,
                stage=stage,
                user_name=user_name,
                message_id=message_id,
            )
        else:
            logger.error(f"Error al enviar draft {draft_id}")
            _respond_ephemeral(
                payload,
                f"❌ Error al enviar el draft para {project_name}. Verifica que el borrador aún exista en Gmail.",
            )
    except Exception as e:
        logger.error(f"Excepción al enviar draft {draft_id}: {e}")
        _respond_ephemeral(
            payload,
            f"❌ Error inesperado al enviar: {e}",
        )


def _update_message_sent(payload, project_name, client_name, stage, user_name, message_id):
    """
    Actualiza el mensaje original en Slack para mostrar que fue enviado.
    Reemplaza los botones con un mensaje de confirmación.
    """
    channel = payload.get("channel", {}).get("id")
    message_ts = payload.get("message", {}).get("ts")

    if not channel or not message_ts:
        return

    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "✅ Follow-Up Enviado", "emoji": True}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Proyecto:*\n{project_name}"},
                {"type": "mrkdwn", "text": f"*Cliente:*\n{client_name}"},
                {"type": "mrkdwn", "text": f"*Etapa:*\n{stage}"},
                {"type": "mrkdwn", "text": f"*Enviado por:*\n{user_name}"},
            ]
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"📨 Enviado a las {now_str} | Message ID: `{message_id[:20]}...`"}
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
    """Responde con un mensaje efímero (solo visible para el usuario que hizo click)."""
    response_url = payload.get("response_url")
    if not response_url:
        return

    import requests
    try:
        requests.post(response_url, json={
            "response_type": "ephemeral",
            "text": text,
        }, timeout=5)
    except Exception as e:
        logger.error(f"Error enviando respuesta efímera: {e}")


# ─── HTTP Handler ────────────────────────────────────────────────────────────

class SlackWebhookHandler(BaseHTTPRequestHandler):
    """Maneja las solicitudes POST de Slack interactivity."""

    def do_POST(self):
        # Leer cuerpo
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")

        # Verificar firma de Slack
        timestamp = self.headers.get("X-Slack-Request-Timestamp", "0")
        signature = self.headers.get("X-Slack-Signature", "")

        if not _verify_slack_signature(body, timestamp, signature):
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Invalid signature")
            return

        # Parsear payload
        try:
            parsed = parse_qs(body)
            payload_str = parsed.get("payload", [""])[0]
            payload = json.loads(payload_str)
        except (json.JSONDecodeError, IndexError) as e:
            logger.error(f"Error parseando payload: {e}")
            self.send_response(400)
            self.end_headers()
            return

        # Responder 200 inmediatamente (Slack espera respuesta en 3 seg)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

        # Procesar acción en background
        interaction_type = payload.get("type")

        if interaction_type == "block_actions":
            actions = payload.get("actions", [])
            for action in actions:
                action_id = action.get("action_id", "")

                if action_id == "send_draft":
                    _handle_send_draft(payload, action)
                elif action_id == "edit_draft_gmail":
                    # No necesita procesamiento — el botón tiene URL directa
                    user = payload.get("user", {}).get("real_name", "Alguien")
                    logger.info(f"{user} abrió borrador en Gmail para editar")
                else:
                    logger.info(f"Acción desconocida: {action_id}")

    def log_message(self, format, *args):
        """Redirigir logs del servidor HTTP al logger."""
        logger.info(f"HTTP: {format % args}")


# ─── Servidor ────────────────────────────────────────────────────────────────

def start_webhook_server():
    """Inicia el servidor de webhooks para interacciones de Slack."""
    server = HTTPServer(("0.0.0.0", WEBHOOK_PORT), SlackWebhookHandler)
    logger.info(f"Slack webhook server escuchando en puerto {WEBHOOK_PORT}")
    logger.info(f"Configura en Slack → Interactivity: https://tu-dominio.com/slack/interactivity")

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
