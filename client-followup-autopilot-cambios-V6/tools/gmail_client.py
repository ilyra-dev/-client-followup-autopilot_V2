"""
Gmail API wrapper for Client Follow-Up Autopilot.
Supports two auth modes:
  - oauth2: Single-user OAuth2 Desktop App flow (legacy)
  - service_account: Google Workspace domain-wide delegation (multi-sender)

All public functions accept an optional from_email parameter to specify the sender.
"""

import base64
import logging
import os
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import (
    GMAIL_AUTH_MODE,
    GMAIL_CREDENTIALS_PATH,
    GMAIL_TOKEN_PATH,
    GMAIL_TOKENS_DIR,
    GMAIL_SERVICE_ACCOUNT_KEYFILE,
    GMAIL_DEFAULT_SENDER_EMAIL,
    GMAIL_SENDER_EMAIL,
)

logger = logging.getLogger(__name__)

# Gmail API scopes (gmail.send not needed for DRAFT mode)
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.compose",
]

# ─── Service Cache ─────────────────────────────────────────────────────────
# Cache Gmail service instances to avoid re-creating on every call
_service_cache = {}  # {email_key: (service, created_at)}
SERVICE_CACHE_TTL = 1800  # 30 minutes


def _get_service(sender_email=None):
    """
    Get a Gmail API service instance.
    Dispatches to OAuth2 or service account based on GMAIL_AUTH_MODE.

    Args:
        sender_email: Email of the sender. In oauth2 mode, if a per-user
                      token exists in tokens/{email}.json, uses that token
                      so drafts/sends happen from that user's account.
    """
    if GMAIL_AUTH_MODE == "service_account":
        return _get_service_sa(sender_email)
    return _get_service_oauth2(sender_email)


def _resolve_token_path(sender_email=None):
    """
    Resolve the token file path for a given sender email.

    Lookup order:
      1. tokens/{sender_email}.json  (per-user token)
      2. token.json                  (default / backward compat)

    Returns:
        (token_path, is_per_user) tuple
    """
    if sender_email:
        per_user_path = GMAIL_TOKENS_DIR / f"{sender_email.lower()}.json"
        if per_user_path.exists():
            return per_user_path, True
    return GMAIL_TOKEN_PATH, False


def _get_service_oauth2(sender_email=None):
    """
    Authenticate via OAuth2 Desktop App flow.

    If sender_email is provided and a per-user token exists at
    tokens/{email}.json, uses that token so the API call runs
    as that user (drafts appear in their Gmail, sends come from them).

    Falls back to the default token.json (original single-user behavior).
    """
    token_path, is_per_user = _resolve_token_path(sender_email)
    cache_key = f"oauth2:{token_path}"

    cached = _service_cache.get(cache_key)
    if cached and (time.time() - cached[1]) < SERVICE_CACHE_TTL:
        return cached[0]

    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            who = sender_email if is_per_user else "default"
            logger.info(f"Refreshing expired Gmail OAuth token ({who})")
            creds.refresh(Request())
            # Save refreshed token
            with open(token_path, "w") as f:
                f.write(creds.to_json())
        else:
            if is_per_user:
                raise FileNotFoundError(
                    f"Token for {sender_email} expired and cannot be refreshed. "
                    f"Re-run: python authorize_gmail.py {sender_email}"
                )
            # Default token: run interactive flow (only works for the machine owner)
            if not os.path.exists(GMAIL_CREDENTIALS_PATH):
                raise FileNotFoundError(
                    f"Gmail credentials not found at {GMAIL_CREDENTIALS_PATH}. "
                    "Download credentials.json from Google Cloud Console."
                )
            logger.info("Running Gmail OAuth flow (first time setup)")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(GMAIL_CREDENTIALS_PATH), SCOPES
            )
            creds = flow.run_local_server(port=0)
            with open(token_path, "w") as f:
                f.write(creds.to_json())
            logger.info(f"Gmail token saved to {token_path}")

    who = sender_email if is_per_user else "default"
    service = build("gmail", "v1", credentials=creds)
    _service_cache[cache_key] = (service, time.time())
    logger.info(f"Gmail OAuth2 service ready ({who})")
    return service


def _get_service_sa(sender_email=None):
    """
    Authenticate via service account with domain-wide delegation.
    Impersonates sender_email (or GMAIL_DEFAULT_SENDER_EMAIL as fallback).
    """
    from google.oauth2 import service_account

    target_email = sender_email or GMAIL_DEFAULT_SENDER_EMAIL
    if not target_email:
        raise ValueError("No sender email specified and GMAIL_DEFAULT_SENDER_EMAIL is empty")

    # Check cache
    cache_key = target_email.lower()
    cached = _service_cache.get(cache_key)
    if cached and (time.time() - cached[1]) < SERVICE_CACHE_TTL:
        return cached[0]

    keyfile = str(GMAIL_SERVICE_ACCOUNT_KEYFILE)
    if not os.path.exists(keyfile):
        raise FileNotFoundError(
            f"Service account keyfile not found at {keyfile}. "
            "Download it from Google Cloud Console > IAM > Service Accounts."
        )

    creds = service_account.Credentials.from_service_account_file(
        keyfile, scopes=SCOPES
    )
    delegated_creds = creds.with_subject(target_email)

    service = build("gmail", "v1", credentials=delegated_creds)
    _service_cache[cache_key] = (service, time.time())
    logger.info(f"Gmail service created for {target_email} (service_account mode)")
    return service


def _build_message(to, subject, body_html, cc=None, thread_id=None, from_email=None, attachments=None):
    """Build a MIME message for Gmail API, optionally with file attachments."""
    from email.mime.base import MIMEBase
    from email import encoders

    if attachments:
        msg = MIMEMultipart("mixed")
        # Add HTML body as a sub-part
        html_part = MIMEText(body_html, "html")
        msg.attach(html_part)

        # Add file attachments
        for att in attachments:
            maintype, subtype = att["mime_type"].split("/", 1) if "/" in att["mime_type"] else ("application", "octet-stream")
            part = MIMEBase(maintype, subtype)
            part.set_payload(att["data"])
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                "attachment",
                filename=att["filename"],
            )
            msg.attach(part)
    else:
        msg = MIMEMultipart("alternative")
        html_part = MIMEText(body_html, "html")
        msg.attach(html_part)

    msg["to"] = to
    msg["from"] = from_email or GMAIL_DEFAULT_SENDER_EMAIL
    msg["subject"] = subject
    if cc:
        msg["cc"] = cc

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    body = {"raw": raw}
    if thread_id:
        body["threadId"] = thread_id
    return body


# ─── Send Email ─────────────────────────────────────────────────────────────

def send_email(to, subject, body_html, cc=None, thread_id=None, from_email=None):
    """
    Send an email via Gmail API.

    Args:
        to: Recipient email address
        subject: Email subject line
        body_html: HTML body content
        cc: Optional CC email address(es)
        thread_id: Optional Gmail thread ID to reply in
        from_email: Sender email (service_account mode: impersonates this user)

    Returns:
        Dict with 'id', 'threadId', 'labelIds' on success, None on failure
    """
    try:
        service = _get_service(from_email)
        message = _build_message(to, subject, body_html, cc, thread_id, from_email)
        result = service.users().messages().send(
            userId="me", body=message
        ).execute()
        sender = from_email or GMAIL_DEFAULT_SENDER_EMAIL
        logger.info(f"Email sent from {sender} to {to} — Message ID: {result['id']}")
        return result
    except HttpError as e:
        logger.error(f"Gmail send error: {e}")
        return None
    except Exception as e:
        logger.error(f"Gmail send unexpected error: {e}")
        return None


# ─── Create Draft ───────────────────────────────────────────────────────────

def create_draft(to, subject, body_html, cc=None, thread_id=None, from_email=None, attachments=None):
    """
    Create a Gmail draft (not sent). Used in DRAFT mode.
    In service_account mode, the draft appears in the sender's Gmail account.

    Args:
        to: Recipient email address
        subject: Email subject line
        body_html: HTML body content
        cc: Optional CC email address(es)
        thread_id: Optional Gmail thread ID
        from_email: Sender email (service_account: draft appears in this user's account)
        attachments: Optional list of dicts with 'filename', 'data' (bytes), 'mime_type'

    Returns:
        Dict with draft 'id' and 'message' on success, None on failure
    """
    try:
        service = _get_service(from_email)
        message = _build_message(to, subject, body_html, cc, thread_id, from_email, attachments=attachments)
        draft = service.users().drafts().create(
            userId="me", body={"message": message}
        ).execute()
        sender = from_email or GMAIL_DEFAULT_SENDER_EMAIL
        logger.info(f"Draft created in {sender}'s account for {to} — Draft ID: {draft['id']}")
        return draft
    except HttpError as e:
        logger.error(f"Gmail draft creation error: {e}")
        return None
    except Exception as e:
        logger.error(f"Gmail draft unexpected error: {e}")
        return None


# ─── Read Inbox ─────────────────────────────────────────────────────────────

def read_inbox(query, max_results=50, from_email=None):
    """
    Read messages from Gmail inbox matching a query.

    Args:
        query: Gmail search query (e.g., 'is:unread label:INBOX')
        max_results: Maximum number of messages to return
        from_email: Whose inbox to read (service_account mode)

    Returns:
        List of message dicts with 'id', 'threadId', 'snippet', 'from', 'subject', 'body'
    """
    try:
        service = _get_service(from_email)
        results = service.users().messages().list(
            userId="me", q=query, maxResults=max_results
        ).execute()

        messages = results.get("messages", [])
        if not messages:
            return []

        detailed = []
        for msg_ref in messages:
            msg = service.users().messages().get(
                userId="me", id=msg_ref["id"], format="full"
            ).execute()
            detailed.append(_parse_message(msg))

        logger.info(f"Read {len(detailed)} messages matching query: {query}")
        return detailed
    except HttpError as e:
        logger.error(f"Gmail read error: {e}")
        return []
    except Exception as e:
        logger.error(f"Gmail read unexpected error: {e}")
        return []


def _parse_message(msg):
    """Extract useful fields from a Gmail message object."""
    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}

    body = ""
    payload = msg.get("payload", {})
    if payload.get("body", {}).get("data"):
        body = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
    elif payload.get("parts"):
        for part in payload["parts"]:
            if part.get("mimeType") in ("text/plain", "text/html") and part.get("body", {}).get("data"):
                body = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
                break

    return {
        "id": msg["id"],
        "threadId": msg["threadId"],
        "snippet": msg.get("snippet", ""),
        "from": headers.get("from", ""),
        "to": headers.get("to", ""),
        "subject": headers.get("subject", ""),
        "date": headers.get("date", ""),
        "body": body,
        "labelIds": msg.get("labelIds", []),
    }


def get_thread(thread_id, from_email=None):
    """
    Get all messages in a Gmail thread.

    Args:
        thread_id: Gmail thread ID
        from_email: Whose account to read from (service_account mode)

    Returns:
        List of parsed message dicts
    """
    try:
        service = _get_service(from_email)
        thread = service.users().threads().get(userId="me", id=thread_id).execute()
        return [_parse_message(msg) for msg in thread.get("messages", [])]
    except HttpError as e:
        logger.error(f"Gmail thread read error: {e}")
        return []


def mark_as_read(message_id, from_email=None):
    """Mark a message as read by removing UNREAD label."""
    try:
        service = _get_service(from_email)
        service.users().messages().modify(
            userId="me", id=message_id,
            body={"removeLabelIds": ["UNREAD"]}
        ).execute()
        logger.info(f"Marked message {message_id} as read")
        return True
    except HttpError as e:
        logger.error(f"Gmail mark-as-read error: {e}")
        return False


def get_draft(draft_id, from_email=None):
    """Get a specific draft by ID. Used by learning engine to compare draft vs sent."""
    try:
        service = _get_service(from_email)
        draft = service.users().drafts().get(userId="me", id=draft_id).execute()
        return _parse_message(draft.get("message", {}))
    except HttpError as e:
        logger.error(f"Gmail get draft error: {e}")
        return None


def send_draft(draft_id, from_email=None):
    """
    Envía un borrador (draft) existente en Gmail.
    Usado cuando el CS aprueba el envío desde Slack.

    Args:
        draft_id: ID del borrador en Gmail
        from_email: Email del remitente (para service_account mode)

    Returns:
        Dict con 'id' (message ID) y 'threadId' del mensaje enviado, o None
    """
    try:
        service = _get_service(from_email)
        result = service.users().drafts().send(
            userId="me",
            body={"id": draft_id}
        ).execute()

        message_id = result.get("id", "")
        thread_id = result.get("threadId", "")
        logger.info(f"Draft {draft_id} enviado exitosamente. Message ID: {message_id}")

        return {"id": message_id, "threadId": thread_id}
    except HttpError as e:
        if e.resp.status == 404:
            logger.error(f"Draft {draft_id} no encontrado — puede que ya fue enviado o eliminado")
        else:
            logger.error(f"Error al enviar draft {draft_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado al enviar draft {draft_id}: {e}")
        return None


def list_sent_messages(query="in:sent", max_results=20, from_email=None):
    """List recently sent messages. Used by learning engine to track what CS actually sent."""
    return read_inbox(query, max_results, from_email)


if __name__ == "__main__":
    # Quick connectivity test
    logging.basicConfig(level=logging.INFO)
    print(f"Gmail auth mode: {GMAIL_AUTH_MODE}")

    if not GMAIL_DEFAULT_SENDER_EMAIL:
        print("ERROR: GMAIL_DEFAULT_SENDER_EMAIL / GMAIL_SENDER_EMAIL not set in .env")
    elif GMAIL_AUTH_MODE == "service_account":
        keyfile = str(GMAIL_SERVICE_ACCOUNT_KEYFILE)
        if not os.path.exists(keyfile):
            print(f"ERROR: Service account keyfile not found at {keyfile}")
        else:
            try:
                service = _get_service(GMAIL_DEFAULT_SENDER_EMAIL)
                profile = service.users().getProfile(userId="me").execute()
                print(f"SUCCESS: Connected to Gmail as {profile['emailAddress']} (service_account mode)")
            except Exception as e:
                print(f"ERROR: Could not connect to Gmail: {e}")
    else:
        if not os.path.exists(GMAIL_CREDENTIALS_PATH):
            print(f"ERROR: credentials.json not found at {GMAIL_CREDENTIALS_PATH}")
            print("Download it from Google Cloud Console > APIs & Services > Credentials")
        else:
            try:
                service = _get_service()
                profile = service.users().getProfile(userId="me").execute()
                print(f"SUCCESS: Connected to Gmail as {profile['emailAddress']} (oauth2 mode)")
            except Exception as e:
                print(f"ERROR: Could not connect to Gmail: {e}")
