"""
Centralized configuration for Client Follow-Up Autopilot.
Loads environment variables from .env and defines system constants.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ─── System Mode ────────────────────────────────────────────────────────────
# DRAFT: Generate Gmail drafts only, CS reviews and sends manually
# SEMI_AUTO: Send automatically with cancellation window
# AUTO: Send immediately, CS gets daily summary
SYSTEM_MODE = os.getenv("SYSTEM_MODE", "DRAFT").upper()
assert SYSTEM_MODE in ("DRAFT", "SEMI_AUTO", "AUTO"), (
    f"Invalid SYSTEM_MODE: {SYSTEM_MODE}. Must be DRAFT, SEMI_AUTO, or AUTO."
)

# ─── API Keys ───────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")  # Pendientes Client Success
NOTION_TEAM_DATABASE_ID = os.getenv("NOTION_TEAM_DATABASE_ID", "")  # CS Team Members
NOTION_PROJECTS_DB_ID = os.getenv("NOTION_PROJECTS_DB_ID", "")  # Proyectos
NOTION_TASKS_DB_ID = os.getenv("NOTION_TASKS_DB_ID", "")  # Pendientes Proyectos
NOTION_MEETINGS_DB_ID = os.getenv("NOTION_MEETINGS_DB_ID", "")  # Reuniones CS
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")

# ─── Gmail ──────────────────────────────────────────────────────────────────
# Auth mode: "oauth2" (single user, legacy) or "service_account" (multi-sender)
GMAIL_AUTH_MODE = os.getenv("GMAIL_AUTH_MODE", "oauth2")
GMAIL_DEFAULT_SENDER_EMAIL = os.getenv("GMAIL_DEFAULT_SENDER_EMAIL", os.getenv("GMAIL_SENDER_EMAIL", ""))
GMAIL_SENDER_EMAIL = GMAIL_DEFAULT_SENDER_EMAIL  # backward compat alias
GMAIL_TEAM_LABEL = os.getenv("GMAIL_TEAM_LABEL", "client-followup-needed")
GMAIL_WORKSPACE_DOMAIN = os.getenv("GMAIL_WORKSPACE_DOMAIN", "leaflatam.com")

# OAuth2 mode paths
GMAIL_CREDENTIALS_PATH = PROJECT_ROOT / "credentials.json"
GMAIL_TOKEN_PATH = PROJECT_ROOT / "token.json"  # default (backward compat)
GMAIL_TOKENS_DIR = PROJECT_ROOT / "tokens"       # per-user tokens: tokens/{email}.json

# Service account mode path
GMAIL_SERVICE_ACCOUNT_KEYFILE = PROJECT_ROOT / os.getenv("GMAIL_SERVICE_ACCOUNT_KEYFILE", "service_account.json")

# ─── Slack ──────────────────────────────────────────────────────────────────
SLACK_DEFAULT_CHANNEL = os.getenv("SLACK_DEFAULT_CHANNEL", "")
SLACK_REVIEW_CHANNEL = os.getenv("SLACK_REVIEW_CHANNEL", "")
SLACK_CS_CHANNEL = os.getenv("SLACK_CS_CHANNEL", "C0A3G6HK7C7")          # #client-success (privado)
SLACK_DELIVERY_CHANNEL = os.getenv("SLACK_DELIVERY_CHANNEL", "C0A6ZFUNJ68")  # #delivery-team (privado)
SLACK_LEADERS_CHANNEL = os.getenv("SLACK_LEADERS_CHANNEL", "C01CD0MJV7B")    # #líderes-proyectos (privado)

# ─── Polling Intervals (seconds) ────────────────────────────────────────────
POLL_INTERVAL_OUTBOUND = int(os.getenv("POLL_INTERVAL_OUTBOUND", "300"))
POLL_INTERVAL_TEAM_INBOUND = int(os.getenv("POLL_INTERVAL_TEAM_INBOUND", "300"))
POLL_INTERVAL_CLIENT_INBOUND = int(os.getenv("POLL_INTERVAL_CLIENT_INBOUND", "300"))

# ─── Semi-Auto Delay ────────────────────────────────────────────────────────
SEMI_AUTO_DELAY = int(os.getenv("SEMI_AUTO_DELAY", "1800"))  # 30 min

# ─── Company ────────────────────────────────────────────────────────────────
COMPANY_NAME = os.getenv("COMPANY_NAME", "")
CS_TEAM_EMAIL = os.getenv("CS_TEAM_EMAIL", "")

# ─── Follow-Up Schedule (business days between stages) ───────────────────────
# Every 48hrs (2 business days) if no client reply
FOLLOWUP_INTERVAL_BUSINESS_DAYS = int(os.getenv("FOLLOWUP_INTERVAL_BUSINESS_DAYS", "2"))
FOLLOWUP_SCHEDULE = {
    1: 0,   # Stage 1: Immediately when status matches
    2: 2,   # Stage 2: 2 business days after Stage 1
    3: 2,   # Stage 3: 2 business days after Stage 2
    4: 2,   # Stage 4: 2 business days after Stage 3
}

# ─── CC Always (names of people to always CC — emails resolved from Notion) ──
# Comma-separated names as they appear in Notion's Owner field
CC_ALWAYS_NAMES = os.getenv("CC_ALWAYS_NAMES", "César Montes, Diana Farje, Piero")

# ─── Language Configuration ─────────────────────────────────────────────────
SUPPORTED_LANGUAGES = ("ES", "EN", "PT")

LANGUAGE_DATE_FORMATS = {
    "ES": "%d/%m/%Y",
    "EN": "%m/%d/%Y",
    "PT": "%d/%m/%Y",
}

# Country → Timezone mapping (12 countries)
COUNTRY_TIMEZONES = {
    "México": "America/Mexico_City",
    "Colombia": "America/Bogota",
    "Chile": "America/Santiago",
    "Perú": "America/Lima",
    "Argentina": "America/Argentina/Buenos_Aires",
    "Brasil": "America/Sao_Paulo",
    "España": "Europe/Madrid",
    "Estados Unidos": "America/New_York",
    "Panamá": "America/Panama",
    "Ecuador": "America/Guayaquil",
    "República Dominicana": "America/Santo_Domingo",
    "Costa Rica": "America/Costa_Rica",
}

# Business hours for sending outbound emails
BUSINESS_HOURS_START = 8   # 8 AM
BUSINESS_HOURS_END = 18    # 6 PM

# ─── File Paths ─────────────────────────────────────────────────────────────
TMP_DIR = PROJECT_ROOT / ".tmp"
STYLE_DATA_DIR = TMP_DIR / "style_data"
DAEMON_LOG_PATH = TMP_DIR / "daemon.log"
HEARTBEAT_PATH = TMP_DIR / "heartbeat"

# Claude model for message generation
CLAUDE_MODEL = "claude-sonnet-4-5-20250929"

# ─── Notion Rate Limiting ───────────────────────────────────────────────────
NOTION_RATE_LIMIT_RPS = 3  # requests per second
GMAIL_SEND_DELAY = 1.0     # seconds between sends
SLACK_SEND_DELAY = 1.1     # seconds between messages
