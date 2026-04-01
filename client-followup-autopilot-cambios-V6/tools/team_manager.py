"""
CS Team Member Manager for Client Follow-Up Autopilot.
Reads team members from a Notion database, caches results,
and provides language-based routing for CC recipients.

If NOTION_TEAM_DATABASE_ID is not configured, falls back to CS_TEAM_EMAIL.
"""

import logging
import time

import notion_client
from config import NOTION_TEAM_DATABASE_ID, CS_TEAM_EMAIL

logger = logging.getLogger(__name__)

# ─── Cache ───────────────────────────────────────────────────────────────────

CACHE_TTL_SECONDS = 300  # 5 minutes

_cache = {
    "members": [],
    "last_refresh": 0.0,
}


def _is_cache_valid():
    """Check if the cache is still within its TTL."""
    return (time.time() - _cache["last_refresh"]) < CACHE_TTL_SECONDS


# ─── Core Functions ──────────────────────────────────────────────────────────

def get_team_members(force_refresh=False):
    """
    Get all active CS team members from the Notion database.
    Results are cached for 5 minutes.

    Args:
        force_refresh: If True, bypass cache and query Notion.

    Returns:
        List of dicts: [{name, email, role, languages}, ...]
    """
    if not NOTION_TEAM_DATABASE_ID:
        return []

    if not force_refresh and _is_cache_valid() and _cache["members"]:
        return _cache["members"]

    # Query Notion for active members
    filter_params = {
        "property": "Active",
        "checkbox": {"equals": True},
    }

    try:
        pages = notion_client.query_database(
            filter_params=filter_params,
            database_id=NOTION_TEAM_DATABASE_ID,
        )
    except Exception as e:
        logger.error(f"Failed to query team members from Notion: {e}")
        # Return stale cache if available
        if _cache["members"]:
            logger.warning("Returning stale team member cache")
            return _cache["members"]
        return []

    members = []
    for page in pages:
        name = notion_client.get_text_property(page, "Name")
        email = notion_client.get_email_property(page, "Email")
        role = notion_client.get_select_property(page, "Role") or "member"
        languages = notion_client.get_multi_select_property(page, "Languages")

        if not email:
            logger.warning(f"Team member '{name}' has no email, skipping")
            continue

        members.append({
            "name": name,
            "email": email,
            "role": role.lower(),
            "languages": [lang.upper() for lang in languages],
        })

    # Update cache
    _cache["members"] = members
    _cache["last_refresh"] = time.time()
    logger.info(f"Team cache refreshed: {len(members)} active members")

    return members


def get_cc_recipients(client_language):
    """
    Get comma-separated CC email string based on client language.

    Routing rules:
    - Admin role: always included (all notifications)
    - Member role: included only if their Languages includes client_language
    - Fallback: returns CS_TEAM_EMAIL if no team DB configured or no members found

    Args:
        client_language: Client's language code (ES, EN, PT)

    Returns:
        Comma-separated email string for CC field, or empty string
    """
    members = get_team_members()

    if not members:
        return CS_TEAM_EMAIL or ""

    lang = (client_language or "ES").upper()
    recipients = []

    for member in members:
        if member["role"] == "admin":
            recipients.append(member["email"])
        elif lang in member["languages"]:
            recipients.append(member["email"])

    if not recipients:
        return CS_TEAM_EMAIL or ""

    return ", ".join(recipients)


def get_daily_summary_recipients():
    """
    Get list of all active team member emails for daily summary.
    Daily summary goes to everyone regardless of language.

    Returns:
        List of email strings. Falls back to [CS_TEAM_EMAIL] if no team DB.
    """
    members = get_team_members()

    if not members:
        return [CS_TEAM_EMAIL] if CS_TEAM_EMAIL else []

    return [m["email"] for m in members]


def resolve_email(name):
    """
    Resolve a team member name to their email address.
    Used to map Notion 'Client Success' and 'Analista' dropdown values to emails.

    Args:
        name: Team member name (as shown in Notion select dropdown)

    Returns:
        Email string, or None if not found
    """
    if not name:
        return None

    members = get_team_members()
    name_lower = name.strip().lower()

    for member in members:
        if member["name"].strip().lower() == name_lower:
            return member["email"]

    logger.warning(f"Could not resolve email for team member: '{name}'")
    return None


def get_admins_cc():
    """
    Get comma-separated CC string of admin (area leader) emails only.
    Used when sending FROM a specific CS member — don't CC other CS members.

    Returns:
        Comma-separated email string, or empty string
    """
    members = get_team_members()
    admins = [m["email"] for m in members if m["role"] == "admin"]
    return ", ".join(admins) if admins else ""


def get_cs_members():
    """
    Get list of active CS members (role='cs' or role='member').
    Used for multi-inbox scanning in service_account mode.

    Returns:
        List of dicts with name, email, role, languages
    """
    members = get_team_members()
    return [m for m in members if m["role"] in ("cs", "member")]


def refresh_cache():
    """Force-refresh the team member cache from Notion."""
    _cache["last_refresh"] = 0.0
    return get_team_members(force_refresh=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if not NOTION_TEAM_DATABASE_ID:
        print("NOTION_TEAM_DATABASE_ID not configured in .env")
        print(f"Fallback: CS_TEAM_EMAIL = {CS_TEAM_EMAIL or '(empty)'}")
    else:
        members = get_team_members()
        print(f"Found {len(members)} active team members:\n")
        for m in members:
            print(f"  {m['name']} ({m['email']}) — Role: {m['role']}, Languages: {', '.join(m['languages'])}")

        print(f"\nCC for ES client: {get_cc_recipients('ES')}")
        print(f"CC for EN client: {get_cc_recipients('EN')}")
        print(f"CC for PT client: {get_cc_recipients('PT')}")
        print(f"Admins CC: {get_admins_cc()}")
        print(f"CS members: {[m['name'] for m in get_cs_members()]}")
        print(f"Daily summary recipients: {get_daily_summary_recipients()}")

        # Test resolve_email
        if members:
            test_name = members[0]["name"]
            print(f"\nResolve '{test_name}': {resolve_email(test_name)}")
