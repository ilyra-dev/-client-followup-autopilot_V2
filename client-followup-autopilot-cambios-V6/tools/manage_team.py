"""
Team management CLI for Client Follow-Up Autopilot.

Provides a single place to:
  - View team members, roles, and Gmail authorization status
  - See which accounts have active tokens
  - Authorize or revoke Gmail access for team members
  - Check system readiness

Usage:
    python manage_team.py                 # Show full team status dashboard
    python manage_team.py authorize       # Authorize a team member's Gmail
    python manage_team.py revoke <email>  # Revoke a member's Gmail token
    python manage_team.py check           # Pre-flight check for production readiness
"""

import logging
import os
import sys
from pathlib import Path

from google.oauth2.credentials import Credentials

import notion_client
import team_manager
from config import (
    GMAIL_TOKENS_DIR,
    GMAIL_TOKEN_PATH,
    GMAIL_DEFAULT_SENDER_EMAIL,
    GMAIL_CREDENTIALS_PATH,
    NOTION_TEAM_DATABASE_ID,
    NOTION_DATABASE_ID,
    SYSTEM_MODE,
)

logging.basicConfig(level=logging.WARNING)

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.compose",
]


def _check_token(email):
    """Check if a per-user OAuth2 token exists and is valid."""
    token_path = GMAIL_TOKENS_DIR / f"{email.lower()}.json"
    if not token_path.exists():
        # Check default token for the default sender
        if email.lower() == GMAIL_DEFAULT_SENDER_EMAIL.lower() and GMAIL_TOKEN_PATH.exists():
            return "Active (default token.json)"
        return None

    try:
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        if creds.valid:
            return "Active"
        elif creds.expired and creds.refresh_token:
            return "Expired (auto-refreshable)"
        else:
            return "Invalid (re-authorize needed)"
    except Exception:
        return "Corrupt"


def show_dashboard():
    """Show full team status with Gmail token info."""
    print("\n" + "=" * 70)
    print("  CLIENT FOLLOW-UP AUTOPILOT — Team Dashboard")
    print("=" * 70)
    print(f"\n  System Mode: {SYSTEM_MODE}")
    print(f"  Default Sender: {GMAIL_DEFAULT_SENDER_EMAIL}")

    members = team_manager.get_team_members(force_refresh=True)
    if not members:
        print("\n  WARNING: No team members found in Notion.")
        print(f"  Check NOTION_TEAM_DATABASE_ID in .env: {NOTION_TEAM_DATABASE_ID}")
        return

    # Group by role
    admins = [m for m in members if m["role"] == "admin"]
    cs_members = [m for m in members if m["role"] == "cs"]
    analysts = [m for m in members if m["role"] not in ("admin", "cs")]

    print(f"\n  Team: {len(members)} active members ({len(admins)} admin, {len(cs_members)} CS, {len(analysts)} analyst)")

    print(f"\n  {'Name':<25} {'Email':<30} {'Role':<10} {'Gmail Token':<28} {'Languages'}")
    print("  " + "-" * 110)

    for group_label, group in [("ADMINS", admins), ("CS TEAM", cs_members), ("ANALYSTS", analysts)]:
        if group:
            print(f"\n  -- {group_label} --")
            for m in sorted(group, key=lambda x: x["name"]):
                token_status = _check_token(m["email"]) or "Not authorized"
                langs = ", ".join(m["languages"]) if m["languages"] else "-"
                indicator = "OK" if token_status and "Invalid" not in token_status and "Corrupt" not in token_status else "!!"
                print(f"  [{indicator}] {m['name']:<23} {m['email']:<30} {m['role']:<10} {token_status:<28} {langs}")

    # Summary
    def _is_usable(email):
        status = _check_token(email)
        return status and "Invalid" not in status and "Corrupt" not in status

    authorized_count = sum(1 for m in members if _is_usable(m["email"]))
    needs_auth = [m for m in members if m["role"] in ("admin", "cs") and not _is_usable(m["email"])]

    print(f"\n  Gmail authorized: {authorized_count}/{len(members)}")
    if needs_auth:
        print(f"\n  ACTION NEEDED — These CS/admin members need Gmail authorization:")
        for m in needs_auth:
            print(f"    python authorize_gmail.py {m['email']}")

    print()


def run_authorize():
    """Interactive flow to authorize a team member."""
    members = team_manager.get_team_members(force_refresh=True)
    if not members:
        print("No team members found in Notion.")
        return

    # Show members without tokens
    unauth = [(m["name"], m["email"], m["role"]) for m in members if not _check_token(m["email"])]

    if not unauth:
        print("\nAll team members are already authorized!")
        return

    print("\nMembers needing Gmail authorization:\n")
    for i, (name, email, role) in enumerate(unauth, 1):
        print(f"  {i}. {name} ({email}) — {role}")

    print(f"\n  0. Enter email manually")
    choice = input("\nSelect (number): ").strip()

    if choice == "0":
        email = input("Email: ").strip()
    elif choice.isdigit() and 1 <= int(choice) <= len(unauth):
        _, email, _ = unauth[int(choice) - 1]
    else:
        print("Invalid choice.")
        return

    # Delegate to authorize_gmail.py
    os.system(f'cd "{Path(__file__).parent}" && python authorize_gmail.py {email}')


def run_check():
    """Pre-flight check for production readiness."""
    print("\n" + "=" * 50)
    print("  PRE-FLIGHT CHECK")
    print("=" * 50)

    checks = []

    # 1. Notion DBs configured
    if NOTION_DATABASE_ID:
        checks.append(("Pendientes CS DB", True, ""))
    else:
        checks.append(("Pendientes CS DB", False, "NOTION_DATABASE_ID not set"))

    if NOTION_TEAM_DATABASE_ID:
        checks.append(("Team Members DB", True, ""))
    else:
        checks.append(("Team Members DB", False, "NOTION_TEAM_DATABASE_ID not set"))

    # 2. Team members
    members = team_manager.get_team_members(force_refresh=True)
    checks.append(("Team members loaded", bool(members), f"{len(members)} members" if members else "0 members"))

    # 3. credentials.json exists
    checks.append(("credentials.json", GMAIL_CREDENTIALS_PATH.exists(), str(GMAIL_CREDENTIALS_PATH)))

    # 4. At least one Gmail token
    has_default = GMAIL_TOKEN_PATH.exists()
    GMAIL_TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    per_user_tokens = list(GMAIL_TOKENS_DIR.glob("*.json"))
    total_tokens = (1 if has_default else 0) + len(per_user_tokens)
    checks.append(("Gmail tokens", total_tokens > 0, f"{total_tokens} token(s)"))

    # 5. CS members with tokens (these are the ones that send emails)
    cs_with_token = []
    cs_without_token = []
    for m in members:
        if m["role"] in ("admin", "cs"):
            if _check_token(m["email"]):
                cs_with_token.append(m["name"])
            else:
                cs_without_token.append(m)

    if cs_without_token:
        checks.append(("CS/Admin tokens", False,
                       f"{len(cs_without_token)} need auth: {', '.join(m['name'] for m in cs_without_token)}"))
    else:
        checks.append(("CS/Admin tokens", True, f"All {len(cs_with_token)} authorized"))

    # 6. Actionable items with client email
    try:
        from check_pending_items import get_actionable_items
        items = get_actionable_items()
        missing_email = [i for i in items if not i.get("client_email")]
        checks.append(("Actionable items", True, f"{len(items)} ready, {len(missing_email)} missing email"))
    except Exception as e:
        checks.append(("Actionable items", False, str(e)))

    # Print results
    all_pass = True
    for name, passed, detail in checks:
        icon = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        detail_str = f" — {detail}" if detail else ""
        print(f"  [{icon}] {name}{detail_str}")

    if all_pass:
        print(f"\n  All checks passed! System is ready for {SYSTEM_MODE} mode.")
    else:
        print(f"\n  Some checks failed. Fix the issues above before going live.")

    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        show_dashboard()
    elif sys.argv[1] == "authorize":
        run_authorize()
    elif sys.argv[1] == "revoke" and len(sys.argv) >= 3:
        os.system(f'cd "{Path(__file__).parent}" && python authorize_gmail.py --revoke {sys.argv[2]}')
    elif sys.argv[1] == "check":
        run_check()
    else:
        print("Usage:")
        print("  python manage_team.py                 # Team dashboard")
        print("  python manage_team.py authorize       # Authorize a member's Gmail")
        print("  python manage_team.py revoke <email>  # Revoke Gmail access")
        print("  python manage_team.py check           # Pre-flight readiness check")
