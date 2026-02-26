"""
Authorize a Gmail account for the Client Follow-Up Autopilot.

Creates a per-user OAuth2 token so the system can send emails and create
drafts from that user's Gmail account.

Usage:
    python authorize_gmail.py belsika@leaflatam.com
    python authorize_gmail.py                        # lists authorized accounts

The user must log in via browser and grant permissions.
Token is saved to tokens/{email}.json
"""

import json
import logging
import os
import sys
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config import GMAIL_CREDENTIALS_PATH, GMAIL_TOKENS_DIR

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.compose",
]


def list_authorized():
    """List all authorized Gmail accounts."""
    GMAIL_TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    tokens = list(GMAIL_TOKENS_DIR.glob("*.json"))

    if not tokens:
        print("\nNo accounts authorized yet.")
        print("Run: python authorize_gmail.py <email@leaflatam.com>")
        return

    print(f"\n{'Email':<35} {'Status':<12} {'Token File'}")
    print("-" * 75)

    for token_path in sorted(tokens):
        email = token_path.stem
        try:
            creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
            if creds.valid:
                status = "Active"
            elif creds.expired and creds.refresh_token:
                status = "Expired (auto-refreshable)"
            else:
                status = "Invalid (re-authorize)"
        except Exception:
            status = "Corrupt"
        print(f"  {email:<35} {status:<12} {token_path.name}")

    # Also check the default token.json
    default_token = GMAIL_TOKENS_DIR.parent / "token.json"
    if default_token.exists():
        print(f"\n  Default token (token.json) also exists — backward compat for cesar@leaflatam.com")


def authorize(email):
    """Run the OAuth2 flow for a specific email and save the token."""
    GMAIL_TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    token_path = GMAIL_TOKENS_DIR / f"{email.lower()}.json"

    if not GMAIL_CREDENTIALS_PATH.exists():
        print(f"ERROR: credentials.json not found at {GMAIL_CREDENTIALS_PATH}")
        print("Download it from Google Cloud Console > APIs & Services > Credentials")
        sys.exit(1)

    # Check if already authorized
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        if creds.valid or (creds.expired and creds.refresh_token):
            if creds.expired:
                creds.refresh(Request())
                with open(token_path, "w") as f:
                    f.write(creds.to_json())

            # Verify the token matches the expected email
            service = build("gmail", "v1", credentials=creds)
            profile = service.users().getProfile(userId="me").execute()
            actual_email = profile["emailAddress"].lower()

            if actual_email == email.lower():
                print(f"\n{email} is already authorized and active.")
                print(f"Token: {token_path}")
                return
            else:
                print(f"WARNING: Existing token is for {actual_email}, not {email}.")
                print("Re-authorizing...")

    # Run OAuth2 flow
    print(f"\nAuthorizing {email}...")
    print(f"A browser window will open. Please log in as {email} and grant permissions.\n")

    flow = InstalledAppFlow.from_client_secrets_file(
        str(GMAIL_CREDENTIALS_PATH), SCOPES
    )
    creds = flow.run_local_server(port=0)

    # Verify the authorized account matches
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    actual_email = profile["emailAddress"].lower()

    if actual_email != email.lower():
        print(f"\nERROR: You logged in as {actual_email} but we expected {email}.")
        print("Please try again and log in with the correct account.")
        sys.exit(1)

    # Save token
    with open(token_path, "w") as f:
        f.write(creds.to_json())

    print(f"\nSUCCESS: {email} authorized!")
    print(f"Token saved to: {token_path}")
    print(f"\nThe system will now create drafts and send emails from {email}'s Gmail account")
    print("for projects where they are assigned as 'Owner - Client Success'.")


def revoke(email):
    """Revoke and delete a user's token."""
    token_path = GMAIL_TOKENS_DIR / f"{email.lower()}.json"

    if not token_path.exists():
        print(f"No token found for {email}")
        return

    token_path.unlink()
    print(f"Token revoked for {email}")
    print(f"Deleted: {token_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        list_authorized()
        print("\nUsage:")
        print("  python authorize_gmail.py <email>      # authorize a new account")
        print("  python authorize_gmail.py --revoke <email>  # revoke access")
        sys.exit(0)

    if sys.argv[1] == "--revoke" and len(sys.argv) >= 3:
        revoke(sys.argv[2])
    else:
        authorize(sys.argv[1])
