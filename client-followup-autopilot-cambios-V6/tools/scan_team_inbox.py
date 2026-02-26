"""
Scan team inbox for emails that need to be relayed to clients.
Looks for emails with a specific Gmail label (default: 'client-followup-needed').
Team members apply this label when they have info that should be sent to a client.
"""

import logging

import gmail_client
from config import GMAIL_TEAM_LABEL

logger = logging.getLogger(__name__)


def scan_team_emails(hours_back=4):
    """
    Scan Gmail for team emails labeled for client follow-up.

    Args:
        hours_back: How far back to look

    Returns:
        List of message dicts from Gmail (id, threadId, from, subject, body, etc.)
    """
    query = f"label:{GMAIL_TEAM_LABEL} is:unread newer_than:{hours_back}h"

    try:
        messages = gmail_client.read_inbox(query, max_results=30)
        logger.info(f"Found {len(messages)} team emails labeled '{GMAIL_TEAM_LABEL}'")
        return messages
    except Exception as e:
        logger.error(f"Error scanning team inbox: {e}")
        return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(f"Scanning for team emails with label: {GMAIL_TEAM_LABEL}\n")

    emails = scan_team_emails()
    if emails:
        for e in emails:
            print(f"  From: {e.get('from', 'unknown')}")
            print(f"  Subject: {e.get('subject', 'no subject')}")
            print(f"  Snippet: {e.get('snippet', '')[:100]}")
            print()
    else:
        print("No team emails found.")
