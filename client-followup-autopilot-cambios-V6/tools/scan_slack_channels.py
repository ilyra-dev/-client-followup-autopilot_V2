"""
Scan Slack channels for team messages that need to be relayed to clients.
Looks for messages with a trigger keyword or emoji reaction in monitored channels.

Trigger detection:
  - Messages containing ':followup:' or '@followup' keyword
  - Messages with a specific emoji reaction (e.g., :envelope:)
"""

import logging
import time
from pathlib import Path

import slack_client
from config import SLACK_REVIEW_CHANNEL, TMP_DIR

logger = logging.getLogger(__name__)

TRIGGER_KEYWORDS = [":followup:", "@followup", "followup:", "para cliente", "enviar a cliente"]
LAST_SCAN_FILE = TMP_DIR / "slack_last_scan_ts"


def _get_last_scan_ts():
    """Get the timestamp of the last Slack scan."""
    if LAST_SCAN_FILE.exists():
        try:
            return LAST_SCAN_FILE.read_text().strip()
        except OSError:
            pass
    return None


def _save_last_scan_ts(ts):
    """Save the current scan timestamp."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    try:
        LAST_SCAN_FILE.write_text(ts)
    except OSError as e:
        logger.error(f"Could not save scan timestamp: {e}")


def _has_trigger(text):
    """Check if a message text contains a trigger keyword."""
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in TRIGGER_KEYWORDS)


def scan_slack_for_followups(channel_ids=None):
    """
    Scan Slack channels for messages flagged for client follow-up.

    Args:
        channel_ids: List of channel IDs to scan. If None, uses SLACK_REVIEW_CHANNEL.

    Returns:
        List of dicts with:
            - text: Message text
            - user: Sender user ID
            - ts: Message timestamp
            - channel: Channel ID
            - thread_ts: Thread timestamp (if in a thread)
    """
    if channel_ids is None:
        if not SLACK_REVIEW_CHANNEL:
            logger.warning("No Slack channels configured for scanning")
            return []
        channel_ids = [SLACK_REVIEW_CHANNEL]

    last_ts = _get_last_scan_ts()
    latest_ts = last_ts or "0"
    results = []

    for channel_id in channel_ids:
        try:
            messages = slack_client.read_messages(channel_id, since_timestamp=last_ts, limit=50)
            for msg in messages:
                # Skip bot messages and already-processed messages
                if msg.get("bot_id") or msg.get("subtype"):
                    continue

                text = msg.get("text", "")
                ts = msg.get("ts", "")

                if _has_trigger(text):
                    results.append({
                        "text": text,
                        "user": msg.get("user", ""),
                        "ts": ts,
                        "channel": channel_id,
                        "thread_ts": msg.get("thread_ts"),
                        "source": "slack",
                    })

                # Track latest timestamp
                if ts > latest_ts:
                    latest_ts = ts

        except Exception as e:
            logger.error(f"Error scanning Slack channel {channel_id}: {e}")

    # Save latest scan timestamp
    if latest_ts != (last_ts or "0"):
        _save_last_scan_ts(latest_ts)

    logger.info(f"Found {len(results)} Slack messages flagged for follow-up")
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Scanning Slack for follow-up triggers...\n")

    messages = scan_slack_for_followups()
    if messages:
        for m in messages:
            print(f"  User: {m['user']}")
            print(f"  Text: {m['text'][:200]}")
            print(f"  Channel: {m['channel']}")
            print()
    else:
        print("No triggered messages found.")
