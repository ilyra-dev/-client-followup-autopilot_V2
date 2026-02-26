"""
Learning Engine for Client Follow-Up Autopilot.
Compares draft emails vs actually sent emails to learn CS team's communication style.

Flow:
1. Draft is created → logged in drafts_log.jsonl
2. CS edits and sends → detected via Gmail API
3. This engine compares draft vs sent → extracts style patterns
4. Patterns saved to style_examples.json for future Claude prompts
5. Metrics updated in learning_metrics.json
"""

import json
import logging
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import gmail_client
import team_manager
from config import GMAIL_AUTH_MODE, STYLE_DATA_DIR
from style_store import save_style_example, load_metrics, save_metrics

logger = logging.getLogger(__name__)


def _ensure_dir():
    STYLE_DATA_DIR.mkdir(parents=True, exist_ok=True)


def _read_jsonl(path):
    """Read a JSONL file, return list of dicts."""
    if not path.exists():
        return []
    entries = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return entries


def _write_jsonl(path, entries):
    """Write a list of dicts as JSONL."""
    with open(path, "w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _strip_html(html):
    """Simple HTML tag removal."""
    clean = re.sub(r"<[^>]+>", "", html)
    return re.sub(r"\s+", " ", clean).strip()


def _similarity(text1, text2):
    """Calculate text similarity ratio (0.0 to 1.0)."""
    if not text1 or not text2:
        return 0.0
    return SequenceMatcher(None, text1, text2).ratio()


# ─── Core Learning Loop ─────────────────────────────────────────────────────

def run_learning_cycle():
    """
    Main learning cycle. Called periodically by the daemon.

    1. Read pending drafts from drafts_log.jsonl
    2. For each pending draft, check if it was sent via Gmail
    3. Compare draft vs sent version
    4. Update draft status and log results
    5. Extract style patterns from good examples
    6. Update metrics

    Returns:
        Dict with 'processed', 'matched', 'unmatched' counts
    """
    drafts_path = STYLE_DATA_DIR / "drafts_log.jsonl"
    sent_path = STYLE_DATA_DIR / "sent_log.jsonl"

    drafts = _read_jsonl(drafts_path)
    sent_entries = _read_jsonl(sent_path)

    # Get recently sent emails from Gmail
    # In service_account mode, scan sent folder of each CS member
    recent_sent = []
    try:
        if GMAIL_AUTH_MODE == "service_account":
            cs_members = team_manager.get_cs_members()
            seen_ids = set()
            for member in cs_members:
                member_sent = gmail_client.list_sent_messages(
                    "in:sent newer_than:2d",
                    max_results=50,
                    from_email=member["email"],
                )
                for msg in member_sent:
                    msg_id = msg.get("id", "")
                    if msg_id not in seen_ids:
                        seen_ids.add(msg_id)
                        msg["_sender_email"] = member["email"]
                        recent_sent.append(msg)
            logger.info(f"Learning: scanned sent from {len(cs_members)} CS members, found {len(recent_sent)} messages")
        else:
            recent_sent = gmail_client.list_sent_messages("in:sent newer_than:2d", max_results=50)
    except Exception as e:
        logger.error(f"Failed to read sent emails: {e}")
        return {"processed": 0, "matched": 0, "unmatched": 0}

    # Build lookup of sent emails by recipient + subject similarity
    stats = {"processed": 0, "matched": 0, "unmatched": 0}
    updated_drafts = []

    for draft in drafts:
        if draft.get("status") != "pending_review":
            updated_drafts.append(draft)
            continue

        stats["processed"] += 1

        # Try to find the matching sent email
        match = _find_matching_sent(draft, recent_sent)

        if match:
            stats["matched"] += 1

            # Compare draft vs sent
            draft_text = draft.get("body_text", "")
            sent_text = _strip_html(match.get("body", match.get("snippet", "")))
            similarity = _similarity(draft_text, sent_text)

            if similarity > 0.95:
                # Sent as-is (no edits)
                draft["status"] = "sent_as_is"
                draft["similarity"] = similarity
                logger.info(f"Draft {draft['draft_id']} sent as-is (similarity: {similarity:.2f})")
            else:
                # CS edited the draft
                draft["status"] = "sent_edited"
                draft["similarity"] = similarity
                draft["sent_text"] = sent_text
                logger.info(f"Draft {draft['draft_id']} sent with edits (similarity: {similarity:.2f})")

                # Save the CS-edited version as a style example
                if sent_text and len(sent_text) > 20:
                    save_style_example(
                        text=f"Subject: {match.get('subject', '')}\n\n{sent_text}",
                        language=draft.get("language", "ES"),
                        project_name=draft.get("project_name", ""),
                        stage=draft.get("stage", 0),
                        source="cs_edit",
                    )

            # Log sent version
            sent_entries.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "draft_id": draft["draft_id"],
                "sent_message_id": match.get("id", ""),
                "sent_subject": match.get("subject", ""),
                "sent_text": sent_text,
                "original_text": draft_text,
                "similarity": similarity,
                "language": draft.get("language", ""),
                "stage": draft.get("stage", 0),
            })
        else:
            # Check if draft is old (> 48h) — likely discarded
            try:
                draft_time = datetime.fromisoformat(draft["timestamp"])
                age_hours = (datetime.now(timezone.utc) - draft_time).total_seconds() / 3600
                if age_hours > 48:
                    draft["status"] = "discarded"
                    stats["unmatched"] += 1
                    logger.info(f"Draft {draft['draft_id']} marked as discarded (>48h old)")
            except (ValueError, KeyError):
                pass

        updated_drafts.append(draft)

    # Write updated files
    _write_jsonl(drafts_path, updated_drafts)
    _write_jsonl(sent_path, sent_entries)

    # Update metrics
    _update_metrics(updated_drafts)

    logger.info(f"Learning cycle: processed={stats['processed']}, matched={stats['matched']}, unmatched={stats['unmatched']}")
    return stats


def _find_matching_sent(draft, sent_emails):
    """
    Find a sent email that matches a draft.
    Matches by recipient email and subject similarity.
    """
    draft_to = draft.get("to", "").lower()
    draft_subject = draft.get("subject", "").lower()

    best_match = None
    best_score = 0

    for sent in sent_emails:
        sent_to = sent.get("to", "").lower()
        sent_subject = sent.get("subject", "").lower()

        # Check recipient match
        if draft_to and draft_to in sent_to:
            # Check subject similarity
            subject_sim = _similarity(draft_subject, sent_subject)
            if subject_sim > 0.6 and subject_sim > best_score:
                best_match = sent
                best_score = subject_sim

    return best_match


def _update_metrics(drafts):
    """Update learning metrics based on all draft statuses."""
    metrics = load_metrics()

    total = len([d for d in drafts if d.get("status") != "pending_review"])
    sent_as_is = len([d for d in drafts if d.get("status") == "sent_as_is"])
    sent_edited = len([d for d in drafts if d.get("status") == "sent_edited"])
    discarded = len([d for d in drafts if d.get("status") == "discarded"])

    metrics["total_drafts"] = total
    metrics["sent_as_is"] = sent_as_is
    metrics["sent_edited"] = sent_edited
    metrics["discarded"] = discarded

    if total > 0:
        metrics["approval_rate"] = round(sent_as_is / total, 3)
        metrics["edit_rate"] = round(sent_edited / total, 3)
    else:
        metrics["approval_rate"] = 0.0
        metrics["edit_rate"] = 0.0

    # Calculate average similarity for edited drafts
    edited_sims = [d.get("similarity", 0) for d in drafts if d.get("status") == "sent_edited"]
    if edited_sims:
        metrics["avg_edit_similarity"] = round(sum(edited_sims) / len(edited_sims), 3)

    metrics["last_updated"] = datetime.now(timezone.utc).isoformat()

    save_metrics(metrics)
    return metrics


def get_mode_recommendation():
    """
    Based on learning metrics, recommend whether the system
    should advance to the next mode.

    Returns:
        Dict with 'current_recommendation', 'approval_rate', 'reason'
    """
    metrics = load_metrics()
    approval_rate = metrics.get("approval_rate", 0.0)
    total = metrics.get("total_drafts", 0)

    if total < 20:
        return {
            "recommendation": "DRAFT",
            "approval_rate": approval_rate,
            "reason": f"Not enough data yet ({total} drafts processed, need at least 20)",
        }

    if approval_rate >= 0.95:
        return {
            "recommendation": "AUTO",
            "approval_rate": approval_rate,
            "reason": f"Approval rate {approval_rate:.0%} — system has learned the team's style well",
        }

    if approval_rate >= 0.80:
        return {
            "recommendation": "SEMI_AUTO",
            "approval_rate": approval_rate,
            "reason": f"Approval rate {approval_rate:.0%} — ready for semi-autonomous with cancel window",
        }

    return {
        "recommendation": "DRAFT",
        "approval_rate": approval_rate,
        "reason": f"Approval rate {approval_rate:.0%} — keep learning from CS edits",
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=== Learning Engine ===\n")

    # Run a learning cycle
    stats = run_learning_cycle()
    print(f"\nCycle results: {stats}")

    # Get mode recommendation
    rec = get_mode_recommendation()
    print(f"\nMode recommendation: {rec['recommendation']}")
    print(f"Reason: {rec['reason']}")

    # Show current metrics
    metrics = load_metrics()
    print(f"\nMetrics: {json.dumps(metrics, indent=2)}")
