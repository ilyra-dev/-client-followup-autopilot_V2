"""
Style Store for Client Follow-Up Autopilot.
Manages learned communication style examples used as few-shot prompts for Claude.
Reads/writes from .tmp/style_data/
"""

import json
import logging
from pathlib import Path

from config import STYLE_DATA_DIR

logger = logging.getLogger(__name__)


def _ensure_dir():
    STYLE_DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_style_examples(language=None, max_examples=3):
    """
    Load style examples for Claude few-shot prompting.

    Args:
        language: Filter by language (ES/EN/PT). None = all.
        max_examples: Max examples to return

    Returns:
        List of example strings (email text that CS team approved/sent)
    """
    style_path = STYLE_DATA_DIR / "style_examples.json"
    if not style_path.exists():
        return []

    try:
        with open(style_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        examples = data.get("examples", [])
        if language:
            examples = [e for e in examples if e.get("language") == language]

        # Return the text of the most recent examples
        examples.sort(key=lambda x: x.get("added_at", ""), reverse=True)
        return [e["text"] for e in examples[:max_examples]]

    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error reading style examples: {e}")
        return []


def save_style_example(text, language, project_name="", stage=0, source="cs_edit"):
    """
    Save a new style example. Called by the learning engine when
    a CS-edited email is identified as a good style reference.

    Args:
        text: The email text (subject + body)
        language: ES/EN/PT
        project_name: Source project
        stage: Follow-up stage
        source: How this example was created ('cs_edit', 'manual', etc.)
    """
    _ensure_dir()
    style_path = STYLE_DATA_DIR / "style_examples.json"

    from datetime import datetime, timezone

    if style_path.exists():
        with open(style_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"examples": []}

    data["examples"].append({
        "text": text,
        "language": language,
        "project_name": project_name,
        "stage": stage,
        "source": source,
        "added_at": datetime.now(timezone.utc).isoformat(),
    })

    # Keep max 30 examples total (10 per language)
    if len(data["examples"]) > 30:
        data["examples"] = data["examples"][-30:]

    with open(style_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info(f"Style example saved ({language}, stage {stage})")


def load_metrics():
    """Load learning metrics."""
    metrics_path = STYLE_DATA_DIR / "learning_metrics.json"
    if not metrics_path.exists():
        return {
            "total_drafts": 0,
            "sent_as_is": 0,
            "sent_edited": 0,
            "discarded": 0,
            "approval_rate": 0.0,
            "edit_rate": 0.0,
            "common_edits": [],
        }

    try:
        with open(metrics_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {
            "total_drafts": 0,
            "sent_as_is": 0,
            "sent_edited": 0,
            "discarded": 0,
            "approval_rate": 0.0,
            "edit_rate": 0.0,
            "common_edits": [],
        }


def save_metrics(metrics):
    """Save learning metrics."""
    _ensure_dir()
    metrics_path = STYLE_DATA_DIR / "learning_metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    logger.info("Learning metrics updated")


def init_style_data():
    """Initialize empty style data files if they don't exist."""
    _ensure_dir()

    files = {
        "drafts_log.jsonl": "",
        "sent_log.jsonl": "",
        "style_examples.json": json.dumps({"examples": []}, indent=2),
        "learning_metrics.json": json.dumps({
            "total_drafts": 0,
            "sent_as_is": 0,
            "sent_edited": 0,
            "discarded": 0,
            "approval_rate": 0.0,
            "edit_rate": 0.0,
            "common_edits": [],
        }, indent=2),
    }

    for filename, content in files.items():
        filepath = STYLE_DATA_DIR / filename
        if not filepath.exists():
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info(f"Initialized {filepath}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_style_data()
    print(f"Style data directory: {STYLE_DATA_DIR}")
    print(f"Style examples: {load_style_examples()}")
    print(f"Metrics: {load_metrics()}")
