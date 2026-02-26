"""
Tests for style_store.py — style examples and metrics persistence.
"""

import json
import sys
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
os.environ.setdefault("SYSTEM_MODE", "DRAFT")

from style_store import (
    load_style_examples,
    save_style_example,
    load_metrics,
    save_metrics,
    init_style_data,
)


class TestStyleStore:
    """Tests for style example storage."""

    def test_load_examples_empty_dir(self, tmp_path):
        """Loading examples from empty directory returns empty list."""
        with patch("style_store.STYLE_DATA_DIR", tmp_path):
            result = load_style_examples()
            assert result == []

    def test_save_and_load_examples(self, tmp_path):
        """Saved examples should be loadable."""
        with patch("style_store.STYLE_DATA_DIR", tmp_path):
            save_style_example("Test email body", "ES", project_name="Proj1", stage=1)
            save_style_example("Another email", "EN", project_name="Proj2", stage=2)

            es_examples = load_style_examples(language="ES")
            assert len(es_examples) == 1
            assert "Test email body" in es_examples[0]

            en_examples = load_style_examples(language="EN")
            assert len(en_examples) == 1

            all_examples = load_style_examples()
            assert len(all_examples) == 2

    def test_max_examples_respected(self, tmp_path):
        """Should return at most max_examples."""
        with patch("style_store.STYLE_DATA_DIR", tmp_path):
            for i in range(10):
                save_style_example(f"Example {i}", "ES", stage=1)

            result = load_style_examples(language="ES", max_examples=3)
            assert len(result) == 3

    def test_examples_capped_at_30(self, tmp_path):
        """Total examples stored should be capped at 30."""
        with patch("style_store.STYLE_DATA_DIR", tmp_path):
            for i in range(40):
                save_style_example(f"Example {i}", "ES", stage=1)

            # Read raw file
            style_path = tmp_path / "style_examples.json"
            with open(style_path) as f:
                data = json.load(f)
            assert len(data["examples"]) <= 30


class TestMetrics:
    """Tests for learning metrics persistence."""

    def test_load_metrics_missing_file(self, tmp_path):
        """Loading metrics with no file returns defaults."""
        with patch("style_store.STYLE_DATA_DIR", tmp_path):
            metrics = load_metrics()
            assert metrics["total_drafts"] == 0
            assert metrics["approval_rate"] == 0.0

    def test_save_and_load_metrics(self, tmp_path):
        """Saved metrics should be loadable."""
        with patch("style_store.STYLE_DATA_DIR", tmp_path):
            metrics = {"total_drafts": 10, "sent_as_is": 8, "approval_rate": 0.8}
            save_metrics(metrics)

            loaded = load_metrics()
            assert loaded["total_drafts"] == 10
            assert loaded["approval_rate"] == 0.8

    def test_init_style_data_creates_files(self, tmp_path):
        """init_style_data should create all required files."""
        with patch("style_store.STYLE_DATA_DIR", tmp_path):
            init_style_data()

            assert (tmp_path / "drafts_log.jsonl").exists()
            assert (tmp_path / "sent_log.jsonl").exists()
            assert (tmp_path / "style_examples.json").exists()
            assert (tmp_path / "learning_metrics.json").exists()

    def test_init_does_not_overwrite_existing(self, tmp_path):
        """init_style_data should not overwrite existing files."""
        with patch("style_store.STYLE_DATA_DIR", tmp_path):
            # Write custom content
            metrics_path = tmp_path / "learning_metrics.json"
            metrics_path.write_text('{"total_drafts": 42}')

            init_style_data()

            data = json.loads(metrics_path.read_text())
            assert data["total_drafts"] == 42
