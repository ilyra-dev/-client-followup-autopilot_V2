# Learning and Training

## Objective
The system learns the CS team's communication style by comparing generated drafts against the emails CS actually sends. This feedback loop progressively improves email quality until the system can operate autonomously.

## How It Works

### Data Flow
```
Draft Generated → CS Reviews/Edits → Email Sent → Learning Engine Compares → Style Updated
```

1. **Draft Creation**: When the system generates a follow-up email (DRAFT mode), it saves the original text to `.tmp/style_data/drafts_log.jsonl`
2. **CS Review**: CS team sees the draft in Gmail and Slack #review channel. They may edit the subject, body, tone, etc.
3. **Sent Detection**: The learning engine polls Gmail sent folder to find emails matching pending drafts (by recipient + subject similarity)
4. **Comparison**: Using difflib, the engine calculates similarity between draft and sent version
5. **Classification**:
   - `sent_as_is` (similarity > 95%): Draft was good enough
   - `sent_edited` (similarity ≤ 95%): CS made changes → extract the CS version as a style example
   - `discarded` (no match after 48h): Draft was rejected entirely

### Style Examples
When CS edits a draft, the sent version is saved to `style_examples.json` as a few-shot example. These examples are injected into Claude's system prompt for future generations, so the AI progressively matches the team's preferred tone and structure.

---

## Execution

### Automatic (via Daemon)
The learning engine runs as part of the daemon cycle, typically every 30 minutes:
```python
# Inside daemon_main.py
learning_engine.run_learning_cycle()
```

### Manual
```bash
cd tools && python learning_engine.py
```

---

## Metrics Tracked

| Metric | Description | Location |
|---|---|---|
| `total_drafts` | Total drafts that have been processed | learning_metrics.json |
| `sent_as_is` | Drafts sent without edits | learning_metrics.json |
| `sent_edited` | Drafts sent after CS edits | learning_metrics.json |
| `discarded` | Drafts never sent (>48h) | learning_metrics.json |
| `approval_rate` | sent_as_is / total (target: >80%) | learning_metrics.json |
| `edit_rate` | sent_edited / total | learning_metrics.json |
| `avg_edit_similarity` | Average similarity of edited drafts | learning_metrics.json |

---

## Mode Transition Criteria

| Current Mode | Advance To | Criteria |
|---|---|---|
| DRAFT | SEMI_AUTO | `approval_rate` > 80% for 2+ weeks, 20+ drafts processed |
| SEMI_AUTO | AUTO | `approval_rate` > 95% for 1+ month + CS explicit approval |

### How to Change Mode
1. Check recommendation: `cd tools && python learning_engine.py`
2. If recommendation says ready, update `.env`:
   ```
   SYSTEM_MODE=SEMI_AUTO
   ```
3. Restart the daemon

**Important**: Never change to AUTO without explicit CS team approval.

---

## Data Files

```
.tmp/style_data/
├── drafts_log.jsonl        # Every draft generated (append-only)
├── sent_log.jsonl          # Matched sent emails with comparison data
├── style_examples.json     # Curated examples for Claude few-shot prompting
└── learning_metrics.json   # Aggregate metrics
```

### Initialize Data Files
```bash
cd tools && python style_store.py
```

---

## Edge Cases

### Draft Not Found in Sent
- Wait 48 hours before marking as discarded
- CS may have sent from a different account or forwarded

### Very Low Similarity (<30%)
- The CS team may have completely rewritten the email
- Still save as style example — the rewrite IS the style

### Multiple Sent Emails Match One Draft
- Engine picks the best match by subject similarity
- If ambiguous, logs a warning for review

### No Style Examples Yet (Cold Start)
- Claude uses its base prompts without few-shot examples
- First 10-20 drafts will have no style guidance
- Quality improves as examples accumulate

---

## Tools Used
- `tools/learning_engine.py` — Core comparison and learning logic
- `tools/style_store.py` — Read/write style examples and metrics
- `tools/gmail_client.py` — Read sent emails for comparison
