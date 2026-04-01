# Flow 1: Outbound Follow-Up Sequence

## Objective
Automatically identify pending client deliverables from Notion and generate follow-up communications using an escalating sequence (Day 1 → Day 3 → Day 7 → Day 14).

## Trigger
Daemon polling cycle runs every 30 minutes during business hours (8 AM - 6 PM in client's timezone).

## Required Inputs
- Notion database with pending items populated
- Gmail API connected and authenticated
- Slack bot connected (for DRAFT mode review channel)
- Claude API key active

## System Mode Behavior
| Mode | Action | CS Team Role |
|---|---|---|
| DRAFT | Creates Gmail draft + posts to Slack #review | Reviews, edits, sends manually |
| SEMI_AUTO | Sends email, notifies CS with cancel window (30 min) | Can cancel/edit within window |
| AUTO | Sends email immediately, logs to Notion | Monitors via daily summary |

---

## Execution Steps

### Step 1: Query Pending Items
```bash
cd tools && python check_pending_items.py
```
- Queries Notion for items where:
  - Status ∈ {Pending, Reminded, Escalated, Overdue}
  - Manual Override = unchecked
  - Follow-Up Stage < 4
  - Follow-up is due (based on schedule logic)
- Returns list of actionable items with full context

### Step 2: For Each Actionable Item
```bash
# Internally called by daemon, or manually:
cd tools && python send_followup.py
```
For each item, the system:
1. Determines recipient (primary contact for Stages 1-3, senior contact for Stage 4)
2. Loads style examples from learning engine (if available)
3. Generates email via Claude API with context: project, pending item, days overdue, impact
4. Falls back to HTML template if Claude API fails
5. Based on SYSTEM_MODE:
   - DRAFT: Creates Gmail draft + Slack notification
   - SEMI_AUTO/AUTO: Sends email
6. Updates Notion: Follow-Up Stage, Last Follow-Up Date, Next Follow-Up Date, Status, Follow-Up Log

### Step 3: Verify
After processing all items, verify in Notion that:
- Each processed item has updated Follow-Up Stage
- Follow-Up Log shows the action taken
- No items were processed that have Manual Override checked

---

## Escalation Schedule

| Stage | Timing | Tone | Recipient |
|---|---|---|---|
| 1 | Due date (or immediately if overdue) | Friendly reminder | Primary contact |
| 2 | 3 days after Stage 1 | Professional, more direct | Primary contact |
| 3 | 7 days after Stage 1 | Urgent, timeline impact | Primary contact |
| 4 | 14 days after Stage 1 | Formal escalation | Senior contact |

After Stage 4, no more automatic follow-ups. Item remains in "Escalated" status for CS manual handling.

---

## Edge Cases

### Missing Client Email
- **Action:** Skip item, log warning in Notion Follow-Up Log
- **Flag:** Add note "MISSING: Client Email" to log
- **Resolution:** CS fills in the email in Notion, item gets picked up next cycle

### Missing Senior Contact (Stage 4)
- **Action:** Send to primary contact with escalation language
- **Flag:** Log "WARNING: No senior contact — escalation sent to primary"
- **Resolution:** CS should add Senior Contact Email to Notion

### Claude API Failure
- **Action:** Use fallback HTML template (tools/templates/)
- **Log:** "Used fallback template — Claude API unavailable"
- **Resolution:** Check ANTHROPIC_API_KEY and API status

### Notion API Rate Limit (429)
- **Action:** Exponential backoff, max 3 retries per request
- **Log:** "Rate limited by Notion API — retrying"
- **Impact:** Some items may be delayed to next cycle, but no data loss

### Gmail Send/Draft Failure
- **Action:** Retry once after 30 seconds
- **Log:** "Gmail error — [error details]"
- **Resolution:** Check OAuth token; if expired, delete token.json and re-authenticate

### Item Already at Stage 4
- **Action:** Skip (no more automatic follow-ups)
- **Note:** CS team should handle these manually

### Manual Override Active
- **Action:** Skip entirely
- **Note:** CS can uncheck Manual Override in Notion to resume the sequence

### Business Hours Check
- **Action:** Outbound emails only sent during 8 AM - 6 PM in client's country timezone
- **Note:** Items due outside business hours are queued for the next business hours window

---

## Language Handling
- Each item has a `Client Language` property (ES, EN, PT)
- Claude generates the entire email in the specified language
- Fallback templates exist in all 3 languages
- Date formats follow language conventions:
  - ES/PT: DD/MM/YYYY
  - EN: MM/DD/YYYY

---

## Learning Integration (DRAFT Mode)
When running in DRAFT mode:
1. System creates Gmail draft and logs to `drafts_log.jsonl`
2. CS team reviews, potentially edits, and sends from Gmail
3. Learning engine detects the sent email via Gmail API
4. Compares draft vs sent version
5. Extracts style patterns and saves to `style_examples.json`
6. Future drafts incorporate learned style as few-shot examples

---

## Monitoring
- All actions logged to Notion Follow-Up Log (per item)
- Daemon activity logged to `.tmp/daemon.log`
- Daily summary sent to CS team (via daily_summary.py)
- Metrics tracked: items processed, emails sent/drafted, failures

## Tools Used
- `tools/check_pending_items.py` — Query Notion for actionable items
- `tools/send_followup.py` — Process a single follow-up (draft or send)
- `tools/compute_next_followup.py` — Date calculation logic
- `tools/claude_client.py` — Email generation
- `tools/gmail_client.py` — Email sending/drafting
- `tools/draft_manager.py` — Draft creation + Slack notification
- `tools/notion_client.py` — Database operations
- `tools/style_store.py` — Style examples for few-shot prompting
