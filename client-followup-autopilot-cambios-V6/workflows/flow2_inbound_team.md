# Flow 2: Team-to-Client Follow-Up Relay

## Objective
When internal team members share information that needs to reach a client (reviews, checklists, deliverables, updates), the system extracts context and relays it professionally to the client.

## Trigger
Daemon polling cycle runs every 15 minutes.

## Input Sources

### Gmail
- Team members apply the label `client-followup-needed` to emails containing client-facing info
- System scans for unread emails with this label

### Slack
- Team members include trigger keywords in their messages:
  - `:followup:` or `@followup`
  - `para cliente` or `enviar a cliente`
- System monitors configured channels for these triggers

---

## Execution Steps

### Step 1: Scan for Team Messages
```bash
cd tools
python scan_team_inbox.py    # Scans Gmail label
python scan_slack_channels.py  # Scans Slack triggers
```

### Step 2: Extract Context
For each message, Claude API extracts:
- **project_name**: Which project this relates to
- **client_name**: Which client (if identifiable)
- **information_type**: Review, checklist, deliverable, update, etc.
- **summary**: Key information in 1-2 sentences
- **action_needed**: What the client should do with this info
- **confidence**: 0.0-1.0 extraction confidence

### Step 3: Match to Notion Record
System searches Notion for a page matching the extracted project name.

### Step 4: Generate Client Email
Claude generates a professional, client-facing email in the client's language:
- Uses friendly tone (Stage 1 equivalent)
- Includes project context and information summary
- Incorporates learned style examples (if available)

### Step 5: Draft or Send
Based on SYSTEM_MODE:
- **DRAFT**: Creates Gmail draft + Slack notification in #review
- **SEMI_AUTO/AUTO**: Sends email directly (CC to CS team)

### Step 6: Log and Cleanup
- Logs relay action in Notion Follow-Up Log
- Marks source email as read (Gmail)
- Records timestamp for Slack (avoid reprocessing)

---

## Edge Cases

### Cannot Identify Project (confidence < 0.5)
- **Action:** Do not send. Flag for CS review.
- **Log:** "LOW CONFIDENCE: Could not reliably identify project"

### No Notion Record Found
- **Action:** Do not send. Log the attempt.
- **Resolution:** CS creates the Notion record or adjusts the project name

### No Client Email in Notion
- **Action:** Do not send. Flag for CS.
- **Resolution:** CS adds client email to Notion

### Multiple Projects Mentioned
- **Action:** Claude extracts the primary project. If ambiguous, flags for CS.
- **Future:** Could create separate follow-ups per project

### Attachments in Team Email
- **Action:** References attachments in the relay email text
- **Note:** Direct attachment forwarding requires additional handling (not in V1)

### Duplicate Messages
- **Gmail:** Marked as read after processing (won't be picked up again)
- **Slack:** Timestamp tracking prevents reprocessing

---

## Tools Used
- `tools/scan_team_inbox.py` — Scan Gmail for labeled team emails
- `tools/scan_slack_channels.py` — Scan Slack for trigger messages
- `tools/extract_and_forward.py` — Extract context + draft/send to client
- `tools/claude_client.py` — Context extraction + email generation
- `tools/gmail_client.py` — Read/send/draft emails
- `tools/draft_manager.py` — Create drafts + Slack review notification
- `tools/notion_client.py` — Find records + log actions
