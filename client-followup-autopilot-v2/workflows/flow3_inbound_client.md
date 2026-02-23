# Flow 3: Inbound from Client

## Objective
Detect when clients respond with requested information, notify the delivery team, and stop the follow-up sequence automatically.

## Trigger
Daemon polling cycle runs every 10 minutes.

## Required Inputs
- Gmail API connected (to read incoming emails)
- Notion database with Gmail Thread IDs populated (from outbound follow-ups)
- Slack bot connected (for delivery team notifications)
- Claude API key active (for response classification)

---

## Execution Steps

### Step 1: Scan for Client Responses
```bash
cd tools && python scan_client_inbox.py
```
Scans using two strategies:
1. **Thread tracking**: Checks all Gmail threads linked in Notion for new unread replies from client emails
2. **Email matching**: Checks inbox for new unread emails from known client email addresses

### Step 2: Classify Each Response
For each detected response, Claude API classifies it:

| Classification | Meaning | Action |
|---|---|---|
| `received` | Client sent the requested information | Stop sequence, notify delivery |
| `partial` | Some info but not complete | Pause sequence, flag for CS |
| `question` | Client is asking a question | Flag for CS, keep sequence |
| `unrelated` | Not related to the request | Log and ignore |

Confidence threshold: Classification must have ≥ 0.7 confidence to auto-act on "received". Below that, flags for CS review.

### Step 3: Process Based on Classification

**If received (confidence ≥ 0.7):**
1. Update Notion status → "Received"
2. Log in Follow-Up Log
3. Notify delivery team via Slack (with project name, client, and item)
4. Notify delivery team via email (with client's original message)
5. Mark Gmail message as read

**If partial:**
1. Update Notion status → "Paused"
2. Log in Follow-Up Log with "PARTIAL" flag
3. CS reviews and decides: request remaining info or accept

**If question:**
1. Log in Follow-Up Log with "QUESTION" flag
2. CS responds manually
3. Follow-up sequence continues (not paused)

**If unrelated:**
1. Log in Follow-Up Log
2. No action taken

---

## Edge Cases

### Client Responds to Old/Resolved Thread
- Item already has Status = "Received"
- **Action:** Ignored (item filtered out during scan)

### Multiple Notion Items Match Same Client Email
- Client may have multiple pending items
- **Action:** Response is processed against all matching items. Claude classifies per item.

### Classification Confidence Below 0.7
- **Action:** Log but don't auto-change status. Flag for CS review.

### Gmail API Error
- **Action:** Skip this cycle, retry next cycle (10 min)
- **Impact:** Minor delay in detection, no data loss

### Client Responds in Different Language
- **Action:** Claude handles multilingual classification
- **Note:** Log language mismatch for CS awareness

### Client Sends Attachment (Not Text)
- **Action:** System detects the email but may not analyze attachment content
- **Flag:** Log "Response may contain attachments — CS should verify"

---

## Tools Used
- `tools/scan_client_inbox.py` — Detect responses via thread tracking + email matching
- `tools/process_client_response.py` — Classify and process each response
- `tools/claude_client.py` — Response classification
- `tools/gmail_client.py` — Read inbox, mark as read
- `tools/notion_client.py` — Update status and logs
- `tools/slack_client.py` — Notify delivery team
