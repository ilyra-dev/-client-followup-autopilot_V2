# Setup and Configuration

## Objective
Configure all external services and API connections required by the Client Follow-Up Autopilot.

## Prerequisites
- Python 3.10+ installed
- Access to Google Cloud Console
- Access to Slack workspace (admin)
- Notion integration permissions
- Anthropic API account

---

## Step 1: Install Python Dependencies

```bash
cd /Users/cesargranda/Documents/Client\ Success\ Leaf
pip install python-dotenv requests anthropic google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client slack-sdk schedule python-dateutil pytz
```

---

## Step 2: Notion Integration

### 2.1 Create Integration
1. Go to [Notion Integrations](https://www.notion.so/my-integrations)
2. Click "New integration"
3. Name: `Client Follow-Up Autopilot`
4. Select the workspace where the pending items database lives
5. Capabilities needed: Read content, Update content, Insert content
6. Copy the **Internal Integration Token** → paste in `.env` as `NOTION_API_KEY`

### 2.2 Share Database with Integration
1. Open the Notion database that tracks pending items
2. Click "..." → "Connections" → Add `Client Follow-Up Autopilot`
3. Copy the database ID from the URL: `https://notion.so/{workspace}/{DATABASE_ID}?v=...`
4. Paste in `.env` as `NOTION_DATABASE_ID`

### 2.3 Add Required Properties to Database
Add these properties to the existing database (if they don't exist):

| Property | Type | Values/Notes |
|---|---|---|
| Client Email | Email | — |
| Client Language | Select | Options: ES, EN, PT |
| Senior Contact Email | Email | — |
| Due Date | Date | — |
| Status | Select | Options: Pending, Reminded, Escalated, Received, Overdue, Paused, Question |
| Follow-Up Stage | Number | Default: 0 |
| Last Follow-Up Date | Date | — |
| Next Follow-Up Date | Date | — |
| Impact Description | Text | — |
| Manual Override | Checkbox | Default: unchecked |
| Follow-Up Log | Text | — |
| Delivery Team Email | Email | — |
| Delivery Team Slack Channel | Text | — |
| Gmail Thread ID | Text | — |
| Client Success | Select | Name of assigned CS member (must match CS Team Members DB) |
| Analista | Select | Name of assigned analyst (must match CS Team Members DB) |

### 2.4 CS Team Members Database (Required for multi-sender routing)

Create a separate Notion database called "CS Team Members" with these properties:

| Property | Type | Notes |
|---|---|---|
| Name | Title | Must match exactly with Client Success / Analista dropdown values |
| Email | Email | @leaflatam.com address |
| Role | Select | Options: admin, cs, analyst |
| Languages | Multi-select | Options: ES, EN, PT |
| Active | Checkbox | Default: checked |

Share this database with the integration (same as step 2.2). Copy its database ID → paste in `.env` as `NOTION_TEAM_DATABASE_ID`.

### 2.5 Verify Connection
```bash
cd tools && python notion_client.py
```
Expected: `SUCCESS: Connected to Notion. Found X records.`

---

## Step 3: Gmail / Google Workspace

### 3.1 Create Google Cloud Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create new project: `Client Follow-Up Autopilot`
3. Enable **Gmail API**: APIs & Services → Library → Search "Gmail API" → Enable

### 3.2 Create OAuth2 Credentials
1. Go to APIs & Services → Credentials
2. Click "Create Credentials" → "OAuth client ID"
3. Application type: **Desktop app**
4. Name: `Follow-Up Autopilot`
5. Download the JSON → save as `credentials.json` in the project root

### 3.3 Configure OAuth Consent Screen
1. Go to APIs & Services → OAuth consent screen
2. App name: `Client Follow-Up Autopilot`
3. Add scopes: `gmail.send`, `gmail.readonly`, `gmail.modify`, `gmail.compose`
4. Add test users (your email and CS team emails)

### 3.4 First-Time Auth Flow
```bash
cd tools && python gmail_client.py
```
This will open a browser window for OAuth authorization. After approving, `token.json` will be created automatically.

Expected: `SUCCESS: Connected to Gmail as user@company.com`

### 3.5 Set .env Variables
```
GMAIL_SENDER_EMAIL=cs@company.com
GMAIL_TEAM_LABEL=client-followup-needed
```

### 3.6 Create Gmail Label (for Flow 2)
In Gmail, create a label called `client-followup-needed`. Team members will apply this label to emails that need to be relayed to clients.

### 3.7 Multi-Sender: Service Account Setup (Optional — for sending FROM each CS member)

This enables the system to send emails impersonating each CS member via Google Workspace domain-wide delegation. **Skip this section if you want to use single-sender mode (all emails from one account).**

#### 3.7.1 Create Service Account
1. Go to Google Cloud Console → IAM & Admin → Service Accounts
2. Click "Create Service Account"
3. Name: `followup-autopilot-sender`
4. Click "Create and Continue" → Skip optional steps → "Done"
5. Click the new service account → "Keys" tab → "Add Key" → "Create new key" → JSON
6. Save the downloaded file as `service_account.json` in the project root

#### 3.7.2 Enable Domain-Wide Delegation
1. In Google Cloud Console → Service Account details → check "Enable Google Workspace Domain-wide Delegation"
2. Note the **Client ID** (numeric, shown in details)
3. Go to [Google Admin Console](https://admin.google.com) → Security → API Controls → Domain-wide Delegation
4. Click "Add new" and enter:
   - **Client ID**: (from step 2)
   - **OAuth Scopes**: `https://www.googleapis.com/auth/gmail.send,https://www.googleapis.com/auth/gmail.readonly,https://www.googleapis.com/auth/gmail.modify,https://www.googleapis.com/auth/gmail.compose`
5. Click "Authorize"

#### 3.7.3 Configure .env
```
GMAIL_AUTH_MODE=service_account
GMAIL_SERVICE_ACCOUNT_KEYFILE=service_account.json
GMAIL_WORKSPACE_DOMAIN=leaflatam.com
GMAIL_DEFAULT_SENDER_EMAIL=cesar@leaflatam.com
```

#### 3.7.4 Verify Multi-Sender
```bash
cd tools && python gmail_client.py
```
Expected: `SUCCESS: Connected via service_account as cesar@leaflatam.com`

**Important:** The Notion "CS Team Members" database must include all team members with correct Name, Email, and Role fields. Names must match exactly with the "Client Success" and "Analista" dropdown values in the projects database.

---

## Step 4: Slack Bot

### 4.1 Create Slack App
1. Go to [Slack API](https://api.slack.com/apps)
2. Click "Create New App" → "From scratch"
3. App name: `Follow-Up Autopilot`
4. Select your workspace

### 4.2 Configure Bot Permissions
Go to "OAuth & Permissions" → Add these **Bot Token Scopes**:
- `chat:write` — Send messages
- `channels:history` — Read channel messages
- `channels:read` — List channels
- `groups:history` — Read private channel messages (if needed)
- `reactions:read` — Read reactions (for trigger detection)

### 4.3 Install to Workspace
1. Click "Install to Workspace" → Authorize
2. Copy the **Bot User OAuth Token** (`xoxb-...`) → paste in `.env` as `SLACK_BOT_TOKEN`
3. Copy the **Signing Secret** from Basic Information → paste as `SLACK_SIGNING_SECRET`

### 4.4 Create Required Channels
1. Create `#followup-review` — Where draft emails are posted for CS review
2. Create `#delivery-notifications` — Where client response alerts go (or use existing)
3. Invite the bot to both channels: `/invite @Follow-Up Autopilot`
4. Get channel IDs (right-click channel → Copy link → ID is the last segment)
5. Set in `.env`:
   ```
   SLACK_REVIEW_CHANNEL=C0XXXXXXX
   SLACK_DEFAULT_CHANNEL=C0YYYYYYY
   ```

### 4.5 Verify Connection
```bash
cd tools && python slack_client.py
```
Expected: `SUCCESS: Connected to Slack as follow-up-autopilot in workspace MyWorkspace`

---

## Step 5: Anthropic (Claude API)

### 5.1 Get API Key
1. Go to [Anthropic Console](https://console.anthropic.com/)
2. Create API key
3. Copy → paste in `.env` as `ANTHROPIC_API_KEY`

### 5.2 Verify Connection
```bash
cd tools && python claude_client.py
```
Expected: `SUCCESS: Connected to Claude API. Response: Connection successful`

---

## Step 6: Company Configuration

Set in `.env`:
```
COMPANY_NAME=YourCompanyName
CS_TEAM_EMAIL=cs-team@company.com
```

---

## Step 7: Create Test Data

In the Notion database, create 3-5 test records:

1. **Test ES** — Spanish client, Due Date = yesterday, Status = Pending, Follow-Up Stage = 0
2. **Test EN** — English client, Due Date = 2 days ago, Status = Pending, Follow-Up Stage = 0
3. **Test PT** — Portuguese client, Due Date = tomorrow, Status = Pending, Follow-Up Stage = 0
4. **Test Override** — Any language, Manual Override = checked
5. **Test Received** — Any language, Status = Received (should be skipped)

---

## Step 8: Full Verification

Run each tool independently to confirm all connections:

```bash
cd tools
python config.py        # Should import without errors
python notion_client.py # Should show connected + record count
python gmail_client.py  # Should show connected email
python slack_client.py  # Should show connected bot
python claude_client.py # Should show successful response
```

All 4 should show SUCCESS. If any fail, check the corresponding .env variable and follow the error message guidance.

---

## Edge Cases and Troubleshooting

| Issue | Solution |
|---|---|
| Gmail OAuth token expired | Delete `token.json` and re-run `gmail_client.py` |
| Notion 401 error | Verify integration is shared with the database |
| Slack "not_in_channel" | Invite the bot to the channel with `/invite` |
| Claude API rate limit | Check your API tier at console.anthropic.com |
| `credentials.json` not found | Download from Google Cloud Console |
| Service account "unauthorized" | Verify domain-wide delegation scopes in admin.google.com |
| CS email not resolved | Check that Name in CS Team Members DB matches Client Success dropdown exactly |
| Multi-sender not working | Verify `GMAIL_AUTH_MODE=service_account` and `service_account.json` exists |

---

## Security Reminders
- NEVER commit `.env`, `credentials.json`, or `token.json` to git
- Notion integration should only access the specific database
- Slack bot should only be in required channels
- Gmail OAuth scopes are minimal (send, read, modify, compose)
