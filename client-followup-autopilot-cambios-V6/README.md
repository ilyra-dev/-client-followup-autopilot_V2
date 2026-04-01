# Client Follow-Up Autopilot

Automated multi-channel client follow-up system for Client Success teams. Monitors Notion for overdue deliverables, generates contextual follow-up emails in 3 languages (ES/EN/PT) using Claude AI, and manages an escalating 4-stage sequence.

## Architecture

Built on the **WAT Framework** (Workflows, Agents, Tools):

```
workflows/          # Markdown SOPs — the instructions
tools/              # Python scripts — deterministic execution
CLAUDE.md           # Agent instructions — AI orchestration layer
```

### System Flows

| Flow | Description | Interval |
|------|-------------|----------|
| **Outbound** | Notion → pending items → generate email → send/draft | 30 min |
| **Client Inbound** | Scan inbox → classify response → update Notion | 10 min |
| **Team Inbound** | Team email/Slack → extract context → relay to client | 15 min |
| **Learning** | Compare drafts vs sent → improve style | 30 min |
| **Daily Summary** | Aggregate metrics → send report to CS team | Daily |

### Escalation Schedule

| Stage | Timing | Tone | Recipient |
|-------|--------|------|-----------|
| 1 | Due date | Friendly reminder | Primary contact |
| 2 | +3 days | Professional, direct | Primary contact |
| 3 | +7 days | Urgent, timeline impact | Primary contact |
| 4 | +14 days | Formal escalation | Senior contact |

### System Modes

| Mode | Behavior | Use When |
|------|----------|----------|
| `DRAFT` | Creates Gmail drafts + Slack review | Starting out, learning team style |
| `SEMI_AUTO` | Sends after cancel window (30 min) | >80% approval rate |
| `AUTO` | Sends immediately | >95% approval rate + CS approval |

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your API keys and configuration
```

### 3. Set up integrations
Follow the detailed guide: [workflows/setup_and_configuration.md](workflows/setup_and_configuration.md)

### 4. Validate setup
```bash
cd tools
python notion_client.py      # Test Notion connection
python gmail_client.py       # Test Gmail connection
python slack_client.py       # Test Slack connection
python claude_client.py      # Test Claude API
python validate_schema.py    # Validate Notion DB schema
python manage_team.py check  # Pre-flight check
```

### 5. Run the daemon
```bash
cd tools
python daemon_main.py
```

## Stack

- **Notion API** — Pending items database, team members, project data
- **Gmail API** — Email sending/drafting (OAuth2 or Service Account)
- **Claude API** — Email generation in 3 languages, response classification
- **Slack SDK** — Draft review notifications, team alerts
- **Python 3.10+** — Core runtime

## Team Management

```bash
cd tools
python manage_team.py              # Team dashboard
python manage_team.py authorize    # Authorize Gmail for a team member
python manage_team.py check        # Pre-flight readiness check
python authorize_gmail.py <email>  # Direct Gmail authorization
```

## Monitoring

```bash
python health_check.py             # Daemon health status
python health_check.py --logs      # With recent log entries
python daily_summary.py            # Generate summary report
python learning_engine.py          # Learning metrics + mode recommendation
```

## Testing

```bash
cd tests
python -m pytest -v                # Run all tests
python -m pytest test_compute.py   # Run specific test file
```

## File Structure

```
.env.example          # Environment variable template
.env                  # Your configuration (gitignored)
credentials.json      # Google OAuth credentials (gitignored)
token.json            # Gmail OAuth token (gitignored)
tokens/               # Per-user Gmail tokens (gitignored)
service_account.json  # Google service account key (gitignored)
tools/                # Python execution scripts
  ├── config.py                 # Centralized configuration
  ├── daemon_main.py            # Main daemon process
  ├── check_pending_items.py    # Query Notion for actionable items
  ├── send_followup.py          # Process follow-up (draft/send)
  ├── compute_next_followup.py  # Date/schedule logic
  ├── claude_client.py          # Claude API (email generation)
  ├── gmail_client.py           # Gmail API (send/draft/read)
  ├── notion_client.py          # Notion API (CRUD + helpers)
  ├── slack_client.py           # Slack API (messages)
  ├── team_manager.py           # CS team member routing
  ├── draft_manager.py          # Draft creation + Slack review
  ├── learning_engine.py        # Draft vs sent comparison
  ├── style_store.py            # Style examples + metrics
  ├── daily_summary.py          # Daily activity report
  ├── scan_client_inbox.py      # Detect client responses
  ├── process_client_response.py# Classify + route responses
  ├── scan_team_inbox.py        # Scan team emails
  ├── scan_slack_channels.py    # Scan Slack triggers
  ├── extract_and_forward.py    # Team → client relay
  ├── health_check.py           # Daemon health monitor
  ├── manage_team.py            # Team management CLI
  ├── authorize_gmail.py        # Gmail OAuth per-user
  ├── validate_schema.py        # Notion schema validation
  └── templates/                # Fallback HTML email templates
workflows/            # Markdown SOP documentation
  ├── setup_and_configuration.md
  ├── flow1_outbound_followup.md
  ├── flow2_inbound_team.md
  ├── flow3_inbound_client.md
  ├── learning_and_training.md
  ├── manual_override.md
  └── daemon_operations.md
tests/                # Unit tests
.tmp/                 # Temporary files (gitignored)
  ├── daemon.log
  ├── heartbeat
  └── style_data/
```

## KPIs

- **60%** reduction in CS time dedicated to follow-ups
- **40%** reduction in client response time
- Increased on-time information delivery rate
- Fewer projects blocked by missing client info

## License

Proprietary — Internal use only.
