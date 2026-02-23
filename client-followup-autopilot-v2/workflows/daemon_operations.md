# Daemon Operations and Monitoring

## Objective
Guide for starting, stopping, monitoring, and troubleshooting the Follow-Up Autopilot daemon.

---

## Start the Daemon

```bash
cd /Users/cesargranda/Documents/Client\ Success\ Leaf/tools
python daemon_main.py
```

The daemon will:
1. Log startup info (mode, intervals)
2. Initialize style data directory
3. Run initial cycles immediately
4. Enter the main loop (polls on schedule)

### Run in Background
```bash
cd /Users/cesargranda/Documents/Client\ Success\ Leaf/tools
nohup python daemon_main.py > /dev/null 2>&1 &
echo $! > ../.tmp/daemon.pid
```

---

## Stop the Daemon

### Graceful (recommended)
```bash
kill $(cat /Users/cesargranda/Documents/Client\ Success\ Leaf/.tmp/daemon.pid)
```
The daemon catches SIGTERM and exits gracefully after completing the current operation.

### Immediate
```bash
kill -9 $(cat /Users/cesargranda/Documents/Client\ Success\ Leaf/.tmp/daemon.pid)
```
Use only if graceful shutdown is unresponsive.

### From Terminal
Press `Ctrl+C` — the daemon catches SIGINT for graceful shutdown.

---

## Check Health

```bash
cd /Users/cesargranda/Documents/Client\ Success\ Leaf/tools
python health_check.py
```

Output:
- **HEALTHY**: Daemon is running, heartbeat < 2 minutes old
- **UNHEALTHY**: Heartbeat is stale, daemon may be stuck
- **NOT_RUNNING**: No heartbeat file found

### With Logs
```bash
python health_check.py --logs
```

---

## View Logs

```bash
# Last 50 lines
tail -50 /Users/cesargranda/Documents/Client\ Success\ Leaf/.tmp/daemon.log

# Follow live
tail -f /Users/cesargranda/Documents/Client\ Success\ Leaf/.tmp/daemon.log

# Search for errors
grep "ERROR" /Users/cesargranda/Documents/Client\ Success\ Leaf/.tmp/daemon.log
```

---

## Change System Mode

1. Edit `.env`:
   ```
   SYSTEM_MODE=SEMI_AUTO   # or DRAFT or AUTO
   ```
2. Restart the daemon (it reads .env on startup)

### Mode Summary
| Mode | Behavior |
|---|---|
| DRAFT | Creates Gmail drafts, posts to Slack #review for CS approval |
| SEMI_AUTO | Sends automatically, CS gets notification with cancel window |
| AUTO | Sends immediately, CS gets daily summary |

---

## Adjust Polling Intervals

Edit `.env`:
```
POLL_INTERVAL_OUTBOUND=1800       # 30 min (seconds)
POLL_INTERVAL_TEAM_INBOUND=900    # 15 min
POLL_INTERVAL_CLIENT_INBOUND=600  # 10 min
```
Restart daemon for changes to take effect.

---

## Troubleshooting

### Daemon Won't Start
| Error | Solution |
|---|---|
| ModuleNotFoundError | Run `pip3 install -r requirements.txt` |
| NOTION_API_KEY not set | Fill in `.env` |
| credentials.json not found | Download from Google Cloud Console |

### Daemon Crashes During Operation
1. Check logs: `python health_check.py --logs`
2. Common causes:
   - Expired Gmail OAuth token → Delete `token.json`, restart
   - Invalid Notion database ID → Verify in `.env`
   - Network timeout → Daemon auto-retries, check connectivity
3. The daemon is designed to catch errors per cycle — a single failure shouldn't crash the entire process

### Duplicate Emails
- The system checks Follow-Up Stage before sending — if stage was already updated, it won't resend
- If duplicates occur, check if the daemon was restarted mid-cycle

### Missing Follow-Ups
- Check if Manual Override is active for the item
- Check if Status is "Received" or "Paused"
- Check if Due Date / Next Follow-Up Date is set correctly
- Check daemon logs for errors

---

## Monitoring Checklist (Daily)

1. ✅ Run `python health_check.py` — confirm HEALTHY
2. ✅ Review Slack #review channel for pending drafts (DRAFT mode)
3. ✅ Check `python daily_summary.py` for activity overview
4. ✅ Check `python learning_engine.py` for learning metrics and mode recommendation
5. ✅ Scan daemon.log for any ERROR entries
