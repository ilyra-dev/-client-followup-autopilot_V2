"""
Main daemon process for Client Follow-Up Autopilot.
Runs three independent cycles:
  1. Outbound: Check Notion → send/draft follow-ups (every 30 min)
  2. Client Inbound: Scan inbox for client responses (every 10 min)
  3. Team Inbound: Scan team email/Slack for relay messages (every 15 min)
  4. Learning: Compare drafts vs sent emails (every 30 min)

Also writes heartbeat for health monitoring.
"""

import logging
import logging.handlers
import os
import signal
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import schedule

from config import (
    SYSTEM_MODE,
    GMAIL_AUTH_MODE,
    GMAIL_DEFAULT_SENDER_EMAIL,
    POLL_INTERVAL_OUTBOUND,
    POLL_INTERVAL_TEAM_INBOUND,
    POLL_INTERVAL_CLIENT_INBOUND,
    HEARTBEAT_PATH,
    DAEMON_LOG_PATH,
    TMP_DIR,
    COUNTRY_TIMEZONES,
    BUSINESS_HOURS_START,
    BUSINESS_HOURS_END,
)

# ─── Logging Setup ──────────────────────────────────────────────────────────

TMP_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            DAEMON_LOG_PATH, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("daemon")

# ─── Graceful Shutdown ──────────────────────────────────────────────────────

_running = True


def _handle_signal(signum, frame):
    global _running
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ─── Heartbeat ──────────────────────────────────────────────────────────────

def _write_heartbeat():
    """Write current timestamp to heartbeat file for health monitoring."""
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat())
    except Exception:
        pass


# ─── Cycle Functions ────────────────────────────────────────────────────────

def outbound_cycle():
    """
    Flow 1: Check Notion for pending items and send/draft follow-ups.
    Respects business hours per client country.
    """
    logger.info("=== OUTBOUND CYCLE START ===")
    try:
        from check_pending_items import get_actionable_items
        from send_followup import send_followup_for_item
        from compute_next_followup import is_within_business_hours

        items = get_actionable_items()
        if not items:
            logger.info("No items need follow-up right now.")
            return

        success_count = 0
        fail_count = 0
        skipped_bh = 0
        for item in items:
            try:
                # Check business hours for client's country
                country = item.get("client_country", "")
                if country and not is_within_business_hours(country):
                    skipped_bh += 1
                    logger.info(f"⏭ {item['project_name']} — Outside business hours for {country}")
                    continue

                result = send_followup_for_item(item)
                if result.get("success"):
                    success_count += 1
                    logger.info(f"✓ {item['project_name']} — Stage {item['next_stage']} ({SYSTEM_MODE})")
                else:
                    fail_count += 1
                    logger.warning(f"✗ {item['project_name']} — {result.get('error', 'unknown error')}")
            except Exception as e:
                fail_count += 1
                logger.error(f"Error processing {item['project_name']}: {e}\n{traceback.format_exc()}")

        logger.info(f"Outbound cycle complete: {success_count} success, {fail_count} failed, {skipped_bh} skipped (business hours)")


    except Exception as e:
        logger.error(f"Outbound cycle error: {e}\n{traceback.format_exc()}")
    finally:
        logger.info("=== OUTBOUND CYCLE END ===")


def client_inbound_cycle():
    """
    Flow 3: Scan for client responses and process them.
    """
    logger.info("=== CLIENT INBOUND CYCLE START ===")
    try:
        from scan_client_inbox import scan_for_responses
        from process_client_response import process_response

        responses = scan_for_responses()
        if not responses:
            logger.info("No client responses detected.")
            return

        for response_data in responses:
            try:
                results = process_response(response_data)
                for r in results:
                    logger.info(f"Processed: {r.get('project', 'unknown')} — {r.get('action', 'unknown')} ({r.get('classification', '')})")
            except Exception as e:
                logger.error(f"Error processing response: {e}\n{traceback.format_exc()}")

        logger.info(f"Client inbound cycle complete: {len(responses)} responses processed")

    except Exception as e:
        logger.error(f"Client inbound cycle error: {e}\n{traceback.format_exc()}")
    finally:
        logger.info("=== CLIENT INBOUND CYCLE END ===")


def team_inbound_cycle():
    """
    Flow 2: Scan team email/Slack for messages to relay to clients.
    """
    logger.info("=== TEAM INBOUND CYCLE START ===")
    try:
        from scan_team_inbox import scan_team_emails
        from scan_slack_channels import scan_slack_for_followups
        from extract_and_forward import process_team_message

        # Scan team emails
        team_emails = scan_team_emails()
        for email_data in team_emails:
            try:
                result = process_team_message(email_data, source="email")
                logger.info(f"Team email processed: {result.get('project', 'unknown')} — {result.get('action', 'unknown')}")
            except Exception as e:
                logger.error(f"Error processing team email: {e}\n{traceback.format_exc()}")

        # Scan Slack channels
        slack_messages = scan_slack_for_followups()
        for msg_data in slack_messages:
            try:
                result = process_team_message(msg_data, source="slack")
                logger.info(f"Slack message processed: {result.get('project', 'unknown')} — {result.get('action', 'unknown')}")
            except Exception as e:
                logger.error(f"Error processing Slack message: {e}\n{traceback.format_exc()}")

        total = len(team_emails) + len(slack_messages)
        logger.info(f"Team inbound cycle complete: {total} messages processed")

    except ImportError:
        logger.warning("Team inbound tools not yet available. Skipping cycle.")
    except Exception as e:
        logger.error(f"Team inbound cycle error: {e}\n{traceback.format_exc()}")
    finally:
        logger.info("=== TEAM INBOUND CYCLE END ===")


def learning_cycle():
    """
    Run the learning engine to compare drafts vs sent emails.
    Only relevant in DRAFT mode, but runs in all modes to track metrics.
    """
    logger.info("=== LEARNING CYCLE START ===")
    try:
        from learning_engine import run_learning_cycle, get_mode_recommendation

        stats = run_learning_cycle()
        logger.info(f"Learning: processed={stats['processed']}, matched={stats['matched']}")

        rec = get_mode_recommendation()
        if rec["recommendation"] != SYSTEM_MODE:
            logger.info(f"MODE RECOMMENDATION: Consider switching to {rec['recommendation']} — {rec['reason']}")

    except Exception as e:
        logger.error(f"Learning cycle error: {e}\n{traceback.format_exc()}")
    finally:
        logger.info("=== LEARNING CYCLE END ===")


def daily_summary_cycle():
    """
    Genera y envía resumen diario al equipo CS.
    """
    logger.info("=== DAILY SUMMARY CYCLE START ===")
    try:
        from daily_summary import send_daily_summary
        send_daily_summary()
        logger.info("Resumen diario enviado exitosamente")
    except Exception as e:
        logger.error(f"Error en resumen diario: {e}\n{traceback.format_exc()}")
    finally:
        logger.info("=== DAILY SUMMARY CYCLE END ===")


def eod_summary_cycle():
    """
    Envía resumen de seguimientos del día al canal de Slack.
    Se ejecuta a las 5 PM hora Perú (22:00 UTC).
    """
    logger.info("=== EOD SLACK SUMMARY START ===")
    try:
        from daily_summary import send_eod_slack_summary
        result = send_eod_slack_summary()
        if result:
            logger.info("Resumen de fin de jornada enviado a Slack")
        else:
            logger.warning("No se pudo enviar resumen EOD a Slack")
    except Exception as e:
        logger.error(f"Error en resumen EOD: {e}\n{traceback.format_exc()}")
    finally:
        logger.info("=== EOD SLACK SUMMARY END ===")


# ─── Main Loop ──────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Client Follow-Up Autopilot — Daemon Starting")
    logger.info(f"Mode: {SYSTEM_MODE}")
    logger.info(f"Gmail auth: {GMAIL_AUTH_MODE}")
    logger.info(f"Default sender: {GMAIL_DEFAULT_SENDER_EMAIL}")
    logger.info(f"Multi-sender: {'YES' if GMAIL_AUTH_MODE == 'service_account' else 'NO (single sender)'}")
    logger.info(f"Outbound interval: {POLL_INTERVAL_OUTBOUND}s")
    logger.info(f"Client inbound interval: {POLL_INTERVAL_CLIENT_INBOUND}s")
    logger.info(f"Team inbound interval: {POLL_INTERVAL_TEAM_INBOUND}s")
    logger.info("=" * 60)

    # Initialize style data directory
    try:
        from style_store import init_style_data
        init_style_data()
    except Exception as e:
        logger.warning(f"Could not initialize style data: {e}")

    # Initialize team member cache
    try:
        import team_manager
        members = team_manager.refresh_cache()
        logger.info(f"Team cache initialized: {len(members)} active members")
    except Exception as e:
        logger.warning(f"Could not initialize team cache: {e}")

    # Schedule cycles
    schedule.every(POLL_INTERVAL_OUTBOUND).seconds.do(outbound_cycle)
    schedule.every(POLL_INTERVAL_CLIENT_INBOUND).seconds.do(client_inbound_cycle)
    schedule.every(POLL_INTERVAL_TEAM_INBOUND).seconds.do(team_inbound_cycle)
    schedule.every(POLL_INTERVAL_OUTBOUND).seconds.do(learning_cycle)

    # Daily summary — configurable time (default 13:00 UTC)
    daily_summary_time = os.environ.get("DAILY_SUMMARY_TIME", "13:00")
    schedule.every().day.at(daily_summary_time).do(daily_summary_cycle)
    logger.info(f"Resumen diario programado a las {daily_summary_time} UTC")

    # EOD Slack summary — 5 PM hora Perú = 22:00 UTC
    eod_time = os.environ.get("EOD_SUMMARY_TIME", "22:00")
    schedule.every().day.at(eod_time).do(eod_summary_cycle)
    logger.info(f"Resumen de fin de jornada (Slack) programado a las {eod_time} UTC (5 PM Perú)")

    # Run initial cycles immediately
    logger.info("Running initial cycles...")
    outbound_cycle()
    client_inbound_cycle()
    learning_cycle()

    # Start Slack webhook server in background thread (para botones interactivos)
    try:
        import threading
        from slack_webhook import start_webhook_server
        webhook_thread = threading.Thread(target=start_webhook_server, daemon=True)
        webhook_thread.start()
        logger.info(f"Slack webhook server iniciado en thread separado (puerto {os.environ.get('SLACK_WEBHOOK_PORT', '3000')})")
    except Exception as e:
        logger.warning(f"No se pudo iniciar webhook server (botones de Slack no funcionarán): {e}")

    # Main loop
    while _running:
        try:
            schedule.run_pending()
            _write_heartbeat()
            time.sleep(10)  # Check schedule every 10 seconds
        except Exception as e:
            logger.error(f"Main loop error: {e}\n{traceback.format_exc()}")
            time.sleep(30)  # Back off on error

    logger.info("Daemon stopped gracefully.")


if __name__ == "__main__":
    main()
