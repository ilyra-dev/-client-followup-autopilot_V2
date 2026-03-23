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


# ─── Lima Business Hours Helpers ────────────────────────────────────────────

def _is_lima_business_hours():
    """
    Returns True if the current moment is within Leaf's operating window:
    Monday–Friday, 09:00–17:59 PET (America/Lima, UTC-5).

    Used to gate the outbound follow-up cycle so drafts/emails are never
    generated on weekends or outside working hours.
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    lima_tz = ZoneInfo("America/Lima")
    now_lima = datetime.now(lima_tz)
    if now_lima.weekday() >= 5:  # 5=Saturday, 6=Sunday
        return False
    if now_lima.hour < 9 or now_lima.hour >= 18:
        return False
    return True


def _is_lima_weekday():
    """
    Returns True if today is a weekday (Mon–Fri) in Lima timezone.
    Used to gate scheduled reports so they don't fire on weekends.
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    lima_tz = ZoneInfo("America/Lima")
    now_lima = datetime.now(lima_tz)
    weekday = now_lima.weekday()
    if weekday >= 5:
        logger.info(f"Today is {now_lima.strftime('%A')} in Lima — skipping scheduled report (weekend)")
        return False
    return True


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
    # ── Business-hours gate (America/Lima, UTC-5) ──────────────────────────
    if not _is_lima_business_hours():
        logger.debug("Outbound cycle skipped — outside Lima business hours (Mon-Fri 09:00-18:00 PET)")
        return
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


def morning_report_cycle():
    """
    CRON 1 — Reporte matutino de seguimientos pendientes del día.
    Ejecuta a las 09:00 PET (14:00 UTC), Lunes a Viernes.
    Logs de auditoría incluidos para verificar ejecución correcta.
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    lima_now = datetime.now(ZoneInfo("America/Lima"))
    logger.info(f"=== MORNING REPORT CYCLE START === Lima time: {lima_now.strftime('%Y-%m-%d %H:%M %Z')}")

    # Weekend guard — schedule fires daily at 14:00 UTC, block weekends
    if not _is_lima_weekday():
        logger.info("Morning report skipped — weekend in Lima timezone")
        return

    try:
        from daily_summary import send_morning_report
        result = send_morning_report()
        if result:
            logger.info("✅ Reporte matutino enviado a Slack correctamente")
        else:
            logger.warning("⚠️ Reporte matutino no pudo enviarse a Slack")
    except Exception as e:
        logger.error(f"Error en reporte matutino: {e}\n{traceback.format_exc()}")
    finally:
        logger.info("=== MORNING REPORT CYCLE END ===")


def evening_report_cycle():
    """
    CRON 2 — Reporte vespertino de seguimientos realizados durante la jornada.
    Ejecuta a las 18:00 PET (23:00 UTC), Lunes a Viernes.
    Logs de auditoría incluidos para verificar ejecución correcta.
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    lima_now = datetime.now(ZoneInfo("America/Lima"))
    logger.info(f"=== EVENING REPORT CYCLE START === Lima time: {lima_now.strftime('%Y-%m-%d %H:%M %Z')}")

    # Weekend guard — schedule fires daily at 23:00 UTC, block weekends
    if not _is_lima_weekday():
        logger.info("Evening report skipped — weekend in Lima timezone")
        return

    try:
        from daily_summary import send_evening_report
        result = send_evening_report()
        if result:
            logger.info("✅ Reporte vespertino enviado a Slack correctamente")
        else:
            logger.warning("⚠️ Reporte vespertino no pudo enviarse a Slack")
    except Exception as e:
        logger.error(f"Error en reporte vespertino: {e}\n{traceback.format_exc()}")
    finally:
        logger.info("=== EVENING REPORT CYCLE END ===")


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

    # ── CRON 1: Morning report — 09:00 PET = 14:00 UTC, Mon-Fri ─────────────
    morning_time = os.environ.get("MORNING_REPORT_TIME", "14:00")
    schedule.every().day.at(morning_time).do(morning_report_cycle)
    logger.info(f"Reporte matutino programado: {morning_time} UTC (09:00 PET) — solo L-V")

    # ── CRON 2: Evening report — 18:00 PET = 23:00 UTC, Mon-Fri ──────────
    evening_time = os.environ.get("EVENING_REPORT_TIME", "23:00")
    schedule.every().day.at(evening_time).do(evening_report_cycle)
    logger.info(f"Reporte vespertino programado: {evening_time} UTC (18:00 PET) — solo L-V")

    # Run initial cycles immediately
    logger.info("Running initial cycles...")
    outbound_cycle()
    client_inbound_cycle()
    learning_cycle()

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
