"""
Compute next follow-up date based on business-day intervals.
Pure logic — no API calls.

Schedule:
  Stage 1: Immediately when item is actionable (status matches)
  Stage 2+: Every 2 business days (48hrs, Mon-Fri) after previous follow-up
  If the computed date falls on a weekend, it moves to the next Monday.
"""

from datetime import datetime, timedelta

from config import FOLLOWUP_SCHEDULE, FOLLOWUP_INTERVAL_BUSINESS_DAYS


def _add_business_days(start_date, business_days):
    """
    Add N business days to a date. Skips weekends (Sat=5, Sun=6).
    """
    current = start_date
    days_added = 0
    while days_added < business_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days_added += 1
    return current


def _next_business_day(date):
    """If date falls on a weekend, move to the next Monday."""
    while date.weekday() >= 5:
        date += timedelta(days=1)
    return date


def compute_next_followup_date(current_stage, last_followup_date=None, due_date=None):
    """
    Compute when the next follow-up should be sent.

    Args:
        current_stage: Current follow-up stage (0-4)
        last_followup_date: Date of the last follow-up sent
        due_date: Original due date (kept for compat, not used for scheduling)

    Returns:
        datetime of next follow-up, or None if all stages are done
    """
    if current_stage >= 4:
        return None

    next_stage = current_stage + 1

    if isinstance(due_date, str) and due_date:
        due_date = datetime.strptime(due_date[:10], "%Y-%m-%d")
    if isinstance(last_followup_date, str) and last_followup_date:
        last_followup_date = datetime.strptime(last_followup_date[:10], "%Y-%m-%d")

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if next_stage == 1:
        # First follow-up: send immediately (next business day if weekend)
        return _next_business_day(today)

    # Stages 2-4: N business days after previous follow-up
    if not last_followup_date:
        return _next_business_day(today)

    interval = FOLLOWUP_SCHEDULE.get(next_stage, FOLLOWUP_INTERVAL_BUSINESS_DAYS)
    return _add_business_days(last_followup_date, interval)


def is_followup_due(current_stage, last_followup_date=None, due_date=None):
    """
    Check if a follow-up is due today or overdue.

    Returns:
        Tuple of (is_due: bool, next_stage: int, next_date: datetime or None)
    """
    if current_stage >= 4:
        return False, None, None

    next_date = compute_next_followup_date(current_stage, last_followup_date, due_date)
    if not next_date:
        return False, None, None

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Only send on business days (Mon-Fri)
    if today.weekday() >= 5:
        return False, current_stage + 1, next_date

    is_due = next_date <= today
    next_stage = current_stage + 1

    return is_due, next_stage, next_date


def days_overdue(due_date):
    """Calculate how many days past the due date."""
    if isinstance(due_date, str) and due_date:
        due_date = datetime.strptime(due_date[:10], "%Y-%m-%d")
    if not due_date:
        return 0

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    delta = (today - due_date).days
    return max(0, delta)


def is_business_day(date=None):
    """Check if a date (default: today) is a business day (Mon-Fri)."""
    if date is None:
        date = datetime.now()
    return date.weekday() < 5


if __name__ == "__main__":
    from datetime import datetime, timedelta

    print("=== compute_next_followup.py — Self Test ===\n")

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Test Stage 0 → 1 (immediate)
    is_due, stage, date = is_followup_due(0)
    print(f"Stage 0: is_due={is_due}, next_stage={stage}, date={date}")

    # Test Stage 1 → 2 (2 biz days after)
    four_days_ago = today - timedelta(days=4)
    is_due, stage, date = is_followup_due(1, last_followup_date=four_days_ago)
    print(f"Stage 1, last 4 days ago: is_due={is_due}, next_stage={stage}, date={date}")

    # Test Stage 4 (complete)
    is_due, stage, date = is_followup_due(4)
    print(f"Stage 4: is_due={is_due}, next_stage={stage}")
    assert not is_due and stage is None

    # Test business day math
    monday = datetime(2026, 2, 23)
    result = _add_business_days(monday, 2)
    print(f"\n2 biz days after Mon 2/23: {result.strftime('%A %m/%d')}")
    assert result.weekday() == 2  # Wednesday

    friday = datetime(2026, 2, 27)
    result = _add_business_days(friday, 2)
    print(f"2 biz days after Fri 2/27: {result.strftime('%A %m/%d')}")
    assert result.weekday() == 1  # Tuesday

    print("\nAll tests passed!")
