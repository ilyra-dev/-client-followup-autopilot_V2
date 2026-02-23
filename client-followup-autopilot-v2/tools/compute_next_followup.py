"""
Compute next follow-up date based on the escalation schedule.
Pure logic — no API calls.

Schedule:
  Stage 1 (Day 1):  On due date or immediately if overdue
  Stage 2 (Day 3):  3 days after Stage 1
  Stage 3 (Day 7):  4 days after Stage 2
  Stage 4 (Day 14): 7 days after Stage 3
"""

from datetime import datetime, timedelta

from config import FOLLOWUP_SCHEDULE


def compute_next_followup_date(current_stage, last_followup_date=None, due_date=None):
    """
    Compute when the next follow-up should be sent.

    Args:
        current_stage: Current follow-up stage (0-4)
            0 = no follow-up sent yet
            1-3 = previous stages completed
            4 = all stages completed (no more follow-ups)
        last_followup_date: Date of the last follow-up sent (datetime or str 'YYYY-MM-DD')
        due_date: Original due date of the pending item (datetime or str 'YYYY-MM-DD')

    Returns:
        datetime of next follow-up, or None if all stages are done
    """
    if current_stage >= 4:
        return None  # All stages completed

    next_stage = current_stage + 1

    if isinstance(due_date, str) and due_date:
        due_date = datetime.strptime(due_date[:10], "%Y-%m-%d")
    if isinstance(last_followup_date, str) and last_followup_date:
        last_followup_date = datetime.strptime(last_followup_date[:10], "%Y-%m-%d")

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if next_stage == 1:
        # First follow-up: on due date or immediately if overdue
        if due_date and due_date > today:
            return due_date
        return today

    # Stages 2-4: days after previous follow-up
    if not last_followup_date:
        return today  # Fallback: send now

    days_map = {
        2: 3,   # 3 days after Stage 1
        3: 4,   # 4 days after Stage 2 (7 total from Stage 1)
        4: 7,   # 7 days after Stage 3 (14 total from Stage 1)
    }
    days_to_add = days_map.get(next_stage, 3)

    return last_followup_date + timedelta(days=days_to_add)


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
    is_due = next_date <= today
    next_stage = current_stage + 1

    return is_due, next_stage, next_date


def days_overdue(due_date):
    """
    Calculate how many days past the due date.

    Args:
        due_date: Due date (datetime or str 'YYYY-MM-DD')

    Returns:
        int: Number of days overdue (0 if not overdue)
    """
    if isinstance(due_date, str) and due_date:
        due_date = datetime.strptime(due_date[:10], "%Y-%m-%d")
    if not due_date:
        return 0

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    delta = (today - due_date).days
    return max(0, delta)


def is_within_business_hours(country=None, start_hour=None, end_hour=None):
    """
    Check if the current time is within business hours for a given country.

    Args:
        country: Country name matching COUNTRY_TIMEZONES keys (e.g., 'México')
        start_hour: Override business hours start (default from config)
        end_hour: Override business hours end (default from config)

    Returns:
        bool: True if current time is within business hours
    """
    import pytz
    from config import COUNTRY_TIMEZONES, BUSINESS_HOURS_START, BUSINESS_HOURS_END

    bh_start = start_hour if start_hour is not None else BUSINESS_HOURS_START
    bh_end = end_hour if end_hour is not None else BUSINESS_HOURS_END

    if country and country in COUNTRY_TIMEZONES:
        tz = pytz.timezone(COUNTRY_TIMEZONES[country])
    else:
        # Default to UTC if country not found
        tz = pytz.UTC

    now_local = datetime.now(pytz.UTC).astimezone(tz)

    # Skip weekends (Monday=0, Sunday=6)
    if now_local.weekday() >= 5:
        return False

    return bh_start <= now_local.hour < bh_end


if __name__ == "__main__":
    # Self-test
    from datetime import datetime, timedelta

    print("=== compute_next_followup.py — Self Test ===\n")

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    three_days_ago = today - timedelta(days=3)

    # Test Stage 0 → 1 (item overdue)
    is_due, stage, date = is_followup_due(0, due_date=yesterday)
    print(f"Stage 0, due yesterday: is_due={is_due}, next_stage={stage}, date={date}")
    assert is_due and stage == 1

    # Test Stage 0 → 1 (item not yet due)
    is_due, stage, date = is_followup_due(0, due_date=tomorrow)
    print(f"Stage 0, due tomorrow: is_due={is_due}, next_stage={stage}, date={date}")
    assert not is_due and stage == 1

    # Test Stage 1 → 2 (3 days after stage 1)
    is_due, stage, date = is_followup_due(1, last_followup_date=three_days_ago)
    print(f"Stage 1, last 3 days ago: is_due={is_due}, next_stage={stage}, date={date}")
    assert is_due and stage == 2

    # Test Stage 4 (complete)
    is_due, stage, date = is_followup_due(4)
    print(f"Stage 4: is_due={is_due}, next_stage={stage}")
    assert not is_due and stage is None

    # Test days_overdue
    overdue = days_overdue(three_days_ago)
    print(f"\nDays overdue (3 days ago): {overdue}")
    assert overdue == 3

    print("\nAll tests passed!")
