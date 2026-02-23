"""
Notion API wrapper for Client Follow-Up Autopilot.
Handles all database operations with rate limiting and retry logic.
"""

import json
import time
import logging
from datetime import datetime, timezone

import requests

from config import (
    NOTION_API_KEY,
    NOTION_DATABASE_ID,
    NOTION_PROJECTS_DB_ID,
    NOTION_TASKS_DB_ID,
    NOTION_RATE_LIMIT_RPS,
)

logger = logging.getLogger(__name__)

NOTION_BASE_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

# Simple rate limiter state
_last_request_time = 0.0


def _rate_limit():
    """Enforce Notion API rate limit (3 req/sec)."""
    global _last_request_time
    min_interval = 1.0 / NOTION_RATE_LIMIT_RPS
    elapsed = time.time() - _last_request_time
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    _last_request_time = time.time()


def _headers():
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def _request(method, endpoint, payload=None, retries=3):
    """Make a Notion API request with rate limiting and retry on 429."""
    url = f"{NOTION_BASE_URL}{endpoint}"
    for attempt in range(retries):
        _rate_limit()
        try:
            resp = requests.request(
                method, url, headers=_headers(), json=payload, timeout=30
            )
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 1))
                logger.warning(f"Notion rate limited. Retrying in {retry_after}s (attempt {attempt + 1}/{retries})")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if attempt < retries - 1 and resp.status_code >= 500:
                logger.warning(f"Notion server error {resp.status_code}. Retrying (attempt {attempt + 1}/{retries})")
                time.sleep(2 ** attempt)
                continue
            logger.error(f"Notion API error: {e} — Response: {resp.text}")
            raise
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                logger.warning(f"Notion request failed: {e}. Retrying (attempt {attempt + 1}/{retries})")
                time.sleep(2 ** attempt)
                continue
            raise
    return None


# ─── Database Operations ────────────────────────────────────────────────────

def query_database(filter_params=None, sorts=None, database_id=None):
    """
    Query a Notion database with optional filter and sort.
    Handles pagination automatically, returning all results.

    Args:
        filter_params: Notion filter object (dict)
        sorts: List of sort objects
        database_id: Override default database ID

    Returns:
        List of page objects
    """
    db_id = database_id or NOTION_DATABASE_ID
    endpoint = f"/databases/{db_id}/query"
    payload = {}
    if filter_params:
        payload["filter"] = filter_params
    if sorts:
        payload["sorts"] = sorts

    all_results = []
    has_more = True
    while has_more:
        data = _request("POST", endpoint, payload)
        if not data:
            break
        all_results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        if has_more:
            payload["start_cursor"] = data["next_cursor"]

    logger.info(f"Queried Notion database: {len(all_results)} results")
    return all_results


def get_page(page_id):
    """Retrieve a single Notion page by ID."""
    return _request("GET", f"/pages/{page_id}")


def update_page(page_id, properties):
    """
    Update properties on a Notion page.

    Args:
        page_id: The page ID to update
        properties: Dict of property names to Notion property value objects
    """
    payload = {"properties": properties}
    result = _request("PATCH", f"/pages/{page_id}", payload)
    logger.info(f"Updated Notion page {page_id}")
    return result


def append_to_log(page_id, message):
    """
    Append a timestamped message to the Follow-Up Log property of a page.
    Reads current log, appends new entry, writes back.

    Args:
        page_id: The page ID
        message: Text to append
    """
    page = get_page(page_id)
    if not page:
        logger.error(f"Cannot append log: page {page_id} not found")
        return None

    current_log = ""
    log_prop = page.get("properties", {}).get("Follow-Up Log", {})
    if log_prop.get("rich_text"):
        current_log = log_prop["rich_text"][0].get("plain_text", "")

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_entry = f"[{timestamp}] {message}"
    updated_log = f"{current_log}\n{new_entry}" if current_log else new_entry

    # Notion rich_text has a 2000 char limit per block; truncate old entries if needed
    if len(updated_log) > 1900:
        lines = updated_log.split("\n")
        while len("\n".join(lines)) > 1900 and len(lines) > 1:
            lines.pop(0)  # Remove oldest entry
        updated_log = "\n".join(lines)

    return update_page(page_id, {
        "Follow-Up Log": {
            "rich_text": [{"text": {"content": updated_log}}]
        }
    })


# ─── Follow-Up Sub-Pages ─────────────────────────────────────────────────────

def create_followup_subpage(page_id, title, content_blocks):
    """
    Create a sub-page inside a Notion page to log a follow-up action.
    Uses POST /pages with page parent (child_page blocks can't be appended via blocks API).

    Args:
        page_id: Parent page ID
        title: Sub-page title (e.g. "Stage 1 — Reminder — 2026-02-15")
        content_blocks: List of Notion block objects for the sub-page body
    """
    page_payload = {
        "parent": {"page_id": page_id},
        "properties": {
            "title": [{"text": {"content": title}}]
        },
        "children": content_blocks or [],
    }

    try:
        result = _request("POST", "/pages", page_payload)
        if not result:
            logger.warning(f"Failed to create sub-page '{title}' under {page_id}")
            return None

        child_page_id = result.get("id")
        logger.info(f"Created sub-page '{title}' under page {page_id}")
        return child_page_id

    except Exception as e:
        logger.warning(f"Error creating sub-page '{title}': {e}")
        return None


def build_subpage_content(entries):
    """
    Build Notion block objects for sub-page content.

    Args:
        entries: List of dicts with 'label' and 'value' keys

    Returns:
        List of Notion block objects (paragraphs with bold labels)
    """
    blocks = []
    for entry in entries:
        blocks.append({
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": f"{entry['label']}: "},
                        "annotations": {"bold": True},
                    },
                    {
                        "type": "text",
                        "text": {"content": str(entry["value"])},
                    },
                ]
            },
        })
    return blocks


# ─── Property Helpers ────────────────────────────────────────────────────────

def get_text_property(page, prop_name):
    """Extract plain text from a rich_text or title property."""
    prop = page.get("properties", {}).get(prop_name, {})
    prop_type = prop.get("type", "")
    if prop_type == "title":
        items = prop.get("title", [])
    elif prop_type == "rich_text":
        items = prop.get("rich_text", [])
    else:
        return ""
    return items[0].get("plain_text", "") if items else ""


def get_select_property(page, prop_name):
    """Extract the name from a select property."""
    prop = page.get("properties", {}).get(prop_name, {})
    select = prop.get("select")
    return select.get("name", "") if select else ""


def get_date_property(page, prop_name):
    """Extract a date string from a date property."""
    prop = page.get("properties", {}).get(prop_name, {})
    date_obj = prop.get("date")
    return date_obj.get("start", "") if date_obj else ""


def get_number_property(page, prop_name):
    """Extract a number value."""
    prop = page.get("properties", {}).get(prop_name, {})
    return prop.get("number", 0) or 0


def get_checkbox_property(page, prop_name):
    """Extract a checkbox value."""
    prop = page.get("properties", {}).get(prop_name, {})
    return prop.get("checkbox", False)


def get_email_property(page, prop_name):
    """Extract an email value."""
    prop = page.get("properties", {}).get(prop_name, {})
    return prop.get("email", "") or ""


def get_multi_select_property(page, prop_name):
    """Extract list of selected option names from a multi_select property."""
    prop = page.get("properties", {}).get(prop_name, {})
    options = prop.get("multi_select", [])
    return [opt.get("name", "") for opt in options]


def get_status_property(page, prop_name):
    """Extract the name from a status property (different from select)."""
    prop = page.get("properties", {}).get(prop_name, {})
    status = prop.get("status")
    return status.get("name", "") if status else ""


def get_people_property(page, prop_name):
    """Extract list of people names from a people property."""
    prop = page.get("properties", {}).get(prop_name, {})
    people = prop.get("people", [])
    return [p.get("name", "") for p in people]


def get_people_first(page, prop_name):
    """Extract the first person's name from a people property."""
    names = get_people_property(page, prop_name)
    return names[0] if names else ""


def get_rollup_text(page, prop_name):
    """Extract text from a rollup property (handles title, rich_text, email arrays)."""
    prop = page.get("properties", {}).get(prop_name, {})
    rollup = prop.get("rollup", {})
    if rollup.get("type") != "array":
        return ""
    arr = rollup.get("array", [])
    if not arr:
        return ""
    first = arr[0]
    ftype = first.get("type", "")
    if ftype in ("title", "rich_text"):
        items = first.get(ftype, [])
        return items[0].get("plain_text", "") if items else ""
    if ftype == "email":
        return first.get("email", "") or ""
    return ""


def get_rollup_people_first(page, prop_name):
    """Extract the first person's name from a rollup of people."""
    prop = page.get("properties", {}).get(prop_name, {})
    rollup = prop.get("rollup", {})
    if rollup.get("type") != "array":
        return ""
    arr = rollup.get("array", [])
    if not arr:
        return ""
    first = arr[0]
    if first.get("type") == "people":
        people = first.get("people", [])
        return people[0].get("name", "") if people else ""
    return ""


def get_rollup_date(page, prop_name):
    """Extract a date string from a rollup of date."""
    prop = page.get("properties", {}).get(prop_name, {})
    rollup = prop.get("rollup", {})
    if rollup.get("type") != "array":
        return ""
    arr = rollup.get("array", [])
    if not arr:
        return ""
    first = arr[0]
    if first.get("type") == "date":
        date_obj = first.get("date")
        return date_obj.get("start", "") if date_obj else ""
    return ""


def get_rollup_status(page, prop_name):
    """Extract a status name from a rollup of status."""
    prop = page.get("properties", {}).get(prop_name, {})
    rollup = prop.get("rollup", {})
    if rollup.get("type") != "array":
        return ""
    arr = rollup.get("array", [])
    if not arr:
        return ""
    first = arr[0]
    if first.get("type") == "status":
        status = first.get("status")
        return status.get("name", "") if status else ""
    return ""


def resolve_client_email(page):
    """
    Resolve client email by following the relation chain:
    Pendientes CS → Entregable Proyecto → Pendientes Proyectos → Proyecto → Proyectos → Correo cliente.

    Falls back to resolving via the tasks DB if the rollup chain fails.
    """
    # First try the rollup chain on the related task
    relation = page.get("properties", {}).get("Entregable Proyecto", {})
    rel_ids = [r["id"] for r in relation.get("relation", [])]
    if not rel_ids:
        return ""

    # Get the related Pendientes Proyectos page
    task_page = get_page(rel_ids[0])
    if not task_page:
        return ""

    # Try the Correo cliente rollup on the task (rolls up from Proyecto)
    email = get_rollup_text(task_page, "Correo cliente")
    if email:
        return email

    # Fallback: follow Proyecto relation → Correo cliente email field
    proj_relation = task_page.get("properties", {}).get("Proyecto", {})
    proj_ids = [r["id"] for r in proj_relation.get("relation", [])]
    if not proj_ids:
        return ""

    proj_page = get_page(proj_ids[0])
    if not proj_page:
        return ""

    return get_email_property(proj_page, "Correo cliente")


def resolve_project_name(page):
    """
    Resolve project name by following the relation chain:
    Pendientes CS → Entregable Proyecto → Pendientes Proyectos → Proyecto → Proyectos → Project Name.
    """
    relation = page.get("properties", {}).get("Entregable Proyecto", {})
    rel_ids = [r["id"] for r in relation.get("relation", [])]
    if not rel_ids:
        return ""

    task_page = get_page(rel_ids[0])
    if not task_page:
        return ""

    # Try Rollup para tasks (which contains project name)
    proj_name = get_rollup_text(task_page, "Rollup para tasks")
    if proj_name:
        return proj_name

    # Fallback: follow Proyecto relation
    proj_relation = task_page.get("properties", {}).get("Proyecto", {})
    proj_ids = [r["id"] for r in proj_relation.get("relation", [])]
    if not proj_ids:
        return ""

    proj_page = get_page(proj_ids[0])
    if not proj_page:
        return ""

    return get_text_property(proj_page, "Project Name")


def resolve_client_name(page):
    """
    Resolve client name by following the relation chain:
    Pendientes CS → Entregable Proyecto → Pendientes Proyectos → Proyecto → Proyectos → Nombre contacto / Empresa.

    Tries multiple common field names for the client/company name.
    """
    relation = page.get("properties", {}).get("Entregable Proyecto", {})
    rel_ids = [r["id"] for r in relation.get("relation", [])]
    if not rel_ids:
        return ""

    task_page = get_page(rel_ids[0])
    if not task_page:
        return ""

    # Try rollup of client name on the task
    for field in ("Nombre contacto [Proyectos]", "Cliente [Proyectos]", "Empresa [Proyectos]"):
        name = get_rollup_text(task_page, field)
        if name:
            return name

    # Fallback: follow Proyecto relation → client name fields
    proj_relation = task_page.get("properties", {}).get("Proyecto", {})
    proj_ids = [r["id"] for r in proj_relation.get("relation", [])]
    if not proj_ids:
        return ""

    proj_page = get_page(proj_ids[0])
    if not proj_page:
        return ""

    for field in ("Nombre contacto", "Cliente", "Empresa", "Company", "Client Name"):
        name = get_text_property(proj_page, field)
        if name:
            return name

    return ""


def resolve_senior_contact_email(page):
    """
    Resolve senior contact email for Stage 4 escalation.
    Follows the relation chain to the project and looks for senior contact fields.
    """
    relation = page.get("properties", {}).get("Entregable Proyecto", {})
    rel_ids = [r["id"] for r in relation.get("relation", [])]
    if not rel_ids:
        return ""

    task_page = get_page(rel_ids[0])
    if not task_page:
        return ""

    # Try rollup of senior contact on the task
    for field in ("Correo senior [Proyectos]", "Senior Contact Email [Proyectos]"):
        email = get_rollup_text(task_page, field)
        if email:
            return email

    # Fallback: follow Proyecto relation
    proj_relation = task_page.get("properties", {}).get("Proyecto", {})
    proj_ids = [r["id"] for r in proj_relation.get("relation", [])]
    if not proj_ids:
        return ""

    proj_page = get_page(proj_ids[0])
    if not proj_page:
        return ""

    for field in ("Correo senior", "Senior Contact Email", "Email contacto senior"):
        email = get_email_property(proj_page, field)
        if email:
            return email

    return ""


def resolve_client_country(page):
    """
    Resolve client country from the project relation chain.
    Used for business hours enforcement.
    """
    relation = page.get("properties", {}).get("Entregable Proyecto", {})
    rel_ids = [r["id"] for r in relation.get("relation", [])]
    if not rel_ids:
        return ""

    task_page = get_page(rel_ids[0])
    if not task_page:
        return ""

    # Try rollup of country on the task
    for field in ("País [Proyectos]", "Country [Proyectos]"):
        country = get_rollup_text(task_page, field)
        if country:
            return country

    # Fallback: follow Proyecto relation
    proj_relation = task_page.get("properties", {}).get("Proyecto", {})
    proj_ids = [r["id"] for r in proj_relation.get("relation", [])]
    if not proj_ids:
        return ""

    proj_page = get_page(proj_ids[0])
    if not proj_page:
        return ""

    for field in ("País", "Country"):
        country = get_select_property(proj_page, field)
        if country:
            return country
        country = get_text_property(proj_page, field)
        if country:
            return country

    return ""


# ─── Notion Property Builders ───────────────────────────────────────────────

def build_select(value):
    """Build a select property value."""
    return {"select": {"name": value}}


def build_number(value):
    """Build a number property value."""
    return {"number": value}


def build_date(date_str):
    """Build a date property value from an ISO date string."""
    return {"date": {"start": date_str}}


def build_checkbox(value):
    """Build a checkbox property value."""
    return {"checkbox": value}


def build_rich_text(text):
    """Build a rich_text property value."""
    return {"rich_text": [{"text": {"content": text}}]}


def build_email(email):
    """Build an email property value."""
    return {"email": email}


def build_status(value):
    """Build a status property value."""
    return {"status": {"name": value}}


if __name__ == "__main__":
    # Quick connectivity test
    logging.basicConfig(level=logging.INFO)
    if not NOTION_API_KEY:
        print("ERROR: NOTION_API_KEY not set in .env")
    elif not NOTION_DATABASE_ID:
        print("ERROR: NOTION_DATABASE_ID not set in .env")
    else:
        try:
            results = query_database()
            print(f"SUCCESS: Connected to Notion. Found {len(results)} records.")
        except Exception as e:
            print(f"ERROR: Could not connect to Notion: {e}")
