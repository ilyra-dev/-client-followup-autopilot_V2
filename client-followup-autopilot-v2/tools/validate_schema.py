"""
Notion database schema validation for Client Follow-Up Autopilot.
Checks that all required properties exist in the configured database.
Run at daemon startup or as a pre-flight check.
"""

import logging
import sys

import notion_client
from config import NOTION_DATABASE_ID, NOTION_TEAM_DATABASE_ID

logger = logging.getLogger(__name__)

# Required properties in Pendientes Client Success DB
REQUIRED_PROPERTIES_MAIN = {
    "Nombre": ["title"],
    "Status": ["status"],
    "Manual Override": ["checkbox"],
    "Follow-Up Stage": ["number"],
    "Fecha límite de Client Success": ["date"],
    "Last Follow-Up Date": ["date"],
    "Next Follow-Up Date": ["date"],
    "Follow-Up Log": ["rich_text"],
    "Gmail Thread ID": ["rich_text"],
    "Client Language": ["select"],
    "Owner - Client Success": ["people"],
    "Entregable Proyecto": ["relation"],
}

# Optional but recommended properties
OPTIONAL_PROPERTIES_MAIN = {
    "Comentarios Client Success": ["rich_text"],
    "Detalle Falta info / Pausado [Proyectos]": ["rollup"],
    "Fecha Objetivo [Proyectos]": ["rollup"],
    "Responsable [Proyectos]": ["rollup"],
    "Status [Proyectos]": ["rollup"],
}

# Required properties in CS Team Members DB
REQUIRED_PROPERTIES_TEAM = {
    "Name": ["title"],
    "Email": ["email"],
    "Role": ["select"],
    "Languages": ["multi_select"],
    "Active": ["checkbox"],
}


def _get_db_schema(database_id):
    """Fetch the schema (property definitions) of a Notion database."""
    try:
        data = notion_client._request("GET", f"/databases/{database_id}")
        if not data:
            return None
        return data.get("properties", {})
    except Exception as e:
        logger.error(f"Failed to fetch database schema: {e}")
        return None


def validate_main_db():
    """
    Validate the Pendientes Client Success database schema.

    Returns:
        Dict with 'valid', 'missing_required', 'missing_optional', 'type_mismatches'
    """
    if not NOTION_DATABASE_ID:
        return {"valid": False, "error": "NOTION_DATABASE_ID not configured"}

    schema = _get_db_schema(NOTION_DATABASE_ID)
    if schema is None:
        return {"valid": False, "error": "Could not fetch database schema"}

    result = {
        "valid": True,
        "missing_required": [],
        "missing_optional": [],
        "type_mismatches": [],
    }

    # Check required properties
    for prop_name, expected_types in REQUIRED_PROPERTIES_MAIN.items():
        if prop_name not in schema:
            result["missing_required"].append(prop_name)
            result["valid"] = False
        else:
            actual_type = schema[prop_name].get("type", "")
            if actual_type not in expected_types:
                result["type_mismatches"].append(
                    f"{prop_name}: expected {expected_types}, got '{actual_type}'"
                )
                result["valid"] = False

    # Check optional properties
    for prop_name, expected_types in OPTIONAL_PROPERTIES_MAIN.items():
        if prop_name not in schema:
            result["missing_optional"].append(prop_name)

    return result


def validate_team_db():
    """
    Validate the CS Team Members database schema.

    Returns:
        Dict with 'valid', 'missing_required', 'type_mismatches'
    """
    if not NOTION_TEAM_DATABASE_ID:
        return {"valid": False, "error": "NOTION_TEAM_DATABASE_ID not configured"}

    schema = _get_db_schema(NOTION_TEAM_DATABASE_ID)
    if schema is None:
        return {"valid": False, "error": "Could not fetch team database schema"}

    result = {
        "valid": True,
        "missing_required": [],
        "type_mismatches": [],
    }

    for prop_name, expected_types in REQUIRED_PROPERTIES_TEAM.items():
        if prop_name not in schema:
            result["missing_required"].append(prop_name)
            result["valid"] = False
        else:
            actual_type = schema[prop_name].get("type", "")
            if actual_type not in expected_types:
                result["type_mismatches"].append(
                    f"{prop_name}: expected {expected_types}, got '{actual_type}'"
                )
                result["valid"] = False

    return result


def validate_all():
    """
    Run all schema validations.

    Returns:
        Dict with 'main_db' and 'team_db' validation results
    """
    return {
        "main_db": validate_main_db(),
        "team_db": validate_team_db(),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=== Notion Schema Validation ===\n")

    results = validate_all()

    for db_name, result in results.items():
        status = "PASS" if result.get("valid") else "FAIL"
        print(f"  [{status}] {db_name}")

        if result.get("error"):
            print(f"         Error: {result['error']}")
        if result.get("missing_required"):
            print(f"         Missing required: {', '.join(result['missing_required'])}")
        if result.get("missing_optional"):
            print(f"         Missing optional: {', '.join(result['missing_optional'])}")
        if result.get("type_mismatches"):
            for m in result["type_mismatches"]:
                print(f"         Type mismatch: {m}")

    all_valid = all(r.get("valid", False) for r in results.values())
    print(f"\n  {'All schemas valid!' if all_valid else 'Schema issues found — fix before going live.'}")
    sys.exit(0 if all_valid else 1)
