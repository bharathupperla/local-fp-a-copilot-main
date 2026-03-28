from typing import Dict, Any


def validate_intent(intent: Dict[str, Any], schema: Dict[str, Any], *args, **kwargs) -> bool:
    """
    Validates intent BEFORE execution.
    Accepts extra args/kwargs for forward compatibility.
    Must be strict but NOT incorrect.
    """

    if not isinstance(intent, dict):
        raise ValueError("Intent must be a dictionary")

    if not isinstance(schema, dict):
        raise ValueError("Schema must be a dictionary")

    measures = intent.get("measures") or []
    filters = intent.get("filters") or {}
    aggregation = intent.get("aggregation", "sum")

    schema_measures = schema.get("measures") or []
    schema_dimensions = schema.get("dimensions") or []

    # -----------------------------------------
    # 1. Measures must exist
    # -----------------------------------------
    if not measures:
        raise ValueError("No measures identified")

    # -----------------------------------------
    # 2. Validate measures exist in schema
    # -----------------------------------------
    for m in measures:
        if m not in schema_measures:
            raise ValueError(f"Invalid measure: {m}")

    # -----------------------------------------
    # 3. Entity-based questions require filters
    # -----------------------------------------
    entity_keys = {
        "customer_name",
        "worker_name",
        "location_code",
        "client",
        "account",
        "cycle",
        "week_num",
        "month",
        "quarter",
        "year",
    }

    mentions_entity = any(
        key in intent.get("filters", {}) for key in entity_keys
    )

    if mentions_entity and not filters:
        raise ValueError(
            "Question refers to specific entities, but no filters were applied."
        )

    # -----------------------------------------
    # 4. Validate filter columns exist
    # -----------------------------------------
    for f in filters.keys():
        if f not in schema_dimensions:
            raise ValueError(f"Invalid filter column: {f}")

    # -----------------------------------------
    # 5. Aggregation sanity
    # -----------------------------------------
    valid_aggs = {"sum", "avg", "min", "max", "count"}
    if aggregation not in valid_aggs:
        raise ValueError(f"Invalid aggregation: {aggregation}")

    # -----------------------------------------
    # 6. Defensive checks (optional but safe)
    # -----------------------------------------
    if not isinstance(filters, dict):
        raise ValueError("Filters must be a dictionary")

    if not isinstance(measures, list):
        raise ValueError("Measures must be a list")

    return True
